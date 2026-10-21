import hashlib
import json
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

from minitcp.file_transfer import (
    FileTransferError,
    IncompleteTransferError,
    IntegrityError,
    _read_exact,
    receive_file,
    send_file,
)
from minitcp.connection import MiniTCPConnection
from minitcp.packet import DATA, ACK, SYN_ACK, Packet
from minitcp.sliding_window import SlidingWindowReceiver, SlidingWindowSender
from minitcp.state import ConnectionState
from minitcp.transport import UDPTransport


class QueueReceiver:
    def __init__(self, data=b"", timeout=False):
        self.data = bytearray(data)
        self.timeout = timeout

    def receive(self):
        if self.timeout:
            raise socket.timeout()
        if not self.data:
            raise socket.timeout()
        amount = min(7, len(self.data))
        result = bytes(self.data[:amount])
        del self.data[:amount]
        return result


class TimeoutOnceReceiver:
    def __init__(self, data):
        self.data = data
        self.calls = 0

    def receive(self):
        self.calls += 1
        if self.calls == 1:
            raise socket.timeout()
        return self.data


class CaptureSender:
    def __init__(self):
        self.data = bytearray()

    def send(self, data):
        self.data.extend(data)


def framed(metadata, payload):
    encoded = json.dumps(metadata, separators=(",", ":")).encode()
    return len(encoded).to_bytes(4, "big") + encoded + payload


class FinalHandshakeAckDropRelay:
    """Local UDP relay that drops a configured number of final handshake ACKs."""

    def __init__(self, server_address, drop_all=False,
                 drop_first_server_data=False):
        self.server_address = server_address
        self.drop_all = drop_all
        self.drop_first_server_data = drop_first_server_data
        self.dropped_server_data = 0
        self.dropped = 0
        self.syn_acks = 0
        self.data_before_retransmitted_syn_ack = 0
        self.client_address = None
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.settimeout(0.02)
        self.address = self.socket.getsockname()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._relay, daemon=True)

    def start(self):
        self.thread.start()

    def _relay(self):
        while not self.stop_event.is_set():
            try:
                data, source = self.socket.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                if self.stop_event.is_set():
                    return
                raise
            if source == self.server_address:
                packet = Packet.deserialize(data)
                if packet.packet_type == SYN_ACK:
                    self.syn_acks += 1
                if (packet.packet_type == DATA
                        and self.drop_first_server_data
                        and self.dropped_server_data == 0):
                    self.dropped_server_data += 1
                    continue
                if self.client_address is not None:
                    self.socket.sendto(data, self.client_address)
                continue

            self.client_address = source
            packet = Packet.deserialize(data)
            if packet.packet_type == DATA and self.syn_acks < 2:
                self.data_before_retransmitted_syn_ack += 1
            if (packet.packet_type == ACK
                    and packet.sequence_number == 101
                    and packet.acknowledgement_number == 501
                    and not packet.payload
                    and (self.drop_all or self.dropped == 0)):
                self.dropped += 1
                continue
            self.socket.sendto(data, self.server_address)

    def close(self):
        self.stop_event.set()
        self.socket.close()
        self.thread.join(timeout=0.5)


class FileTransferTests(unittest.TestCase):
    def test_exact_read_retries_transient_socket_timeout_until_deadline(self):
        receiver = TimeoutOnceReceiver(b"head")
        result = _read_exact(
            receiver, 4, deadline=time.monotonic() + 0.5
        )
        self.assertEqual(result, b"head")
        self.assertEqual(receiver.calls, 2)

    def test_exact_read_still_fails_when_bounded_deadline_expires(self):
        receiver = QueueReceiver(timeout=True)
        started = time.monotonic()
        with self.assertRaises(IncompleteTransferError):
            _read_exact(receiver, 4, deadline=started + 0.02)
        self.assertLess(time.monotonic() - started, 0.2)

    def test_only_matching_duplicate_syn_ack_is_re_acked(self):
        client = UDPTransport("127.0.0.1", 0)
        server = UDPTransport("127.0.0.1", 0)
        client.bind()
        server.bind()
        client_address = client.socket.getsockname()
        server_address = server.socket.getsockname()
        server.set_timeout(0.05)
        connection = MiniTCPConnection(client)
        connection.peer_address = server_address
        connection.sequence_number = 100
        connection.acknowledgement_number = 501
        connection.state = ConnectionState.ESTABLISHED
        invalid_packets = (
            Packet(SYN_ACK, sequence_number=499, acknowledgement_number=101),
            Packet(SYN_ACK, sequence_number=500, acknowledgement_number=100),
            Packet(SYN_ACK, sequence_number=500, acknowledgement_number=101,
                   payload=b"unexpected"),
            Packet(DATA, sequence_number=101, payload=b"data"),
        )
        try:
            for packet in invalid_packets:
                self.assertFalse(connection.respond_to_retransmitted_syn_ack(
                    packet, server_address
                ))
            valid = Packet(SYN_ACK, sequence_number=500,
                           acknowledgement_number=101)
            self.assertFalse(connection.respond_to_retransmitted_syn_ack(
                valid, client_address
            ))
            with self.assertRaises(socket.timeout):
                server.receive()

            self.assertTrue(connection.respond_to_retransmitted_syn_ack(
                valid, server_address
            ))
            ack = Packet.deserialize(server.receive()[0])
            self.assertEqual(ack.packet_type, ACK)
            self.assertEqual(ack.sequence_number, 101)
            self.assertEqual(ack.acknowledgement_number, 501)
        finally:
            client.close()
            server.close()

    def test_lost_final_handshake_ack_recovers_and_transfers_file(self):
        source_payload = b"file after lost handshake ACK\x00\xff"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.bin"
            destination = root / "received"
            destination.mkdir()
            source.write_bytes(source_payload)

            server = UDPTransport("127.0.0.1", 0)
            server.bind()
            server_address = server.socket.getsockname()
            relay = FinalHandshakeAckDropRelay(
                server_address, drop_first_server_data=True
            )
            relay.start()
            server_results = []

            def receive_transfer():
                try:
                    connection = MiniTCPConnection(server)
                    # Delay the server's repeated SYN-ACK until after the
                    # client's first DATA timeout, forcing DATA to arrive
                    # while accept() is still in SYN_RECEIVED.
                    connection.accept(handshake_timeout=0.12, max_retries=8)
                    server.set_timeout(0.5)
                    receiver = SlidingWindowReceiver(connection, segment_size=64)
                    status_sender = SlidingWindowSender(
                        connection, segment_size=64, timeout=0.1,
                    max_retries=4,
                        incoming_data_handler=receiver.acknowledge_retransmitted_data,
                    )
                    path = receive_file(receiver, status_sender, destination)
                    server_results.append((connection.state, path, None))
                except Exception as exc:
                    server_results.append((None, None, exc))
                finally:
                    server.close()

            worker = threading.Thread(target=receive_transfer)
            client = UDPTransport("127.0.0.1", 0)
            client.bind()
            client.set_timeout(1.0)
            try:
                worker.start()
                connection = MiniTCPConnection(client)
                connection.connect(relay.address)
                client.set_timeout(0.1)
                sender = SlidingWindowSender(connection, segment_size=64,
                                             timeout=0.1, max_retries=1)
                status_receiver = SlidingWindowReceiver(connection, segment_size=64)
                response = send_file(sender, status_receiver, source)
                worker.join(timeout=1.0)

                self.assertFalse(worker.is_alive())
                self.assertEqual(relay.dropped, 1)
                self.assertEqual(relay.dropped_server_data, 1)
                self.assertGreaterEqual(relay.syn_acks, 2)
                self.assertGreater(relay.data_before_retransmitted_syn_ack, 0)
                self.assertEqual(connection.state, ConnectionState.ESTABLISHED)
                self.assertEqual(len(server_results), 1)
                server_state, received_path, error = server_results[0]
                self.assertIsNone(error)
                self.assertEqual(server_state, ConnectionState.ESTABLISHED)
                self.assertTrue(response["ok"])
                self.assertEqual(source.stat().st_size, received_path.stat().st_size)
                self.assertEqual(hashlib.sha256(source.read_bytes()).digest(),
                                 hashlib.sha256(received_path.read_bytes()).digest())
                self.assertEqual(response["sha256"],
                                 hashlib.sha256(source.read_bytes()).hexdigest())
            finally:
                client.close()
                relay.close()
                if worker.is_alive():
                    worker.join(timeout=1.0)
            self.assertFalse(worker.is_alive())
            self.assertFalse(relay.thread.is_alive())
            self.assertEqual(server.socket.fileno(), -1)
            self.assertEqual(client.socket.fileno(), -1)
            self.assertEqual(relay.socket.fileno(), -1)

    def test_permanently_lost_final_handshake_ack_fails_within_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.bin"
            source.write_bytes(b"bounded handshake failure")
            server = UDPTransport("127.0.0.1", 0)
            server.bind()
            server_address = server.socket.getsockname()
            relay = FinalHandshakeAckDropRelay(server_address, drop_all=True)
            relay.start()
            server_errors = []

            def accept_only():
                try:
                    MiniTCPConnection(server).accept(
                        handshake_timeout=0.05, max_retries=3
                    )
                except Exception as exc:
                    server_errors.append(exc)
                finally:
                    server.close()

            worker = threading.Thread(target=accept_only)
            client = UDPTransport("127.0.0.1", 0)
            client.bind()
            client.set_timeout(1.0)
            started = time.monotonic()
            client_error = None
            try:
                worker.start()
                connection = MiniTCPConnection(client)
                connection.connect(relay.address)
                client.set_timeout(0.05)
                sender = SlidingWindowSender(connection, segment_size=64,
                                             timeout=0.1, max_retries=2)
                with self.assertRaises(TimeoutError) as caught:
                    send_file(sender, SlidingWindowReceiver(connection), source)
                client_error = caught.exception
                worker.join(timeout=0.8)
                self.assertFalse(worker.is_alive())
                self.assertLess(time.monotonic() - started, 1.0)
                self.assertIsInstance(server_errors[0], TimeoutError)
                self.assertIn("handshake timed out", str(server_errors[0]))
                self.assertGreater(relay.syn_acks, 1)
            finally:
                client.close()
                relay.close()
                if worker.is_alive():
                    worker.join(timeout=0.5)
            self.assertIsNotNone(client_error)
            self.assertFalse(worker.is_alive())
            self.assertFalse(relay.thread.is_alive())
            self.assertEqual(server.socket.fileno(), -1)
            self.assertEqual(client.socket.fileno(), -1)
            self.assertEqual(relay.socket.fileno(), -1)

    def test_real_udp_receiver_reacks_retransmitted_final_data(self):
        """A dropped final ACK is recovered when final DATA is retransmitted."""
        server_transport = UDPTransport("127.0.0.1", 0)
        client_transport = UDPTransport("127.0.0.1", 0)
        server_transport.bind()
        client_transport.bind()
        server_address = server_transport.socket.getsockname()
        client_address = client_transport.socket.getsockname()
        server_transport.set_timeout(0.2)
        client_transport.set_timeout(0.5)
        connection = MiniTCPConnection(server_transport)
        connection.peer_address = client_address
        connection.state = ConnectionState.ESTABLISHED
        connection.sequence_number = 500
        connection.acknowledgement_number = 101
        receiver = SlidingWindowReceiver(connection, segment_size=32)
        client_connection = MiniTCPConnection(client_transport)
        client_connection.peer_address = server_address
        client_connection.state = ConnectionState.ESTABLISHED
        client_connection.sequence_number = 100
        client_connection.acknowledgement_number = 501
        client_receiver = SlidingWindowReceiver(client_connection, segment_size=32)
        final_data = Packet(DATA, sequence_number=101, payload=b"tail")
        response_result = []

        def send_verification_response():
            try:
                sender = SlidingWindowSender(
                    connection, timeout=0.1, max_retries=2,
                    incoming_data_handler=receiver.acknowledge_retransmitted_data,
                )
                sender.send(b"status")
                response_result.append(None)
            except Exception as exc:
                response_result.append(exc)

        worker = threading.Thread(target=send_verification_response)
        try:
            # Initial final DATA is received; deliberately consume and drop its ACK.
            client_transport.send(final_data.serialize(), server_address)
            self.assertEqual(receiver.receive(), b"tail")
            first_ack = Packet.deserialize(client_transport.receive()[0])
            self.assertEqual(first_ack.packet_type, ACK)
            self.assertEqual(first_ack.acknowledgement_number, 105)
            # The receiver is now sending the verification response and can
            # still consume/re-ACK a retransmission of the final file DATA.
            worker.start()
            client_transport.send(final_data.serialize(), server_address)
            repeated_ack = None
            deadline = time.monotonic() + 0.5
            while time.monotonic() < deadline:
                incoming = Packet.deserialize(client_transport.receive()[0])
                if incoming.packet_type == ACK and incoming.acknowledgement_number == 105:
                    repeated_ack = incoming
                    break
            self.assertEqual(repeated_ack.acknowledgement_number, 105)
            client_result = []
            client_worker = threading.Thread(
                target=lambda: client_result.append(client_receiver.receive())
            )
            client_worker.start()
            client_worker.join(timeout=0.5)
            worker.join(timeout=0.5)
            self.assertEqual(client_result, [b"status"])
            self.assertEqual(response_result, [None])
            self.assertFalse(worker.is_alive())
        finally:
            server_transport.close()
            client_transport.close()
            if worker.is_alive():
                worker.join(timeout=0.5)

    def receive(self, payload, name="payload.bin", expected=None):
        expected = hashlib.sha256(payload).hexdigest() if expected is None else expected
        metadata = {"name": name, "size": len(payload), "sha256": expected}
        response_sender = CaptureSender()
        directory = tempfile.TemporaryDirectory()
        path = receive_file(QueueReceiver(framed(metadata, payload)),
                            response_sender, directory.name, chunk_size=5)
        return directory, path, response_sender.data

    def test_empty_file(self):
        directory, path, _ = self.receive(b"")
        self.assertEqual(path.read_bytes(), b"")
        self.assertEqual(path.parent, Path(directory.name))
        directory.cleanup()

    def test_small_binary_with_nulls_and_matching_sha256(self):
        payload = b"\x00\xffmini\x00tcp"
        directory, path, response = self.receive(payload)
        self.assertEqual(path.read_bytes(), payload)
        result_length = int.from_bytes(response[:4], "big")
        result = json.loads(response[4:4 + result_length])
        self.assertTrue(result["ok"])
        self.assertEqual(result["sha256"], hashlib.sha256(payload).hexdigest())
        directory.cleanup()

    def test_file_larger_than_segment_size(self):
        payload = bytes(range(256)) * 9
        directory, path, _ = self.receive(payload)
        self.assertEqual(path.stat().st_size, len(payload))
        self.assertEqual(hashlib.sha256(path.read_bytes()).digest(),
                         hashlib.sha256(payload).digest())
        directory.cleanup()

    def test_integrity_mismatch_is_not_published(self):
        payload = b"corrupted"
        with tempfile.TemporaryDirectory() as directory:
            metadata = {"name": "bad.bin", "size": len(payload),
                        "sha256": "0" * 64}
            status = CaptureSender()
            with self.assertRaises(IntegrityError):
                receive_file(QueueReceiver(framed(metadata, payload)), status,
                            directory)
            self.assertFalse((Path(directory) / "bad.bin").exists())
            self.assertTrue(status.data)

    def test_truncated_transfer_is_reported_and_not_published(self):
        payload = b"short"
        metadata = {"name": "partial.bin", "size": 10,
                    "sha256": hashlib.sha256(payload).hexdigest()}
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(IncompleteTransferError):
                receive_file(QueueReceiver(framed(metadata, payload)),
                             CaptureSender(), directory)
            self.assertFalse((Path(directory) / "partial.bin").exists())

    def test_unsafe_names_are_rejected(self):
        for name in ("../escape", "sub/file", "..", "C:\\escape"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                metadata = {"name": name, "size": 0,
                            "sha256": hashlib.sha256(b"").hexdigest()}
                with self.assertRaises(FileTransferError):
                    receive_file(QueueReceiver(framed(metadata, b"")),
                                 CaptureSender(), directory)

    def test_sender_streams_and_waits_for_verified_status(self):
        payload = bytes(range(256)) * 4
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.bin"
            source.write_bytes(payload)
            wire_sender = CaptureSender()
            status_data = json.dumps({"ok": True, "name": source.name,
                                      "size": len(payload),
                                      "sha256": hashlib.sha256(payload).hexdigest()}).encode()
            status_receiver = QueueReceiver(len(status_data).to_bytes(4, "big") + status_data)
            result = send_file(wire_sender, status_receiver, source,
                               chunk_size=100)
            self.assertEqual(result["sha256"], hashlib.sha256(payload).hexdigest())
            header_length = int.from_bytes(wire_sender.data[:4], "big")
            metadata = json.loads(wire_sender.data[4:4 + header_length])
            self.assertEqual(metadata["size"], len(payload))
            self.assertEqual(wire_sender.data[4 + header_length:], payload)


if __name__ == "__main__":
    unittest.main()
