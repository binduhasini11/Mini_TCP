import socket
import threading
import time

from minitcp.connection import MiniTCPConnection
from minitcp.packet import ACK, DATA, Packet


class SlidingWindowSender:
    """
    Sliding-window reliable sender using byte-based sequence numbers.

    Multiple DATA packets may be outstanding at the same time.
    ACKs are cumulative: an ACK acknowledges all bytes before its
    acknowledgement number.
    """

    def __init__(
        self,
        connection: MiniTCPConnection,
        window_size: int = 4,
        timeout: float = 1.0,
        max_retries: int = 5,
        segment_size: int = 512,
    ):
        if connection.peer_address is None:
            raise RuntimeError("Connection has no peer address")

        if connection.state.value != "ESTABLISHED":
            raise RuntimeError("Connection is not established")

        if window_size < 1:
            raise ValueError("window_size must be at least 1")

        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")

        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")

        if segment_size < 1:
            raise ValueError("segment_size must be at least 1")

        self.connection = connection
        self.window_size = window_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.segment_size = segment_size

        # The first data byte follows the client's handshake sequence.
        self.base = connection.sequence_number + 1
        self.next_sequence_number = self.base

        self._lock = threading.Lock()

    def send(self, payload: bytes) -> None:
        """
        Send a complete byte string reliably using a sliding window.

        This method returns only after all payload bytes have been
        cumulatively acknowledged.
        """
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")

        if not payload:
            return

        packets = []

        # Split the payload into segments.
        sequence = self.next_sequence_number

        for offset in range(0, len(payload), self.segment_size):
            chunk = payload[offset:offset + self.segment_size]

            packet = Packet(
                packet_type=DATA,
                sequence_number=sequence,
                payload=chunk,
            )

            packets.append(packet)
            sequence += len(chunk)

        first_sequence = packets[0].sequence_number
        final_sequence = sequence

        # Each outstanding segment stores its most recent send time
        # and number of retransmissions.
        outstanding = {}

        next_packet_index = 0
        acknowledged_until = first_sequence

        old_timeout = getattr(
            self.connection.transport.socket,
            "gettimeout",
            lambda: None,
        )()

        try:
            while acknowledged_until < final_sequence:

                # Fill the sending window.
                while (
                    next_packet_index < len(packets)
                    and len(outstanding) < self.window_size
                ):
                    packet = packets[next_packet_index]

                    print(
                        f"SENDER: Sending DATA "
                        f"SEQ={packet.sequence_number} "
                        f"| Length={len(packet.payload)} bytes"
                    )

                    self.connection.send_packet(
                        packet,
                        self.connection.peer_address,
                    )

                    outstanding[packet.sequence_number] = {
                        "packet": packet,
                        "sent_at": time.monotonic(),
                        "retries": 0,
                    }

                    next_packet_index += 1

                # Wait briefly for an ACK so the sender can also
                # notice retransmission deadlines.
                self.connection.transport.set_timeout(0.05)

                try:
                    response, address = self.connection.receive_packet()
                except socket.timeout:
                    response = None
                    address = None

                now = time.monotonic()

                # Process cumulative ACKs.
                if (
                    response is not None
                    and address == self.connection.peer_address
                    and response.packet_type == ACK
                ):
                    ack_number = response.acknowledgement_number

                    if acknowledged_until < ack_number <= final_sequence:
                        acknowledged_until = ack_number
                        self.base = ack_number

                        # Remove every segment fully covered by ACK.
                        for seq in list(outstanding):
                            packet = outstanding[seq]
                            packet_end = (
                                packet["packet"].sequence_number
                                + len(packet["packet"].payload)
                            )

                            if packet_end <= ack_number:
                                del outstanding[seq]

                        print(
                            f"SENDER: Received cumulative ACK={ack_number}"
                        )

                # Retransmit segments whose timeout has expired.
                for seq in sorted(outstanding):
                    item = outstanding[seq]

                    if now - item["sent_at"] < self.timeout:
                        continue

                    if item["retries"] >= self.max_retries:
                        raise TimeoutError(
                            f"DATA SEQ={seq} failed after "
                            f"{self.max_retries} retransmissions"
                        )

                    print(
                        f"SENDER: TIMEOUT for SEQ={seq}; "
                        f"retransmitting"
                    )

                    self.connection.send_packet(
                        item["packet"],
                        self.connection.peer_address,
                    )

                    item["sent_at"] = time.monotonic()
                    item["retries"] += 1

        finally:
            self.connection.transport.set_timeout(old_timeout)

        # Advance the connection's sequence number only after the
        # entire payload has been acknowledged.
        self.next_sequence_number = final_sequence
        self.connection.sequence_number = final_sequence

        print(
            f"SENDER: All {len(payload)} bytes acknowledged. "
            f"Next SEQ={self.next_sequence_number}"
        )


class SlidingWindowReceiver:
    """
    Sliding-window receiver with cumulative ACKs.

    This implementation buffers out-of-order segments and delivers
    data only when the missing earlier bytes have arrived.
    """

    def __init__(
        self,
        connection: MiniTCPConnection,
        window_size: int = 4,
        segment_size: int = 512,
    ):
        if connection.peer_address is None:
            raise RuntimeError("Connection has no peer address")

        if connection.state.value != "ESTABLISHED":
            raise RuntimeError("Connection is not established")

        if window_size < 1:
            raise ValueError("window_size must be at least 1")

        if segment_size < 1:
            raise ValueError("segment_size must be at least 1")

        self.connection = connection
        self.window_size = window_size
        self.segment_size = segment_size
        self.receive_window_bytes = window_size * segment_size

        # The receiver expects the byte immediately after the
        # client's handshake sequence number.
        self.expected_sequence_number = connection.acknowledgement_number

        self.buffer = {}
        self._ready = bytearray()

    def receive(self) -> bytes:
        """
        Return the next contiguous DATA segment.

        Out-of-order data is buffered. Duplicate or out-of-window
        packets are not delivered twice.
        """
        while not self._ready:
            packet, address = self.connection.receive_packet()

            if address != self.connection.peer_address:
                continue

            if packet.packet_type != DATA:
                continue

            seq = packet.sequence_number
            payload = packet.payload

            if not payload:
                self._send_ack()
                continue

            packet_end = seq + len(payload)

            # Already received data: acknowledge the current point.
            if packet_end <= self.expected_sequence_number:
                print(
                    f"RECEIVER: Duplicate DATA SEQ={seq}; "
                    f"sending ACK={self.expected_sequence_number}"
                )
                self._send_ack()
                continue

            # Keep only packets inside the receive window.
            max_sequence = (
                self.expected_sequence_number
                + self.receive_window_bytes
            )

            if (
                seq < self.expected_sequence_number
                or packet_end > max_sequence
            ):
                print(
                    f"RECEIVER: DATA SEQ={seq} outside receive window; "
                    f"sending ACK={self.expected_sequence_number}"
                )
                self._send_ack()
                continue

            if seq not in self.buffer:
                self.buffer[seq] = payload

            # Deliver any now-contiguous data segments.
            while self.expected_sequence_number in self.buffer:
                chunk = self.buffer.pop(self.expected_sequence_number)
                self._ready.extend(chunk)
                self.expected_sequence_number += len(chunk)

            self._send_ack()

        data = bytes(self._ready)
        self._ready.clear()
        return data

    def _send_ack(self):
        ack = Packet(
            packet_type=ACK,
            sequence_number=self.connection.sequence_number,
            acknowledgement_number=self.expected_sequence_number,
        )

        print(
            f"RECEIVER: Sending cumulative "
            f"ACK={self.expected_sequence_number}"
        )

        self.connection.send_packet(
            ack,
            self.connection.peer_address,
        )
