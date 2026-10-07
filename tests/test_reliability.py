import socket
import unittest

from minitcp.packet import ACK, DATA, Packet
from minitcp.reliability import ReliableReceiver, ReliableSender
from minitcp.state import ConnectionState


PEER = ("127.0.0.1", 9001)


class FakeTransport:
    def __init__(self):
        self.timeout = None

    def set_timeout(self, timeout):
        self.timeout = timeout


class FakeConnection:
    def __init__(self, incoming=None, timeout_first=False):
        self.state = ConnectionState.ESTABLISHED
        self.peer_address = PEER
        self.sequence_number = 100
        self.acknowledgement_number = 100

        self.transport = FakeTransport()
        self.sent_packets = []

        self.incoming = list(incoming or [])
        self.timeout_first = timeout_first
        self.receive_count = 0

    def send_packet(self, packet, address):
        self.sent_packets.append((packet, address))

    def receive_packet(self):
        self.receive_count += 1

        if self.timeout_first and self.receive_count == 1:
            raise socket.timeout()

        if not self.incoming:
            raise socket.timeout()

        return self.incoming.pop(0)


class TestReliableReceiver(unittest.TestCase):
    def test_duplicate_data_is_not_delivered_twice(self):
        duplicate = Packet(
            packet_type=DATA,
            sequence_number=99,
            payload=b"old",
        )

        valid = Packet(
            packet_type=DATA,
            sequence_number=100,
            payload=b"new",
        )

        connection = FakeConnection(
            incoming=[
                (duplicate, PEER),
                (valid, PEER),
            ]
        )

        receiver = ReliableReceiver(connection)

        data = receiver.receive()

        self.assertEqual(data, b"new")
        self.assertEqual(len(connection.sent_packets), 2)

        first_ack = connection.sent_packets[0][0]
        second_ack = connection.sent_packets[1][0]

        self.assertEqual(first_ack.packet_type, ACK)
        self.assertEqual(first_ack.acknowledgement_number, 100)

        self.assertEqual(second_ack.packet_type, ACK)
        self.assertEqual(second_ack.acknowledgement_number, 103)

    def test_out_of_order_data_sends_cumulative_ack(self):
        out_of_order = Packet(
            packet_type=DATA,
            sequence_number=110,
            payload=b"future",
        )

        valid = Packet(
            packet_type=DATA,
            sequence_number=100,
            payload=b"hello",
        )

        connection = FakeConnection(
            incoming=[
                (out_of_order, PEER),
                (valid, PEER),
            ]
        )

        receiver = ReliableReceiver(connection)

        data = receiver.receive()

        self.assertEqual(data, b"hello")
        self.assertEqual(len(connection.sent_packets), 2)

        first_ack = connection.sent_packets[0][0]
        second_ack = connection.sent_packets[1][0]

        self.assertEqual(first_ack.packet_type, ACK)
        self.assertEqual(first_ack.acknowledgement_number, 100)

        self.assertEqual(second_ack.packet_type, ACK)
        self.assertEqual(second_ack.acknowledgement_number, 105)


class TestReliableSender(unittest.TestCase):
    def test_timeout_causes_retransmission(self):
        expected_ack = 101 + len(b"hello")

        ack = Packet(
            packet_type=ACK,
            acknowledgement_number=expected_ack,
        )

        connection = FakeConnection(
            incoming=[(ack, PEER)],
            timeout_first=True,
        )

        sender = ReliableSender(
            connection,
            timeout=0.1,
            max_retries=2,
        )

        sender.send(b"hello")

        self.assertEqual(len(connection.sent_packets), 2)

        self.assertEqual(
            connection.sent_packets[0][0].sequence_number,
            101,
        )

        self.assertEqual(
            connection.sent_packets[1][0].sequence_number,
            101,
        )

    def test_retry_exhaustion_raises_timeout(self):
        connection = FakeConnection(
            incoming=[],
        )

        sender = ReliableSender(
            connection,
            timeout=0.01,
            max_retries=2,
        )

        with self.assertRaises(TimeoutError):
            sender.send(b"hello")

        self.assertEqual(len(connection.sent_packets), 3)


if __name__ == "__main__":
    unittest.main()
