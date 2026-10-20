import unittest
from unittest.mock import Mock

from minitcp.packet import ACK, DATA, Packet
from minitcp.sliding_window import SlidingWindowReceiver


class TestSlidingWindowReceiver(unittest.TestCase):

    def setUp(self):
        self.connection = Mock()

        self.connection.peer_address = ("127.0.0.1", 9000)
        self.connection.sequence_number = 500
        self.connection.acknowledgement_number = 101

        self.connection.state.value = "ESTABLISHED"

        self.receiver = SlidingWindowReceiver(
            self.connection,
            window_size=4,
            segment_size=8,
        )

    def queue_packet(self, packet, address=None):
        if address is None:
            address = self.connection.peer_address

        self.connection.receive_packet.return_value = (
            packet,
            address,
        )

    def test_receives_in_order_data(self):
        packet = Packet(
            packet_type=DATA,
            sequence_number=101,
            payload=b"hello",
        )

        self.queue_packet(packet)

        result = self.receiver.receive()

        self.assertEqual(result, b"hello")
        self.assertEqual(
            self.receiver.expected_sequence_number,
            106,
        )

        ack = self.connection.send_packet.call_args.args[0]

        self.assertEqual(ack.packet_type, ACK)
        self.assertEqual(ack.acknowledgement_number, 106)

    def test_buffers_out_of_order_data(self):
        later_packet = Packet(
            packet_type=DATA,
            sequence_number=106,
            payload=b"world",
        )

        earlier_packet = Packet(
            packet_type=DATA,
            sequence_number=101,
            payload=b"hello",
        )

        self.connection.receive_packet.side_effect = [
            (later_packet, self.connection.peer_address),
            (earlier_packet, self.connection.peer_address),
        ]

        result = self.receiver.receive()

        self.assertEqual(result, b"helloworld")
        self.assertEqual(
            self.receiver.expected_sequence_number,
            111,
        )

    def test_duplicate_data_is_not_delivered_twice(self):
        packet = Packet(
            packet_type=DATA,
            sequence_number=101,
            payload=b"hello",
        )

        duplicate = Packet(
            packet_type=DATA,
            sequence_number=101,
            payload=b"hello",
        )

        self.connection.receive_packet.side_effect = [
            (packet, self.connection.peer_address),
            (duplicate, self.connection.peer_address),
        ]

        first_result = self.receiver.receive()

        self.assertEqual(first_result, b"hello")

        # The next receive call should ignore the duplicate and
        # return only when new contiguous data arrives.
        next_packet = Packet(
            packet_type=DATA,
            sequence_number=106,
            payload=b"world",
        )

        self.connection.receive_packet.side_effect = [
            (duplicate, self.connection.peer_address),
            (next_packet, self.connection.peer_address),
        ]

        second_result = self.receiver.receive()

        self.assertEqual(second_result, b"world")

    def test_rejects_segment_outside_receive_window(self):
        # The receive window is 4 * 8 = 32 bytes.
        # Starting at sequence 134 would exceed the window.
        packet = Packet(
            packet_type=DATA,
            sequence_number=134,
            payload=b"x",
        )

        self.queue_packet(packet)

        # Follow with valid data so receive() can return.
        valid_packet = Packet(
            packet_type=DATA,
            sequence_number=101,
            payload=b"ok",
        )

        self.connection.receive_packet.side_effect = [
            (packet, self.connection.peer_address),
            (valid_packet, self.connection.peer_address),
        ]

        result = self.receiver.receive()

        self.assertEqual(result, b"ok")
        self.assertEqual(
            self.receiver.expected_sequence_number,
            103,
        )


if __name__ == "__main__":
    unittest.main()
