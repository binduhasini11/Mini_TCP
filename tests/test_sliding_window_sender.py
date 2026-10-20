import socket
import time
import unittest
from unittest.mock import Mock

from minitcp.packet import ACK, DATA, Packet
from minitcp.sliding_window import SlidingWindowSender


class TestSlidingWindowSender(unittest.TestCase):

    def setUp(self):
        self.connection = Mock()

        self.connection.peer_address = ("127.0.0.1", 9000)
        self.connection.sequence_number = 100
        self.connection.acknowledgement_number = 500
        self.connection.state.value = "ESTABLISHED"

        self.connection.transport.socket.gettimeout.return_value = None

    def make_ack(self, ack_number):
        return (
            Packet(
                packet_type=ACK,
                acknowledgement_number=ack_number,
            ),
            self.connection.peer_address,
        )

    def make_sender(self, **kwargs):
        defaults = {
            "window_size": 2,
            "timeout": 1.0,
            "max_retries": 2,
            "segment_size": 4,
        }
        defaults.update(kwargs)

        return SlidingWindowSender(self.connection, **defaults)

    def test_sends_multiple_segments_and_processes_acks(self):
        sender = self.make_sender()

        self.connection.receive_packet.side_effect = [
            self.make_ack(105),
            self.make_ack(109),
        ]

        sender.send(b"abcdefgh")

        sent_packets = [
            call.args[0]
            for call in self.connection.send_packet.call_args_list
            if call.args[0].packet_type == DATA
        ]

        self.assertEqual(len(sent_packets), 2)
        self.assertEqual(sent_packets[0].sequence_number, 101)
        self.assertEqual(sent_packets[0].payload, b"abcd")
        self.assertEqual(sent_packets[1].sequence_number, 105)
        self.assertEqual(sent_packets[1].payload, b"efgh")

        self.assertEqual(sender.next_sequence_number, 109)
        self.assertEqual(self.connection.sequence_number, 109)

    def test_single_segment_ack_updates_rtt_estimator(self):
        sender = self.make_sender()

        self.connection.receive_packet.side_effect = [
            self.make_ack(105),
        ]

        sender.send(b"data")

        self.assertIsNotNone(sender.rtt_estimator.srtt)
        self.assertGreater(sender.rtt_estimator.srtt, 0)

    def test_cumulative_ack_completes_transfer(self):
        sender = self.make_sender()

        # One ACK acknowledges both four-byte segments.
        self.connection.receive_packet.side_effect = [
            self.make_ack(109),
        ]

        sender.send(b"abcdefgh")

        self.assertEqual(sender.next_sequence_number, 109)

        # A single cumulative ACK covering multiple segments is
        # ambiguous for RTT measurement, so no sample is taken.
        self.assertIsNone(sender.rtt_estimator.srtt)

    def test_retransmitted_segment_does_not_update_rtt(self):
        sender = self.make_sender(
            window_size=1,
            timeout=0.1,
            max_retries=2,
            segment_size=4,
        )

        calls = {"count": 0}

        def receive_with_one_timeout():
            calls["count"] += 1

            if calls["count"] == 1:
                # Allow the initial retransmission deadline to expire.
                time.sleep(0.12)
                raise socket.timeout()

            return self.make_ack(105)

        self.connection.receive_packet.side_effect = (
            receive_with_one_timeout
        )

        sender.send(b"data")

        sent_packets = [
            call.args[0]
            for call in self.connection.send_packet.call_args_list
            if call.args[0].packet_type == DATA
        ]

        self.assertEqual(len(sent_packets), 2)
        self.assertEqual(
            sent_packets[0].sequence_number,
            sent_packets[1].sequence_number,
        )

        # Karn's algorithm: the retransmitted segment's ACK must not
        # be used to estimate RTT.
        self.assertIsNone(sender.rtt_estimator.srtt)

        # The timeout should have triggered exponential backoff.
        self.assertGreaterEqual(sender.rtt_estimator.rto, 0.2)


if __name__ == "__main__":
    unittest.main()
