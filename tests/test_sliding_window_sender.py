import socket
import time
import unittest
from unittest.mock import Mock

from minitcp.congestion import CongestionState
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
        sender.congestion_controller.cwnd = 2.0

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


class TestSlidingWindowCongestionIntegration(unittest.TestCase):
    """
    Integration tests for CongestionController with SlidingWindowSender.
    Uses mocked connections and deterministic ACK sequences.
    """

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
            "window_size": 10,
            "timeout": 1.0,
            "max_retries": 2,
            "segment_size": 4,
        }
        defaults.update(kwargs)

        return SlidingWindowSender(self.connection, **defaults)

    def test_slow_start_window_growth_controls_transmission(self):
        sender = self.make_sender(window_size=10, segment_size=4)
        self.assertEqual(sender.congestion_controller.cwnd, 1.0)
        self.assertEqual(
            sender.congestion_controller.state,
            CongestionState.SLOW_START,
        )

        # 12 bytes = 3 segments of 4 bytes: [101..105, 105..109, 109..113]
        # Iteration 1: cwnd=1.0 -> transmits SEQ 101. ACK 105 arrives -> cwnd becomes 2.0
        # Iteration 2: cwnd=2.0 -> transmits SEQ 105, 109. ACK 109 arrives -> cwnd becomes 3.0
        # Iteration 3: ACK 113 arrives -> cwnd becomes 4.0
        self.connection.receive_packet.side_effect = [
            self.make_ack(105),
            self.make_ack(109),
            self.make_ack(113),
        ]

        sender.send(b"123456789012")

        self.assertEqual(sender.next_sequence_number, 113)
        self.assertEqual(sender.congestion_controller.cwnd, 4.0)
        self.assertEqual(
            sender.congestion_controller.state,
            CongestionState.SLOW_START,
        )

        sent_packets = [
            call.args[0]
            for call in self.connection.send_packet.call_args_list
            if call.args[0].packet_type == DATA
        ]
        self.assertEqual(len(sent_packets), 3)
        self.assertEqual(
            [p.sequence_number for p in sent_packets],
            [101, 105, 109],
        )

    def test_congestion_avoidance_additive_increase(self):
        sender = self.make_sender(window_size=10, segment_size=4)
        sender.congestion_controller.cwnd = 4.0
        sender.congestion_controller.ssthresh = 4.0
        sender.congestion_controller.state = (
            CongestionState.CONGESTION_AVOIDANCE
        )

        self.connection.receive_packet.side_effect = [
            self.make_ack(105),
        ]

        sender.send(b"abcd")

        self.assertEqual(sender.next_sequence_number, 105)
        # cwnd increases by 1.0 / cwnd = 1.0 / 4.0 = 0.25
        self.assertAlmostEqual(sender.congestion_controller.cwnd, 4.25)
        self.assertEqual(
            sender.congestion_controller.state,
            CongestionState.CONGESTION_AVOIDANCE,
        )

    def test_three_duplicate_acks_trigger_fast_retransmit_and_recovery(self):
        sender = self.make_sender(window_size=4, segment_size=4)
        sender.congestion_controller.cwnd = 4.0
        sender.congestion_controller.ssthresh = 8.0

        # Send 8 bytes (2 segments: SEQ 101, SEQ 105)
        # 3 duplicate ACKs for SEQ 101 trigger Fast Retransmit.
        # An optional 4th duplicate ACK inflates cwnd by 1 in Fast Recovery.
        # Final ACK 109 exits Fast Recovery to Congestion Avoidance with cwnd=ssthresh.
        self.connection.receive_packet.side_effect = [
            self.make_ack(101),
            self.make_ack(101),
            self.make_ack(101),
            self.make_ack(101),
            self.make_ack(109),
        ]

        sender.send(b"abcdefgh")

        sent_packets = [
            call.args[0]
            for call in self.connection.send_packet.call_args_list
            if call.args[0].packet_type == DATA
        ]
        self.assertEqual(len(sent_packets), 3)
        self.assertEqual(sent_packets[0].sequence_number, 101)
        self.assertEqual(sent_packets[1].sequence_number, 105)
        # 3rd packet sent was Fast Retransmit of SEQ 101:
        self.assertEqual(sent_packets[2].sequence_number, 101)

        # After 3 duplicate ACKs: ssthresh = max(4.0/2, 2.0) = 2.0
        # 4th duplicate ACK in Fast Recovery incremented cwnd: 5.0 -> 6.0
        # New ACK 109 exits Fast Recovery: cwnd = ssthresh = 2.0
        self.assertEqual(sender.congestion_controller.ssthresh, 2.0)
        self.assertEqual(sender.congestion_controller.cwnd, 2.0)
        self.assertEqual(
            sender.congestion_controller.state,
            CongestionState.CONGESTION_AVOIDANCE,
        )
        self.assertEqual(sender.next_sequence_number, 109)

    def test_timeout_reduces_cwnd_and_enters_slow_start(self):
        sender = self.make_sender(
            window_size=4,
            timeout=0.1,
            max_retries=2,
            segment_size=4,
        )
        sender.congestion_controller.cwnd = 6.0
        sender.congestion_controller.ssthresh = 6.0
        sender.congestion_controller.state = (
            CongestionState.CONGESTION_AVOIDANCE
        )

        calls = {"count": 0}

        def receive_with_timeout():
            calls["count"] += 1
            if calls["count"] == 1:
                time.sleep(0.12)
                raise socket.timeout()
            return self.make_ack(105)

        self.connection.receive_packet.side_effect = receive_with_timeout

        sender.send(b"data")

        # Timeout cuts ssthresh to max(6.0/2, 2.0) = 3.0, resets cwnd to 1.0 (Slow Start)
        # Retransmission succeeds on ACK 105, which increases cwnd to 2.0 in Slow Start
        self.assertEqual(sender.congestion_controller.ssthresh, 3.0)
        self.assertEqual(sender.congestion_controller.cwnd, 2.0)
        self.assertEqual(
            sender.congestion_controller.state,
            CongestionState.SLOW_START,
        )
        self.assertEqual(sender.next_sequence_number, 105)

    def test_effective_window_limits_burst_size(self):
        sender = self.make_sender(window_size=10, segment_size=4)
        # Set cwnd = 2.0, so effective_window = min(10, 2) = 2
        sender.congestion_controller.cwnd = 2.0

        burst_sizes = []

        def track_receive():
            sent = len([
                c for c in self.connection.send_packet.call_args_list
                if c.args[0].packet_type == DATA
            ])
            burst_sizes.append(sent)
            if len(burst_sizes) == 1:
                return self.make_ack(105)
            elif len(burst_sizes) == 2:
                return self.make_ack(109)
            elif len(burst_sizes) == 3:
                return self.make_ack(113)
            return self.make_ack(117)

        self.connection.receive_packet.side_effect = track_receive

        # 16 bytes = 4 segments of 4 bytes
        sender.send(b"1234567890123456")

        # Even though window_size=10 and 4 packets were to be sent,
        # effective_window=2 constrained the initial transmission to 2 segments
        self.assertEqual(burst_sizes[0], 2)
        self.assertEqual(sender.next_sequence_number, 117)

    def test_invalid_ack_does_not_advance_congestion_state(self):
        sender = self.make_sender(window_size=4, segment_size=4)
        self.assertEqual(sender.congestion_controller.cwnd, 1.0)

        # Transmits SEQ 101 (ends at 105).
        # ACK 113 acknowledges unsent bytes -> rejected, cwnd unchanged.
        # ACK 105 acknowledges SEQ 101 -> valid, cwnd advances to 2.0.
        self.connection.receive_packet.side_effect = [
            self.make_ack(113),
            self.make_ack(105),
        ]

        sender.send(b"data")

        self.assertEqual(sender.congestion_controller.cwnd, 2.0)
        self.assertEqual(sender.next_sequence_number, 105)


if __name__ == "__main__":
    unittest.main()
