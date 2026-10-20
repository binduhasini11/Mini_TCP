import unittest

from minitcp.congestion import (
    CongestionController,
    CongestionState,
)


class TestCongestionController(unittest.TestCase):

    def test_starts_in_slow_start(self):
        controller = CongestionController()

        self.assertEqual(controller.cwnd, 1.0)
        self.assertEqual(
            controller.state,
            CongestionState.SLOW_START,
        )

    def test_slow_start_increases_cwnd(self):
        controller = CongestionController(
            initial_cwnd=1,
            initial_ssthresh=4,
        )

        controller.on_new_ack()
        self.assertEqual(controller.cwnd, 2.0)

        controller.on_new_ack()
        self.assertEqual(controller.cwnd, 3.0)

        controller.on_new_ack()
        self.assertEqual(controller.cwnd, 4.0)

        self.assertEqual(
            controller.state,
            CongestionState.CONGESTION_AVOIDANCE,
        )

    def test_congestion_avoidance_grows_gradually(self):
        controller = CongestionController(
            initial_cwnd=4,
            initial_ssthresh=4,
        )

        controller.on_new_ack()

        self.assertAlmostEqual(controller.cwnd, 4.25)
        self.assertEqual(
            controller.state,
            CongestionState.CONGESTION_AVOIDANCE,
        )

    def test_three_duplicate_acks_trigger_fast_retransmit(self):
        controller = CongestionController(
            initial_cwnd=10,
            initial_ssthresh=20,
        )

        self.assertFalse(controller.on_duplicate_ack())
        self.assertFalse(controller.on_duplicate_ack())

        should_retransmit = controller.on_duplicate_ack()

        self.assertTrue(should_retransmit)
        self.assertEqual(controller.ssthresh, 5.0)
        self.assertEqual(controller.cwnd, 8.0)
        self.assertEqual(
            controller.state,
            CongestionState.FAST_RECOVERY,
        )

    def test_timeout_resets_congestion_window(self):
        controller = CongestionController(
            initial_cwnd=10,
            initial_ssthresh=20,
        )

        controller.on_timeout()

        self.assertEqual(controller.cwnd, 1.0)
        self.assertEqual(controller.ssthresh, 5.0)
        self.assertEqual(
            controller.state,
            CongestionState.SLOW_START,
        )

    def test_new_ack_exits_fast_recovery(self):
        controller = CongestionController(
            initial_cwnd=10,
            initial_ssthresh=20,
        )

        for _ in range(3):
            controller.on_duplicate_ack()

        controller.on_new_ack()

        self.assertEqual(controller.cwnd, 5.0)
        self.assertEqual(
            controller.state,
            CongestionState.CONGESTION_AVOIDANCE,
        )


if __name__ == "__main__":
    unittest.main()