import unittest

from minitcp.rtt import RTTEstimator


class TestRTTEstimator(unittest.TestCase):

    def test_first_sample_initializes_estimates(self):
        estimator = RTTEstimator()
        rto = estimator.update(0.2)

        self.assertAlmostEqual(estimator.srtt, 0.2)
        self.assertAlmostEqual(estimator.rttvar, 0.1)
        self.assertAlmostEqual(rto, 0.6)

    def test_second_sample_updates_estimates(self):
        estimator = RTTEstimator()
        estimator.update(0.2)

        rto = estimator.update(0.3)

        expected_rttvar = 0.75 * 0.1 + 0.25 * 0.1
        expected_srtt = 0.875 * 0.2 + 0.125 * 0.3

        self.assertAlmostEqual(estimator.rttvar, expected_rttvar)
        self.assertAlmostEqual(estimator.srtt, expected_srtt)
        self.assertAlmostEqual(
            rto,
            expected_srtt + 4 * expected_rttvar,
        )

    def test_timeout_backoff_doubles_rto(self):
        estimator = RTTEstimator(initial_rto=1.0)

        self.assertEqual(estimator.backoff(), 2.0)
        self.assertEqual(estimator.backoff(), 4.0)

    def test_backoff_does_not_exceed_maximum(self):
        estimator = RTTEstimator(
            initial_rto=30.0,
            max_rto=60.0,
        )

        self.assertEqual(estimator.backoff(), 60.0)
        self.assertEqual(estimator.backoff(), 60.0)

    def test_invalid_rtt_sample_is_rejected(self):
        estimator = RTTEstimator()

        for sample in (0, -1, float("inf"), float("nan")):
            with self.subTest(sample=sample):
                with self.assertRaises(ValueError):
                    estimator.update(sample)


if __name__ == "__main__":
    unittest.main()
