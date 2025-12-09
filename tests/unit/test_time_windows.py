import unittest

from emissions_tracker.tracker import SECONDS_PER_DAY, BittensorEmissionTracker


class ResolveTimeWindowTests(unittest.TestCase):
    def test_lookback_takes_priority_over_last_timestamp(self):
        now = 10 * SECONDS_PER_DAY
        start, end = BittensorEmissionTracker._resolve_time_window(
            "contract income",
            last_timestamp=999999,
            lookback_days=5,
            now=now,
        )

        self.assertEqual(end, now)
        self.assertEqual(start, now - (5 * SECONDS_PER_DAY))

    def test_prior_timestamp_used_when_lookback_missing(self):
        start, end = BittensorEmissionTracker._resolve_time_window(
            "sales",
            last_timestamp=12345,
            lookback_days=None,
            now=20000,
        )

        self.assertEqual(start, 12346)
        self.assertEqual(end, 20000)

    def test_requires_lookback_when_no_prior_timestamp(self):
        with self.assertRaises(ValueError):
            BittensorEmissionTracker._resolve_time_window(
                "transfers", last_timestamp=0, lookback_days=None, now=100
            )

    def test_rejects_non_positive_lookback_values(self):
        with self.assertRaises(ValueError):
            BittensorEmissionTracker._resolve_time_window(
                "income", last_timestamp=123, lookback_days=0, now=1000
            )
        with self.assertRaises(ValueError):
            BittensorEmissionTracker._resolve_time_window(
                "income", last_timestamp=123, lookback_days=-1, now=1000
            )


if __name__ == "__main__":
    unittest.main()
