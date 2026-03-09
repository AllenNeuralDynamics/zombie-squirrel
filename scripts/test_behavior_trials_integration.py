"""Integration test for the behavior_trials acorn against a known asset."""

import unittest

import pandas as pd

from zombie_squirrel import behavior_trials

SUBJECT_ID = "828418"
ASSET_NAME = "828418_2026-02-13_18-31-13_processed_2026-02-14_12-44-45"


class TestBehaviorTrialsIntegration(unittest.TestCase):
    """Integration tests for behavior_trials acorn."""

    @classmethod
    def setUpClass(cls):
        cls.df = behavior_trials(
            subject_id=SUBJECT_ID,
            asset_names=ASSET_NAME,
            force_update=True,
        )

    def test_returns_dataframe(self):
        self.assertIsInstance(self.df, pd.DataFrame)

    def test_not_empty(self):
        self.assertGreater(len(self.df), 0)

    def test_required_columns_present(self):
        for col in ("asset_name", "subject_id", "id", "start_time", "stop_time"):
            self.assertIn(col, self.df.columns)

    def test_asset_name_matches(self):
        self.assertTrue((self.df["asset_name"] == ASSET_NAME).all())

    def test_subject_id_matches(self):
        self.assertTrue((self.df["subject_id"] == SUBJECT_ID).all())

    def test_trial_times_are_numeric(self):
        self.assertTrue(pd.api.types.is_numeric_dtype(self.df["start_time"]))
        self.assertTrue(pd.api.types.is_numeric_dtype(self.df["stop_time"]))

    def test_stop_time_after_start_time(self):
        self.assertTrue((self.df["stop_time"] >= self.df["start_time"]).all())


if __name__ == "__main__":
    unittest.main()
