"""Integration tests for procedures and brain_injections acorns against S3."""

import unittest

import boto3
import pandas as pd

from zombie_squirrel.acorns import NAMES

BUCKET = "allen-data-views"
PROCEDURES_KEY = f"data-asset-cache/zs_{NAMES['procedures']}.pqt"
INJECTIONS_KEY = f"data-asset-cache/zs_{NAMES['injections']}.pqt"
TEST_SUBJECT = "813992"


def _s3_key_exists(key: str) -> bool:
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


class TestProceduresS3(unittest.TestCase):
    """Integration tests for the procedures acorn on S3."""

    def test_file_exists(self):
        self.assertTrue(_s3_key_exists(PROCEDURES_KEY), f"No procedures file found at s3://{BUCKET}/{PROCEDURES_KEY}")

    def test_has_expected_columns(self):
        import os

        os.environ["FOREST_TYPE"] = "s3"
        from zombie_squirrel.acorns import ACORN_REGISTRY

        df = ACORN_REGISTRY[NAMES["procedures"]](force_update=False)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        for col in ("procedure_key", "subject_id", "surgery_start_date", "procedure_type"):
            self.assertIn(col, df.columns, f"Missing column: {col}")

    def test_procedure_keys_are_unique(self):
        import os

        os.environ["FOREST_TYPE"] = "s3"
        from zombie_squirrel.acorns import ACORN_REGISTRY

        df = ACORN_REGISTRY[NAMES["procedures"]](force_update=False)
        self.assertEqual(df["procedure_key"].nunique(), len(df), "procedure_key values are not unique")

    def test_contains_test_subject(self):
        import os

        os.environ["FOREST_TYPE"] = "s3"
        from zombie_squirrel.acorns import ACORN_REGISTRY

        df = ACORN_REGISTRY[NAMES["procedures"]](force_update=False)
        self.assertIn(TEST_SUBJECT, df["subject_id"].values, f"Subject {TEST_SUBJECT} not found in procedures table")


class TestBrainInjectionsS3(unittest.TestCase):
    """Integration tests for the brain_injections acorn on S3."""

    def test_file_exists(self):
        self.assertTrue(_s3_key_exists(INJECTIONS_KEY), f"No brain_injections file found at s3://{BUCKET}/{INJECTIONS_KEY}")

    def test_has_expected_columns(self):
        import os

        os.environ["FOREST_TYPE"] = "s3"
        from zombie_squirrel.acorns import ACORN_REGISTRY

        df = ACORN_REGISTRY[NAMES["injections"]](force_update=False)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        for col in (
            "procedure_key",
            "subject_id",
            "surgery_start_date",
            "procedure_type",
            "targeted_structure_acronym",
            "injection_profile",
            "injection_volume",
            "injection_volume_unit",
        ):
            self.assertIn(col, df.columns, f"Missing column: {col}")

    def test_procedure_keys_join_to_procedures_table(self):
        """Every procedure_key in brain_injections must appear in procedures."""
        import os

        os.environ["FOREST_TYPE"] = "s3"
        from zombie_squirrel.acorns import ACORN_REGISTRY

        proc_df = ACORN_REGISTRY[NAMES["procedures"]](force_update=False)
        inj_df = ACORN_REGISTRY[NAMES["injections"]](force_update=False)

        orphans = set(inj_df["procedure_key"]) - set(proc_df["procedure_key"])
        self.assertEqual(orphans, set(), f"brain_injections has procedure_keys not in procedures: {orphans}")

    def test_contains_brain_injections(self):
        import os

        os.environ["FOREST_TYPE"] = "s3"
        from zombie_squirrel.acorns import ACORN_REGISTRY

        df = ACORN_REGISTRY[NAMES["injections"]](force_update=False)
        self.assertTrue(
            (df["procedure_type"] == "Brain injection").any(),
            "No 'Brain injection' rows found in brain_injections table",
        )


if __name__ == "__main__":
    unittest.main()
