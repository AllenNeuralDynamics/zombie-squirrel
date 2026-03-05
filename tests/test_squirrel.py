"""Unit tests for zombie_squirrel.squirrel module."""

import json
import unittest

from zombie_squirrel.acorn_helpers.asset_basics import asset_basics_columns
from zombie_squirrel.acorn_helpers.qc import qc_columns
from zombie_squirrel.acorn_helpers.source_data import source_data_columns
from zombie_squirrel.acorn_helpers.unique_project_names import unique_project_names_columns
from zombie_squirrel.acorn_helpers.unique_subject_ids import unique_subject_ids_columns
from zombie_squirrel.squirrel import Acorn, AcornType, Column, Squirrel


class TestAcornType(unittest.TestCase):
    """Test AcornType enumeration."""

    def test_metadata_value(self):
        """Test metadata type value."""
        self.assertEqual(AcornType.metadata.value, "metadata")

    def test_asset_value(self):
        """Test asset type value."""
        self.assertEqual(AcornType.asset.value, "asset")

    def test_event_value(self):
        """Test event type value."""
        self.assertEqual(AcornType.event.value, "event")

    def test_realtime_value(self):
        """Test realtime type value."""
        self.assertEqual(AcornType.realtime.value, "realtime")

    def test_all_types_count(self):
        """Test total number of acorn types."""
        self.assertEqual(len(AcornType), 4)


class TestAcorn(unittest.TestCase):
    """Test Acorn class."""

    def _make_acorn(self, **kwargs):
        """Create test acorn with defaults."""
        defaults = {
            "name": "test_acorn",
            "description": "A test acorn",
            "location": "s3://bucket/path/file.pqt",
            "partitioned": False,
            "type": AcornType.metadata,
            "columns": [Column(name="col1", description=""), Column(name="col2", description="")],
        }
        defaults.update(kwargs)
        return Acorn(**defaults)

    def test_basic_creation(self):
        """Test basic acorn creation and attributes."""
        acorn = self._make_acorn()
        self.assertEqual(acorn.name, "test_acorn")
        self.assertEqual(acorn.location, "s3://bucket/path/file.pqt")
        self.assertFalse(acorn.partitioned)
        self.assertIsNone(acorn.partition_key)
        self.assertEqual(acorn.type, AcornType.metadata)
        self.assertEqual(acorn.columns, [Column(name="col1", description=""), Column(name="col2", description="")])

    def test_partitioned_with_key(self):
        """Test acorn with partitioning key."""
        acorn = self._make_acorn(
            partitioned=True,
            partition_key="subject_id",
            type=AcornType.asset,
        )
        self.assertTrue(acorn.partitioned)
        self.assertEqual(acorn.partition_key, "subject_id")

    def test_partition_key_defaults_none(self):
        """Test partition key defaults to None."""
        acorn = self._make_acorn()
        self.assertIsNone(acorn.partition_key)

    def test_asset_type(self):
        """Test acorn with asset type."""
        acorn = self._make_acorn(type=AcornType.asset)
        self.assertEqual(acorn.type, AcornType.asset)

    def test_event_type(self):
        """Test acorn with event type."""
        acorn = self._make_acorn(type=AcornType.event)
        self.assertEqual(acorn.type, AcornType.event)

    def test_realtime_type(self):
        """Test acorn with realtime type."""
        acorn = self._make_acorn(type=AcornType.realtime)
        self.assertEqual(acorn.type, AcornType.realtime)

    def test_serialization_includes_type_value(self):
        """Test serialized acorn includes type value."""
        acorn = self._make_acorn(type=AcornType.metadata)
        data = json.loads(acorn.model_dump_json())
        self.assertEqual(data["type"], "metadata")

    def test_serialization_includes_all_fields(self):
        """Test serialized acorn includes all fields."""
        acorn = self._make_acorn()
        data = json.loads(acorn.model_dump_json())
        for field in ("name", "description", "location", "partitioned", "partition_key", "type", "columns"):
            self.assertIn(field, data)

    def test_columns_preserved(self):
        """Test acorn columns are preserved."""
        cols = [
            Column(name="_id", description=""),
            Column(name="_last_modified", description=""),
            Column(name="subject_id", description=""),
        ]
        acorn = self._make_acorn(columns=cols)
        self.assertEqual(acorn.columns, cols)


class TestSquirrel(unittest.TestCase):
    """Test Squirrel class."""

    def _make_acorn(self, name="a"):
        """Create test acorn with specified name."""
        return Acorn(
            name=name,
            description="A test acorn",
            location="s3://bucket/path.pqt",
            partitioned=False,
            type=AcornType.metadata,
            columns=[Column(name="col1", description="")],
        )

    def test_empty_acorns(self):
        """Test squirrel with empty acorns list."""
        squirrel = Squirrel(acorns=[])
        self.assertEqual(squirrel.acorns, [])

    def test_single_acorn(self):
        """Test squirrel with single acorn."""
        acorn = self._make_acorn()
        squirrel = Squirrel(acorns=[acorn])
        self.assertEqual(len(squirrel.acorns), 1)

    def test_multiple_acorns(self):
        """Test squirrel with multiple acorns."""
        acorns = [self._make_acorn(name=f"acorn_{i}") for i in range(3)]
        squirrel = Squirrel(acorns=acorns)
        self.assertEqual(len(squirrel.acorns), 3)

    def test_serialization_top_level_key(self):
        """Test serialized squirrel has acorns top-level key."""
        squirrel = Squirrel(acorns=[self._make_acorn()])
        data = json.loads(squirrel.model_dump_json())
        self.assertIn("acorns", data)
        self.assertEqual(len(data["acorns"]), 1)

    def test_serialization_roundtrip(self):
        """Test squirrel serialization and deserialization."""
        acorn = Acorn(
            name="quality_control",
            description="QC data per subject",
            location="s3://bucket/qc/",
            partitioned=True,
            partition_key="subject_id",
            type=AcornType.asset,
            columns=[Column(name="name", description=""), Column(name="stage", description="")],
        )
        squirrel = Squirrel(acorns=[acorn])
        data = json.loads(squirrel.model_dump_json())
        restored = Squirrel.model_validate(data)
        self.assertEqual(restored.acorns[0].name, "quality_control")
        self.assertEqual(restored.acorns[0].partition_key, "subject_id")
        self.assertTrue(restored.acorns[0].partitioned)


class TestColumnsHelpers(unittest.TestCase):
    """Test column helper functions."""

    def test_unique_project_names_columns(self):
        """Test unique project names columns."""
        cols = unique_project_names_columns()
        self.assertIsInstance(cols, list)
        names = [c.name for c in cols]
        self.assertIn("project_name", names)

    def test_unique_subject_ids_columns(self):
        """Test unique subject IDs columns."""
        cols = unique_subject_ids_columns()
        self.assertIsInstance(cols, list)
        names = [c.name for c in cols]
        self.assertIn("subject_id", names)

    def test_asset_basics_columns(self):
        """Test asset basics columns."""
        cols = asset_basics_columns()
        self.assertIsInstance(cols, list)
        names = [c.name for c in cols]
        for expected in ("_id", "_last_modified", "subject_id", "modalities", "project_name"):
            self.assertIn(expected, names)

    def test_source_data_columns(self):
        """Test source data columns."""
        cols = source_data_columns()
        self.assertIsInstance(cols, list)
        names = [c.name for c in cols]
        self.assertIn("source_data", names)

    def test_qc_columns(self):
        """Test QC columns."""
        cols = qc_columns()
        self.assertIsInstance(cols, list)
        names = [c.name for c in cols]
        for expected in ("name", "stage", "status", "asset_name"):
            self.assertIn(expected, names)


if __name__ == "__main__":
    unittest.main()
