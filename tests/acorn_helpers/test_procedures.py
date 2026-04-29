"""Unit tests for procedures acorn."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

import zombie_squirrel.acorns as acorns
from zombie_squirrel.acorn_helpers.procedures import (
    _axis_names_from_coord_sys,
    _coord_systems_from_procedures,
    _extract_first_dynamics,
    _extract_injection_row,
    _extract_translation_by_axes,
    _serialize_materials,
    brain_injections,
    procedures,
)
from zombie_squirrel.forest import MemoryTree

SAMPLE_RECORD = {
    "_id": "abc123",
    "subject": {"subject_id": "813992"},
    "procedures": {
        "object_type": "Procedures",
        "subject_id": "813992",
        "coordinate_system": {
            "object_type": "Coordinate system",
            "name": "BREGMA_ARID",
            "origin": "Bregma",
            "axes": [
                {"object_type": "Axis", "name": "AP", "direction": "Posterior_to_anterior"},
                {"object_type": "Axis", "name": "ML", "direction": "Left_to_right"},
                {"object_type": "Axis", "name": "SI", "direction": "Superior_to_inferior"},
                {"object_type": "Axis", "name": "Depth", "direction": "Up_to_down"},
            ],
            "axis_unit": "millimeter",
        },
        "subject_procedures": [
            {
                "object_type": "Surgery",
                "start_date": "2025-09-24",
                "procedures": [
                    {
                        "object_type": "Headframe",
                        "headframe_type": "AI Straight bar",
                    },
                    {
                        "object_type": "Brain injection",
                        "injection_materials": [
                            {
                                "object_type": "Viral material",
                                "name": "AAV-test",
                                "titer": 1e13,
                                "titer_unit": "gc/mL",
                            }
                        ],
                        "targeted_structure": {
                            "atlas": "CCFv3",
                            "name": "Nucleus accumbens",
                            "acronym": "ACB",
                            "id": "56",
                        },
                        "relative_position": ["Left"],
                        "dynamics": [
                            {
                                "object_type": "Injection dynamics",
                                "profile": "Bolus",
                                "volume": 300,
                                "volume_unit": "nanoliter",
                                "duration": None,
                                "duration_unit": "minute",
                            }
                        ],
                        "protocol_id": "dx.doi.org/10.17504/protocols.io.test",
                        "coordinate_system_name": "BREGMA_ARID",
                        "coordinates": [
                            [
                                {
                                    "object_type": "Translation",
                                    "translation": [1.3, -1.8, 0, 4.4],
                                }
                            ]
                        ],
                    },
                ],
            }
        ],
    },
}


class TestAxisHelpers(unittest.TestCase):
    """Tests for coordinate axis helper functions."""

    def test_axis_names_from_coord_sys(self):
        cs = {"axes": [{"name": "AP"}, {"name": "ML"}, {"name": "SI"}, {"name": "Depth"}]}
        self.assertEqual(_axis_names_from_coord_sys(cs), ["AP", "ML", "SI", "Depth"])

    def test_axis_names_empty(self):
        self.assertEqual(_axis_names_from_coord_sys({}), [])

    def test_coord_systems_from_procedures_top_level(self):
        proc_block = {
            "coordinate_system": {
                "name": "BREGMA_ARID",
                "axes": [{"name": "AP"}, {"name": "ML"}, {"name": "SI"}, {"name": "Depth"}],
            }
        }
        result = _coord_systems_from_procedures(proc_block, {})
        self.assertIn("BREGMA_ARID", result)
        self.assertEqual(result["BREGMA_ARID"], ["AP", "ML", "SI", "Depth"])

    def test_coord_systems_surgery_overrides(self):
        proc_block = {}
        surgery = {
            "coordinate_system": {
                "name": "CUSTOM",
                "axes": [{"name": "X"}, {"name": "Y"}],
            }
        }
        result = _coord_systems_from_procedures(proc_block, surgery)
        self.assertIn("CUSTOM", result)

    def test_extract_translation_by_axes(self):
        coords = [[{"object_type": "Translation", "translation": [1.3, -1.8, 0.0, 4.4]}]]
        result = _extract_translation_by_axes(coords, ["AP", "ML", "SI", "Depth"])
        self.assertEqual(result, {"AP": 1.3, "ML": -1.8, "SI": 0.0, "Depth": 4.4})

    def test_extract_translation_no_translation(self):
        coords = [[{"object_type": "Rotation", "rotation": [0, 0, 1, 45]}]]
        result = _extract_translation_by_axes(coords, ["AP", "ML"])
        self.assertEqual(result, {"AP": None, "ML": None})

    def test_extract_translation_empty_coords(self):
        result = _extract_translation_by_axes([], ["AP", "ML"])
        self.assertEqual(result, {"AP": None, "ML": None})

    def test_extract_first_dynamics(self):
        d = [{"profile": "Bolus", "volume": 300, "volume_unit": "nanoliter", "duration": None}]
        result = _extract_first_dynamics(d)
        self.assertEqual(result["injection_profile"], "Bolus")
        self.assertEqual(result["injection_volume"], 300)
        self.assertEqual(result["injection_volume_unit"], "nanoliter")

    def test_extract_first_dynamics_empty(self):
        result = _extract_first_dynamics([])
        self.assertIsNone(result["injection_profile"])
        self.assertIsNone(result["injection_volume"])
        self.assertIsNone(result["injection_volume_unit"])


class TestSerializeMaterials(unittest.TestCase):
    """Tests for _serialize_materials helper."""

    def test_empty(self):
        self.assertEqual(_serialize_materials([]), "")

    def test_single_material(self):
        m = [{"name": "AAV-test", "titer": 1e13}]
        self.assertEqual(_serialize_materials(m), "AAV-test")

    def test_multiple_materials(self):
        m = [{"name": "AAV-GCaMP"}, {"name": "AAV-ChRmine"}]
        self.assertEqual(_serialize_materials(m), "AAV-GCaMP; AAV-ChRmine")


class TestExtractInjectionRow(unittest.TestCase):
    """Tests for _extract_injection_row helper."""

    def setUp(self):
        self.coord_sys_map = {"BREGMA_ARID": ["AP", "ML", "SI", "Depth"]}

    def test_brain_injection_row(self):
        proc = SAMPLE_RECORD["procedures"]["subject_procedures"][0]["procedures"][1]
        row = _extract_injection_row("813992_0_1", "813992", "2025-09-24", proc, self.coord_sys_map)
        self.assertEqual(row["procedure_key"], "813992_0_1")
        self.assertEqual(row["subject_id"], "813992")
        self.assertEqual(row["surgery_start_date"], "2025-09-24")
        self.assertEqual(row["procedure_type"], "Brain injection")
        self.assertEqual(row["targeted_structure_acronym"], "ACB")
        self.assertEqual(row["targeted_structure_name"], "Nucleus accumbens")
        self.assertEqual(row["relative_position"], "Left")
        self.assertEqual(row["coordinate_system_name"], "BREGMA_ARID")
        self.assertEqual(row["AP"], 1.3)
        self.assertEqual(row["ML"], -1.8)
        self.assertEqual(row["SI"], 0)
        self.assertEqual(row["Depth"], 4.4)
        self.assertIn("AAV-test", row["injection_materials"])
        self.assertEqual(row["injection_profile"], "Bolus")
        self.assertEqual(row["injection_volume"], 300)
        self.assertEqual(row["injection_volume_unit"], "nanoliter")

    def test_missing_targeted_structure(self):
        proc = {"object_type": "Brain injection", "targeted_structure": None, "dynamics": [], "injection_materials": []}
        row = _extract_injection_row("sub1_0_0", "sub1", "2025-01-01", proc, {})
        self.assertEqual(row["targeted_structure_name"], "")
        self.assertEqual(row["targeted_structure_acronym"], "")

    def test_unknown_coord_sys_gives_no_axis_columns(self):
        proc = {
            "object_type": "Brain injection",
            "coordinate_system_name": "UNKNOWN",
            "coordinates": [[{"object_type": "Translation", "translation": [1, 2, 3, 4]}]],
            "dynamics": [],
            "injection_materials": [],
        }
        row = _extract_injection_row("sub1_0_0", "sub1", "2025-01-01", proc, {})
        self.assertNotIn("AP", row)


class TestProceduresAcorn(unittest.TestCase):
    """Tests for procedures() acorn function."""

    def setUp(self):
        acorns.TREE = MemoryTree()

    @patch("zombie_squirrel.acorn_helpers.procedures.MetadataDbClient")
    def test_procedures_force_update(self, mock_client_class):
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = [SAMPLE_RECORD]

        df = procedures(force_update=True)

        self.assertFalse(df.empty)
        self.assertIn("procedure_key", df.columns)
        self.assertIn("procedure_type", df.columns)
        self.assertIn("Headframe", df["procedure_type"].values)
        self.assertIn("Brain injection", df["procedure_type"].values)
        self.assertEqual(df.iloc[0]["surgery_start_date"], "2025-09-24")

    @patch("zombie_squirrel.acorn_helpers.procedures.MetadataDbClient")
    def test_procedures_cache_hit(self, mock_client_class):
        cached_df = pd.DataFrame({"subject_id": ["813992"], "procedure_type": ["Headframe"]})
        acorns.TREE.hide("procedures", cached_df)

        df = procedures(force_update=False)

        self.assertEqual(len(df), 1)
        mock_client_class.assert_not_called()

    def test_procedures_empty_cache_raises(self):
        with self.assertRaises(ValueError):
            procedures(force_update=False)

    @patch("zombie_squirrel.acorn_helpers.procedures.MetadataDbClient")
    def test_procedures_no_records_found(self, mock_client_class):
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = []

        df = procedures(force_update=True)

        self.assertTrue(df.empty)

    @patch("zombie_squirrel.acorn_helpers.procedures.MetadataDbClient")
    def test_procedures_deduplicates_by_subject_id(self, mock_client_class):
        """Multiple records for the same subject should only yield one pass through surgery data."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        duplicate = dict(SAMPLE_RECORD)
        duplicate["_id"] = "other-id"
        mock_client_instance.retrieve_docdb_records.return_value = [SAMPLE_RECORD, duplicate]

        df = procedures(force_update=True)

        self.assertEqual(len(df), 2)


class TestBrainInjectionsAcorn(unittest.TestCase):
    """Tests for brain_injections() acorn function."""

    def setUp(self):
        acorns.TREE = MemoryTree()

    @patch("zombie_squirrel.acorn_helpers.procedures.MetadataDbClient")
    def test_brain_injections_force_update(self, mock_client_class):
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = [SAMPLE_RECORD]

        df = brain_injections(force_update=True)

        self.assertFalse(df.empty)
        self.assertIn("targeted_structure_acronym", df.columns)
        self.assertIn("procedure_key", df.columns)
        self.assertEqual(df.iloc[0]["targeted_structure_acronym"], "ACB")
        self.assertEqual(df.iloc[0]["AP"], 1.3)
        self.assertEqual(df.iloc[0]["injection_profile"], "Bolus")
        self.assertEqual(df.iloc[0]["injection_volume"], 300)

    @patch("zombie_squirrel.acorn_helpers.procedures.MetadataDbClient")
    def test_brain_injections_cache_hit(self, mock_client_class):
        cached_df = pd.DataFrame({"subject_id": ["813992"], "targeted_structure_acronym": ["ACB"]})
        acorns.TREE.hide("brain_injections", cached_df)

        df = brain_injections(force_update=False)

        self.assertEqual(len(df), 1)
        mock_client_class.assert_not_called()

    def test_brain_injections_empty_cache_raises(self):
        with self.assertRaises(ValueError):
            brain_injections(force_update=False)

    @patch("zombie_squirrel.acorn_helpers.procedures.MetadataDbClient")
    def test_no_injections_returns_empty(self, mock_client_class):
        """Record with only non-injection procedures yields empty injections table."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        record_no_injections = {
            "_id": "xyz",
            "subject": {"subject_id": "000001"},
            "procedures": {
                "subject_procedures": [
                    {
                        "object_type": "Surgery",
                        "start_date": "2025-01-01",
                        "procedures": [{"object_type": "Headframe"}],
                    }
                ]
            },
        }
        mock_client_instance.retrieve_docdb_records.return_value = [record_no_injections]

        df = brain_injections(force_update=True)

        self.assertTrue(df.empty)


if __name__ == "__main__":
    unittest.main()
