"""Unit tests for the platform_swdb cache tables."""

import json
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from biodata_cache.cache_table_helpers.platform_swdb import (
    SWDB_SETS,
    _date_from_name,
    _default_location,
    _events_frame,
    _https_url,
    _parse_s3,
    _subject_from_name,
    _table_frame,
    build_swdb_sessions,
    extract_swdb_asset,
    platform_swdb_events_columns,
    platform_swdb_eye_columns,
    platform_swdb_performance_columns,
    platform_swdb_running_columns,
    platform_swdb_sessions_columns,
    platform_swdb_trials,
    platform_swdb_trials_columns,
    swdb_asset_names,
    swdb_set_for_asset,
)

_ASSET = "ecephys_664851_2023-11-13_12-49-51_nwb_2026-07-24_13-29-15"


class _FakeDataset:
    """Stands in for an h5py dataset holding a 1-D array or a scalar."""

    def __init__(self, data):
        self._data = np.asarray(data) if isinstance(data, (list, np.ndarray)) else data

    def __getitem__(self, key):
        return self._data

    @property
    def dtype(self):
        return self._data.dtype

    @property
    def shape(self):
        return self._data.shape


class _FakeGroup(dict):
    """Stands in for an h5py group; supports `path in root` and `root[path]`."""

    def __contains__(self, key):
        head, _, rest = key.partition("/")
        if not dict.__contains__(self, head):
            return False
        return True if not rest else rest in dict.__getitem__(self, head)

    def __getitem__(self, key):
        head, _, rest = key.partition("/")
        node = dict.__getitem__(self, head)
        return node if not rest else node[rest]


def _group(**columns):
    """Build a fake table group from column name -> array."""
    return _FakeGroup({name: _FakeDataset(values) for name, values in columns.items()})


# --- pure helpers -------------------------------------------------------------


def test_parse_s3_splits_bucket_and_key():
    assert _parse_s3("s3://aind-open-data/asset_1/") == ("aind-open-data", "asset_1")


def test_parse_s3_rejects_non_s3_uri():
    with pytest.raises(ValueError, match="Not an S3 URI"):
        _parse_s3("https://example.com/thing")


def test_https_url_is_virtual_hosted():
    assert _https_url("buck", "a/b.nwb") == "https://buck.s3.us-west-2.amazonaws.com/a/b.nwb"


def test_default_location_uses_open_data():
    assert _default_location("asset_1") == "s3://aind-open-data/asset_1"


def test_name_parsers_extract_subject_and_date():
    assert _subject_from_name(_ASSET) == "664851"
    assert _date_from_name(_ASSET) == "2023-11-13"


def test_name_parsers_return_none_for_unparseable():
    assert _subject_from_name("not-an-asset") is None
    assert _date_from_name("not-an-asset") is None


def test_curated_set_membership():
    assert swdb_set_for_asset(_ASSET) == "dynamic-routing"
    assert swdb_set_for_asset("something_else") is None
    assert len(swdb_asset_names()) == len(SWDB_SETS["dynamic-routing"]["assets"])


def test_curated_assets_are_unique():
    names = swdb_asset_names()
    assert len(names) == len(set(names))


# --- table reading ------------------------------------------------------------


def test_table_frame_reads_requested_columns_and_decodes_bytes():
    root = _FakeGroup(
        {
            "intervals": _FakeGroup(
                {
                    "trials": _group(
                        start_time=[1.0, 2.0],
                        stim_name=np.array([b"vis1", b"sound2"], dtype=object),
                    )
                }
            )
        }
    )
    df = _table_frame(root, "intervals/trials", ["start_time", "stim_name", "absent"])
    assert list(df.columns) == ["start_time", "stim_name"]
    assert df["stim_name"].tolist() == ["vis1", "sound2"]


def test_table_frame_returns_empty_for_missing_group():
    assert _table_frame(_FakeGroup({}), "intervals/nope", ["start_time"]).empty


def test_table_frame_drops_ragged_columns():
    # index columns of a ragged NWB column have a different length and would
    # otherwise raise when assembled into a DataFrame
    group = _group(start_time=[1.0, 2.0, 3.0], tags=[b"a", b"b"])
    root = _FakeGroup({"intervals": _FakeGroup({"epochs": group})})
    df = _table_frame(root, "intervals/epochs", ["start_time", "tags"])
    assert list(df.columns) == ["start_time"]
    assert len(df) == 3


# --- event stream -------------------------------------------------------------


def _events_root():
    return _FakeGroup(
        {
            "processing": _FakeGroup(
                {
                    "behavior": _FakeGroup(
                        {
                            "licks": _group(
                                timestamps=[1.0, 2.0],
                                duration=[0.1, 0.2],
                                is_likely_lick=[True, False],
                            ),
                            "rewards": _group(timestamps=[2.5], is_solenoid_time=[True]),
                            "quiescent_interval_violations": _group(timestamps=[0.5]),
                        }
                    )
                }
            ),
            "intervals": _FakeGroup(
                {
                    "epochs": _group(
                        start_time=[0.0, 10.0],
                        stop_time=[9.0, 20.0],
                        script_name=np.array([b"RFMapping", b"DynamicRouting1"], dtype=object),
                    ),
                    "optotagging_trials": _group(
                        start_time=[11.0],
                        stop_time=[11.5],
                        location=np.array([b"VISp"], dtype=object),
                        power=[0.4],
                    ),
                    "aud_rf_mapping_trials": _group(start_time=[3.0], stop_time=[3.5], freq=[8000.0]),
                }
            ),
        }
    )


def test_events_frame_collects_every_kind_sorted_by_time():
    events = _events_frame(_events_root())
    assert set(events["kind"]) == {
        "lick",
        "reward",
        "quiescent_violation",
        "epoch",
        "opto",
        "aud_rf",
    }
    assert events["t"].is_monotonic_increasing


def test_events_frame_carries_labels_and_values():
    events = _events_frame(_events_root())
    epochs = events[events["kind"] == "epoch"]
    assert epochs["label"].tolist() == ["RFMapping", "DynamicRouting1"]
    assert epochs["t_stop"].tolist() == [9.0, 20.0]

    licks = events[events["kind"] == "lick"]
    assert licks["value"].tolist() == [1.0, 0.0]
    # lick duration becomes the event end
    assert licks["t_stop"].tolist() == [1.1, 2.2]

    opto = events[events["kind"] == "opto"]
    assert opto["label"].tolist() == ["VISp"]
    assert opto["value"].tolist() == [0.4]

    assert events[events["kind"] == "aud_rf"]["value"].tolist() == [8000.0]


def test_events_frame_tolerates_missing_optional_groups():
    # assets without optotagging_trials are a real structural variant of this set
    root = _events_root()
    del root["intervals"]["optotagging_trials"]
    events = _events_frame(root)
    assert "opto" not in set(events["kind"])
    assert "lick" in set(events["kind"])


def test_events_frame_empty_when_nothing_present():
    assert _events_frame(_FakeGroup({})).empty


# --- extraction ---------------------------------------------------------------


def _full_root():
    root = _events_root()
    root["intervals"]["trials"] = _group(
        trial_index=[0, 1],
        block_index=[0, 0],
        start_time=[1.0, 5.0],
        stop_time=[4.0, 8.0],
        stim_name=np.array([b"vis1", b"sound1"], dtype=object),
        is_hit=[True, False],
    )
    root["intervals"]["performance"] = _group(block_index=[0], hit_rate=[0.75])
    root["processing"]["behavior"]["eye_tracking"] = _group(timestamps=[0.0, 0.1], pupil_area=[10.0, 11.0])
    root["processing"]["behavior"]["running_speed"] = _group(timestamps=[0.0, 0.1], data=[1.0, 2.0])
    root["units"] = _group(id=[1, 2, 3])
    root["session_start_time"] = _FakeDataset(b"2023-11-13T12:49:51-08:00")
    root["general"] = _FakeGroup({"subject": _group(subject_id=b"664851")})
    return root


@patch("biodata_cache.cache_table_helpers.platform_swdb.registry")
@patch("biodata_cache.cache_table_helpers.platform_swdb._open_nwb")
def test_extract_writes_every_partition_and_returns_summary(mock_open, mock_registry):
    mock_registry.NAMES = {
        "swdb_sessions": "platform_swdb_sessions",
        "swdb_trials": "platform_swdb_trials",
        "swdb_performance": "platform_swdb_performance",
        "swdb_events": "platform_swdb_events",
        "swdb_eye": "platform_swdb_eye",
        "swdb_running": "platform_swdb_running",
    }
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    root = _full_root()
    root.close = MagicMock()
    mock_open.return_value = (root, None)

    summary = extract_swdb_asset(_ASSET)

    written = {c[0][0]: c[0][1] for c in mock_registry.BACKEND.write.call_args_list}
    assert set(written) == {
        f"platform_swdb_trials/{_ASSET}",
        f"platform_swdb_performance/{_ASSET}",
        f"platform_swdb_events/{_ASSET}",
        f"platform_swdb_eye/{_ASSET}",
        f"platform_swdb_running/{_ASSET}",
    }
    # running_speed's `data` column is renamed to the friendlier `speed`
    assert "speed" in written[f"platform_swdb_running/{_ASSET}"].columns

    assert summary["set_id"] == "dynamic-routing"
    assert summary["subject_id"] == "664851"
    assert summary["session_date"] == "2023-11-13"
    assert summary["n_trials"] == 2
    assert summary["n_licks"] == 2
    assert summary["n_rewards"] == 1
    assert summary["n_units"] == 3
    assert summary["has_units"] is True
    assert summary["has_optotagging"] is True
    assert summary["has_eye_tracking"] is True
    assert json.loads(summary["epochs"]) == ["RFMapping", "DynamicRouting1"]
    # duration spans the whole session, not just the trials
    assert summary["session_duration_s"] == 20.0
    root.close.assert_called_once()


@patch("biodata_cache.cache_table_helpers.platform_swdb.registry")
@patch("biodata_cache.cache_table_helpers.platform_swdb._open_nwb")
def test_extract_returns_empty_when_no_nwb_found(mock_open, mock_registry):
    mock_registry.NAMES = {"swdb_sessions": "platform_swdb_sessions"}
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_open.return_value = (None, None)

    assert extract_swdb_asset(_ASSET) == {}
    mock_registry.BACKEND.write.assert_not_called()


# --- sessions catalog ---------------------------------------------------------


@patch("biodata_cache.cache_table_helpers.platform_swdb.registry")
def test_build_sessions_merges_over_existing_rows(mock_registry):
    mock_registry.NAMES = {"swdb_sessions": "platform_swdb_sessions"}
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_registry.BACKEND.read.return_value = pd.DataFrame(
        [
            {"set_id": "dynamic-routing", "asset_name": "kept", "session_date": "2023-01-01", "n_trials": 1},
            {"set_id": "dynamic-routing", "asset_name": "rebuilt", "session_date": "2023-02-01", "n_trials": 1},
        ]
    )

    build_swdb_sessions(
        [{"set_id": "dynamic-routing", "asset_name": "rebuilt", "session_date": "2023-02-01", "n_trials": 99}]
    )

    df = mock_registry.BACKEND.write.call_args[0][1]
    # the skipped asset survives, and the rebuilt one takes the fresh value
    assert set(df["asset_name"]) == {"kept", "rebuilt"}
    assert df[df["asset_name"] == "rebuilt"]["n_trials"].iloc[0] == 99


@patch("biodata_cache.cache_table_helpers.platform_swdb.registry")
def test_build_sessions_noop_without_summaries(mock_registry):
    mock_registry.NAMES = {"swdb_sessions": "platform_swdb_sessions"}
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"

    assert build_swdb_sessions([]).empty
    mock_registry.BACKEND.write.assert_not_called()


# --- registered table behaviour ----------------------------------------------


@patch("biodata_cache.cache_table_helpers.platform_swdb.registry")
def test_trials_lazy_returns_location(mock_registry):
    mock_registry.NAMES = {"swdb_trials": "platform_swdb_trials"}
    mock_registry.BACKEND.get_location.return_value = "s3://bucket/part"

    assert platform_swdb_trials(_ASSET, lazy=True) == "s3://bucket/part"


@patch("biodata_cache.cache_table_helpers.platform_swdb.registry")
def test_trials_raises_on_empty_cache(mock_registry):
    mock_registry.NAMES = {"swdb_trials": "platform_swdb_trials"}
    mock_registry.BACKEND.read.return_value = pd.DataFrame()

    with pytest.raises(ValueError, match="Cache is empty"):
        platform_swdb_trials(_ASSET)


@patch("biodata_cache.cache_table_helpers.platform_swdb.registry")
def test_trials_returns_cached_frame(mock_registry):
    mock_registry.NAMES = {"swdb_trials": "platform_swdb_trials"}
    expected = pd.DataFrame({"trial_index": [0, 1]})
    mock_registry.BACKEND.read.return_value = expected

    pd.testing.assert_frame_equal(platform_swdb_trials(_ASSET), expected)


# --- column definitions -------------------------------------------------------


def test_column_definitions_are_named_and_described():
    for columns in (
        platform_swdb_sessions_columns(),
        platform_swdb_trials_columns(),
        platform_swdb_performance_columns(),
        platform_swdb_events_columns(),
        platform_swdb_eye_columns(),
        platform_swdb_running_columns(),
    ):
        assert columns
        assert all(c.name and c.description for c in columns)
        names = [c.name for c in columns]
        assert len(names) == len(set(names))


def test_events_columns_cover_the_long_format_schema():
    assert [c.name for c in platform_swdb_events_columns()] == ["kind", "t", "t_stop", "label", "value"]
