"""Dynamic Routing block-switch activity, time-warped onto a standard 5-trial
template, for every QC-passing unit in the SWDB 2026 Dynamic Routing dataset.

A Dynamic Routing session alternates 6 blocks between the visual- and
auditory-rewarded task rule, so every session has exactly 5 block switches. Each
switch is one of two types: ``aud_to_vis`` or ``vis_to_aud``. "The block-switch
trials" are the 5 trials centered on the switch: the 2 trials ending the
outgoing block, the switch trial itself (the first trial of the incoming
block), and the 2 trials starting the incoming block.

Real trial durations vary instance to instance (and one session has a trial
with a multi-minute gap in it), so averaging spikes on a shared *real-time*
axis would smear together events that land at different times in different
instances -- trial 3 might be 5s long in one instance and 8s in another.
Instead, each of the 5 trials is linearly time-warped onto its own fixed-width
slot of a standard template: trial at offset ``o`` (o in -2..2) maps its own
``[start_time, stop_time)`` onto template positions ``[o+2, o+3)``, so trial
boundaries always fall at integers 0..5 regardless of real duration, and every
instance's "20% into the switch trial" lands at the same template position.
Spike counts from every instance of a given switch type within a unit's own
session are warped this way, histogrammed on the shared template grid, and
divided by the total real seconds each bin actually covers (which differs
instance to instance) to get a proper mean firing rate -- a time-warped PSTH.

`platform_swdb_dr_switch_markers` carries the same warp applied to each
trial's stimulus/response/reward events, so a viewer can draw them at the
right template position alongside the activity.

Computed once for the whole (small, 12-asset) dataset -- not part of the
nightly sync pipeline. See ``scripts/build_swdb_dr_switch.py``.
"""

import logging

import boto3
import numpy as np
import pandas as pd
from botocore.config import Config

import biodata_cache.registry as registry
from biodata_cache.cache_table_helpers.asset_basics import asset_basics
from biodata_cache.cache_table_helpers.shared.nwb_zarr import (
    download_zarr_store,
    find_nwb_prefixes,
    load_zmetadata,
    parse_s3,
)
from biodata_cache.cache_table_helpers.swdb_public_assets import SWDB_2026_DERIVED_ASSETS
from biodata_cache.models import Column
from biodata_cache.utils import CacheLogMessage, setup_logging

# The 5 "block-switch trials": 2 trials ending the outgoing block, the switch
# trial itself (first trial of the incoming block, template slot _PRE_TRIALS),
# and 2 trials starting the incoming block.
_PRE_TRIALS = 2
_POST_TRIALS = 2
_N_TRIALS = _PRE_TRIALS + 1 + _POST_TRIALS
_N_PHASE_BINS = 60  # per trial; ~90ms/bin at a typical ~5.5s trial duration
_N_BINS = _N_TRIALS * _N_PHASE_BINS
_MAX_WORKERS = 32

_TRIAL_COLUMNS = [
    "block_index", "rewarded_modality", "start_time", "stop_time",
    "stim_start_time", "stim_stop_time", "response_time", "reward_time",
    "is_response", "is_rewarded",
]
_UNIT_COLUMNS = ["unit_id", "is_qc_pass"]

DR_ASSET_NAMES = SWDB_2026_DERIVED_ASSETS["dr"]

# Template x-axis: trial slot boundaries fall at integers 0.._N_TRIALS; every
# switch instance's 5 real trials are linearly warped onto these 5 unit-width
# slots regardless of their real duration. Shared across every asset/direction.
TEMPLATE_EDGES = np.arange(_N_BINS + 1) / _N_PHASE_BINS


def _log(message: str) -> None:
    """Emit a structured cache log message for the swdb_dr_switch table."""
    logging.info(
        CacheLogMessage(
            backend=registry.BACKEND.__class__.__name__,
            table=registry.NAMES["swdb_dr_switch"],
            message=message,
        ).to_json()
    )


def _read_group_columns(client, bucket: str, nwb_prefix: str, zmetadata: bytes, metadata: dict, group: str, columns: list[str]) -> dict | None:
    """Read named scalar arrays from one zarr group (e.g. ``intervals/trials`` or ``units``)."""
    paths = [f"{group}/{c}" for c in columns if f"{group}/{c}/.zarray" in metadata]
    if len(paths) != len(columns):
        return None
    import zarr

    store = download_zarr_store(client, bucket, nwb_prefix, zmetadata, paths, max_workers=_MAX_WORKERS)
    root = zarr.open_consolidated(store, mode="r")
    node = root
    for part in group.split("/"):
        node = node[part]
    return {c: np.asarray(node[c][:]) for c in columns}


def _trial_frame(trials: dict, idx: int) -> dict | None:
    """Return one trial's real span and its events, warped to a 0..1 phase within it.

    Returns None for a degenerate trial (non-positive duration), which should
    never happen in real data but is guarded against rather than trusted.
    """
    start = float(trials["start_time"][idx])
    stop = float(trials["stop_time"][idx])
    duration = stop - start
    if not (duration > 0):
        return None
    has_response = bool(trials["is_response"][idx])
    has_reward = bool(trials["is_rewarded"][idx])
    return {
        "start": start,
        "duration": duration,
        "stim_on": (float(trials["stim_start_time"][idx]) - start) / duration,
        "stim_off": (float(trials["stim_stop_time"][idx]) - start) / duration,
        "response": ((float(trials["response_time"][idx]) - start) / duration) if has_response else None,
        "reward": ((float(trials["reward_time"][idx]) - start) / duration) if has_reward else None,
    }


def _find_switches(trials: dict) -> list[dict]:
    """Return the 5 block-switch instances from a session's trial columns.

    Each instance carries the direction label and `trials`: {offset: frame} for
    offset -2..+2 (see _trial_frame), the per-instance data _switch_markers()
    and _asset_rows() warp onto the shared template.
    """
    block = trials["block_index"]
    modality = trials["rewarded_modality"]
    n = len(block)

    switch_indices = [i for i in range(1, n) if block[i] != block[i - 1]]
    switches = []
    for switch_idx in switch_indices:
        lo = switch_idx - _PRE_TRIALS
        hi = switch_idx + _POST_TRIALS
        if lo < 0 or hi >= n:
            _log(f"Skipping switch at trial {switch_idx}: not enough trials for the 5-trial window")
            continue
        direction = f"{modality[lo]}_to_{modality[switch_idx]}"
        trial_frames = {}
        degenerate = False
        for offset in range(-_PRE_TRIALS, _POST_TRIALS + 1):
            frame = _trial_frame(trials, switch_idx + offset)
            if frame is None:
                degenerate = True
                break
            trial_frames[offset] = frame
        if degenerate:
            _log(f"Skipping switch at trial {switch_idx}: degenerate trial duration")
            continue
        switches.append({"direction": direction, "trials": trial_frames})
    return switches


def _switch_markers(switches_by_asset: dict) -> pd.DataFrame:
    """Return each direction's stim/response/reward events, warped onto the template.

    One row per (direction, trial_offset): the median template position of that
    trial's stimulus onset/offset, and (when present) response and reward,
    across every switch instance of that direction in the dataset. Median, not
    mean, is used defensively even though the warp already removes most
    session-to-session duration skew.
    """
    by_key: dict[tuple[str, int], list[dict]] = {}
    for switches in switches_by_asset.values():
        for switch in switches:
            for offset, frame in switch["trials"].items():
                slot = offset + _PRE_TRIALS
                by_key.setdefault((switch["direction"], offset), []).append(
                    {
                        "stim_on_x": slot + frame["stim_on"],
                        "stim_off_x": slot + frame["stim_off"],
                        "response_x": (slot + frame["response"]) if frame["response"] is not None else None,
                        "reward_x": (slot + frame["reward"]) if frame["reward"] is not None else None,
                    }
                )

    rows = []
    for (direction, offset), entries in by_key.items():
        responses = np.array([e["response_x"] for e in entries if e["response_x"] is not None])
        rewards = np.array([e["reward_x"] for e in entries if e["reward_x"] is not None])
        rows.append(
            {
                "direction": direction,
                "trial_offset": offset,
                "stim_on_x": float(np.median([e["stim_on_x"] for e in entries])),
                "stim_off_x": float(np.median([e["stim_off_x"] for e in entries])),
                "response_x": float(np.median(responses)) if responses.size else None,
                "response_frac": responses.size / len(entries),
                "reward_x": float(np.median(rewards)) if rewards.size else None,
                "reward_frac": rewards.size / len(entries),
                "n_instances": len(entries),
            }
        )
    return pd.DataFrame(rows).sort_values(["direction", "trial_offset"]).reset_index(drop=True)


def _seconds_by_direction(switches: list[dict]) -> dict:
    """Return, per direction, the real seconds each template bin covers.

    Every instance's trial at a given offset always contributes its full
    duration/_N_PHASE_BINS to each of that slot's _N_PHASE_BINS bins (the warp
    is linear), so this depends only on trial timing -- shared across every
    unit in the asset.
    """
    seconds: dict[str, np.ndarray] = {}
    for switch in switches:
        arr = seconds.setdefault(switch["direction"], np.zeros(_N_BINS))
        for offset, frame in switch["trials"].items():
            slot = offset + _PRE_TRIALS
            start_bin = slot * _N_PHASE_BINS
            arr[start_bin : start_bin + _N_PHASE_BINS] += frame["duration"] / _N_PHASE_BINS
    return seconds


def _discover_asset(location: str) -> tuple[str, str, dict, bytes, dict, list[dict]] | None:
    """Find the NWB store carrying both trials and units for one asset.

    Returns (bucket, nwb_prefix, metadata, zmetadata, unit_cols, switches), or None if
    no NWB in this asset has both a usable trials table and a units group.
    """
    bucket, key = parse_s3(location)
    client = boto3.client("s3", config=Config(max_pool_connections=_MAX_WORKERS))
    for nwb_prefix in find_nwb_prefixes(client, bucket, key):
        loaded = load_zmetadata(client, bucket, nwb_prefix)
        if loaded is None:
            continue
        zmetadata, metadata = loaded
        trial_cols = _read_group_columns(client, bucket, nwb_prefix, zmetadata, metadata, "intervals/trials", _TRIAL_COLUMNS)
        if trial_cols is None:
            continue
        unit_cols = _read_group_columns(client, bucket, nwb_prefix, zmetadata, metadata, "units", _UNIT_COLUMNS)
        if unit_cols is None:
            continue
        switches = _find_switches(trial_cols)
        if not switches:
            continue
        return bucket, nwb_prefix, metadata, zmetadata, unit_cols, switches
    return None


def _asset_rows(
    asset_name: str, bucket: str, nwb_prefix: str, zmetadata: bytes, unit_cols: dict, switches: list[dict]
) -> pd.DataFrame:
    """Return the (unit_id, direction, bin) mean-rate rows for one asset."""
    client = boto3.client("s3", config=Config(max_pool_connections=_MAX_WORKERS))
    qc_pass = np.flatnonzero(unit_cols["is_qc_pass"])
    if len(qc_pass) == 0:
        _log(f"No QC-passing units for asset {asset_name}")
        return pd.DataFrame()

    spike_arrays = download_zarr_store(
        client,
        bucket,
        nwb_prefix,
        zmetadata,
        ["units/spike_times", "units/spike_times_index"],
        max_workers=_MAX_WORKERS,
    )
    import zarr

    spike_root = zarr.open_consolidated(spike_arrays, mode="r")["units"]
    spike_times = np.asarray(spike_root["spike_times"][:])
    spike_index = np.asarray(spike_root["spike_times_index"][:])

    seconds_by_direction = _seconds_by_direction(switches)
    n_instances_by_direction: dict[str, int] = {}
    for switch in switches:
        n_instances_by_direction[switch["direction"]] = n_instances_by_direction.get(switch["direction"], 0) + 1

    rows = []
    for unit_pos in qc_pass:
        unit_id = unit_cols["unit_id"][unit_pos]
        start = 0 if unit_pos == 0 else int(spike_index[unit_pos - 1])
        stop = int(spike_index[unit_pos])
        unit_spikes = spike_times[start:stop]
        if unit_spikes.size == 0:
            continue

        spike_counts_by_direction: dict[str, np.ndarray] = {}
        for switch in switches:
            direction = switch["direction"]
            counts = spike_counts_by_direction.setdefault(direction, np.zeros(_N_BINS))
            template_positions = []
            for offset, frame in switch["trials"].items():
                slot = offset + _PRE_TRIALS
                trial_start = frame["start"]
                trial_stop = trial_start + frame["duration"]
                in_trial = (unit_spikes >= trial_start) & (unit_spikes < trial_stop)
                if not in_trial.any():
                    continue
                phases = (unit_spikes[in_trial] - trial_start) / frame["duration"]
                template_positions.append(slot + phases)
            if template_positions:
                hist, _ = np.histogram(np.concatenate(template_positions), bins=TEMPLATE_EDGES)
                counts += hist

        for direction, counts in spike_counts_by_direction.items():
            seconds = seconds_by_direction[direction]
            has_data = seconds > 0
            if not has_data.any():
                continue
            bin_idx = np.flatnonzero(has_data)
            mean_rate = counts[bin_idx] / seconds[bin_idx]
            rows.append(
                pd.DataFrame(
                    {
                        "asset_name": asset_name,
                        "unit_id": unit_id,
                        "direction": direction,
                        "bin_index": bin_idx,
                        "template_x": TEMPLATE_EDGES[bin_idx] + (0.5 / _N_PHASE_BINS),
                        "mean_rate_hz": mean_rate,
                        "n_instances": n_instances_by_direction[direction],
                    }
                )
            )

    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


@registry.register_table(registry.NAMES["swdb_dr_switch"])
def platform_swdb_dr_switch(force_update: bool = False) -> pd.DataFrame:
    """Return the Dynamic Routing block-switch activity table, building it on demand."""
    df = registry.BACKEND.read(registry.NAMES["swdb_dr_switch"])

    if df.empty and not force_update:
        raise ValueError("Cache is empty. Use force_update=True to fetch data from S3.")

    if df.empty or force_update:
        setup_logging()
        _log("Updating cache")

        basics = asset_basics()
        location_by_name = dict(zip(basics["name"], basics["location"], strict=False))

        discovered = {}
        for asset_name in DR_ASSET_NAMES:
            location = location_by_name.get(asset_name)
            if not location:
                _log(f"No asset_basics location for {asset_name}; skipping")
                continue
            try:
                found = _discover_asset(location)
            except Exception as exc:
                logging.exception(f"swdb_dr_switch discovery failed for asset {asset_name}: {exc}")
                continue
            if found is None:
                _log(f"No usable trials/units NWB found for {asset_name}; skipping")
                continue
            discovered[asset_name] = found

        if not discovered:
            raise RuntimeError("No Dynamic Routing asset produced usable block switches")

        # A byproduct of the same discovery pass: representative event timing per
        # direction, warped onto the same template as the activity below.
        switches_by_asset = {name: found[5] for name, found in discovered.items()}
        markers_df = _switch_markers(switches_by_asset)
        registry.BACKEND.write(registry.NAMES["swdb_dr_switch_markers"], markers_df)
        _log(f"Wrote {len(markers_df)} trial-event marker rows")

        frames = []
        for asset_name, (bucket, nwb_prefix, _metadata, zmetadata, unit_cols, switches) in discovered.items():
            try:
                asset_df = _asset_rows(asset_name, bucket, nwb_prefix, zmetadata, unit_cols, switches)
            except Exception as exc:
                logging.exception(f"swdb_dr_switch failed for asset {asset_name}: {exc}")
                continue
            if not asset_df.empty:
                frames.append(asset_df)
            _log(f"{asset_name}: {len(asset_df)} rows")

        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
            columns=["asset_name", "unit_id", "direction", "bin_index", "template_x", "mean_rate_hz", "n_instances"]
        )
        registry.BACKEND.write(registry.NAMES["swdb_dr_switch"], df)

    return df


def platform_swdb_dr_switch_columns() -> list[Column]:
    return [
        Column(name="asset_name", description="Derived Dynamic Routing asset name"),
        Column(name="unit_id", description="Unit identifier; joinable with platform_ecephys_units.unit_id"),
        Column(name="direction", description="Block-switch type: 'aud_to_vis' or 'vis_to_aud'"),
        Column(name="bin_index", description="Index into the shared template bin grid (60 bins per trial)"),
        Column(name="template_x", description="Bin center on the template x-axis: 0..5, where each integer is a trial boundary (0=start of trial -2, 2=switch trial onset, 5=end of trial +2). Every switch instance's 5 real trials are linearly time-warped onto this axis regardless of their real duration."),
        Column(name="mean_rate_hz", description="Mean firing rate in this bin, averaged (by real seconds covered, not instance count) across every switch of this direction in the unit's own session"),
        Column(name="n_instances", description="Number of switch instances of this direction in the unit's own session"),
    ]


@registry.register_table(registry.NAMES["swdb_dr_switch_markers"])
def platform_swdb_dr_switch_markers(force_update: bool = False) -> pd.DataFrame:
    """Return representative trial-event template positions per direction, building it on demand.

    Written as a byproduct of `platform_swdb_dr_switch()` (both tables come from
    the same per-instance trial frames), so `force_update` here simply triggers
    that same builder.
    """
    df = registry.BACKEND.read(registry.NAMES["swdb_dr_switch_markers"])

    if df.empty and not force_update:
        raise ValueError("Cache is empty. Use force_update=True to fetch data from S3.")

    if df.empty or force_update:
        platform_swdb_dr_switch(force_update=True)
        df = registry.BACKEND.read(registry.NAMES["swdb_dr_switch_markers"])

    return df


def platform_swdb_dr_switch_markers_columns() -> list[Column]:
    return [
        Column(name="direction", description="Block-switch type: 'aud_to_vis' or 'vis_to_aud'"),
        Column(name="trial_offset", description="Trial position relative to the switch trial: -2..-1 end the outgoing block, 0 is the switch trial, +1..+2 start the incoming block"),
        Column(name="stim_on_x", description="Median template-x position (see platform_swdb_dr_switch.template_x) of this trial's stimulus onset"),
        Column(name="stim_off_x", description="Median template-x position of this trial's stimulus offset"),
        Column(name="response_x", description="Median template-x position of this trial's response, in instances that had one (null if none did)"),
        Column(name="response_frac", description="Fraction of instances of this direction where this trial had a response"),
        Column(name="reward_x", description="Median template-x position of this trial's reward delivery, in instances that had one (null if none did)"),
        Column(name="reward_frac", description="Fraction of instances of this direction where this trial was rewarded"),
        Column(name="n_instances", description="Number of switch instances (across every session) this marker is averaged over"),
    ]
