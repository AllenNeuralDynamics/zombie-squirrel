# Camstim video sync alignment — plan

A shared problem across every acquisition that used **camstim** (Allen
`stim`/mesoscope + behavior stack). It affects, at minimum:

- **mFISH / multiplane-ophys** (this new viewer),
- **Dynamic Routing** (already uses a NI-DAQ sync file),
- **some Dynamic Foraging** camstim sessions.

The current behavior-video playback (`web/src/lib/behaviors/playback-video.js`)
syncs each `<video>` to the session timeline using **a single scalar offset**
(`t0`, or a Harp `ReferenceTime` from a per-camera `metadata.csv`). That is
correct for the Harp-based rigs (VR foraging etc.) but **wrong for camstim
rigs**, where:

- cameras free-run at ~60 fps but with **jitter and dropped frames**, so frame
  _i_ is not at `t0 + i/fps`;
- the true per-frame time comes from the **NI-DAQ sync file** (`sync.h5`), which
  records a rising edge per camera exposure.

## What the sync file contains (verified)

Raw asset `behavior/<id>_sync.h5` — classic AllenSDK sync format:

- `data`: (N, 2) uint32 — `[sample_index, bit_state]` transition list.
- `meta`: Python-dict string with:
  - `ni_daq.sample_rate = 100000.0` (100 kHz), `counter_output_freq`,
  - `line_labels`: bit → label.

Relevant `line_labels` for this mFISH session:

| bit | label | meaning |
|-----|-------|---------|
| 0 | `vsync_2p` | 2-photon frame clock (ophys) |
| 2 | `vsync_stim` | stimulus monitor vsync |
| 4 | `stim_photodiode` | photodiode on stim monitor |
| 21 | `beh_cam_frame_readout` | Behavior camera frame |
| 22 | `face_cam_frame_readout` | Face camera frame |
| 25 | `eye_cam_frame_readout` | Eye camera frame |
| 26 | `nose_cam_frame_readout` | Nose camera frame |
| 27–30 | `*_cam_exposing` | per-camera exposure gate |
| 31 | `lick_sensor` | licks |

Per-camera frame times = rising edges of `<cam>_cam_frame_readout` (or the
`_exposing` line), in seconds = `sample_index / 100000`, then shifted onto the
**session clock** (the same clock the NWB timestamps use — for this session the
NWB series start at ~11.86 s, so the sync sample stream needs the matching
zero/offset that the pipeline already applied to ophys + behavior).

Frame count sanity: video JSON reports `FramesRecorded` (e.g. Behavior 57706,
Eye 57691) at ~60.0024 fps for a 16-min recording — matches the number of
`_frame_readout` rising edges (minus any dropped frames, which is exactly why a
scalar offset is insufficient).

## Why not read sync.h5 in the browser

- It is ~8 MB and encoded as a transition list at 100 kHz; decoding rising
  edges per line client-side is wasteful and slow on every page load.
- The alignment logic (which line, dropped-frame handling, session-clock
  offset) is pipeline knowledge better computed once, server-side.

## Proposed solution: precomputed per-camera frame-time sidecars

For each affected asset, precompute and cache a compact **per-camera frame-time
array** aligned to the session clock:

- One array per camera: `float32[n_frames]` = session-clock time of each video
  frame. (Dropped frames simply absent → array length matches the mp4 frame
  count.)
- Storage options (decide with cache side): a small `.npy`/binary blob, a
  parquet with columns `camera, frame_index, t`, or a JSON of typed arrays.
  Binary (Float32) is smallest; ~57k frames × 4 B ≈ 230 KB/camera.
- Location: alongside the other caches (distributed registry), keyed by raw
  asset name, e.g. `video_frame_times/<raw_asset>/<camera>.f32`.

### Deriving the arrays (server-side)

1. Read `sync.h5` (`data`, `meta`).
2. For each camera line, compute rising-edge sample indices → seconds
   (`/sample_rate`).
3. Apply the same session-clock zero the pipeline used for ophys/behavior
   (so frame times share the NWB clock). For camstim/ophys this is typically
   defined via `vsync_2p` / photodiode / a barcode; **must match** whatever the
   NWB alignment used so video, calcium, and behavior overlay correctly.
4. Reconcile against the mp4 frame count from the video JSON `FramesRecorded`
   (handle any `LostFrames`).
5. Emit one array per camera.

## Consumer changes (zombie, separate PR)

`web/src/lib/behaviors/playback-video.js` currently supports a scalar `t0`.
Extend the video descriptor to optionally accept a **frame-time array**:

- If a frame-time array is present for a camera, seek by
  `frameIndex = binarySearch(frameTimes, sessionTime)` and set
  `video.currentTime = frameIndex / nominalFps` (or store the mp4's own PTS if
  needed). This makes scrubbing/playback frame-accurate.
- If absent, fall back to the existing scalar-offset behavior.
- Camera name mapping: camstim cameras are `Behavior`, `Eye`, `Face`, `Nose`
  (see raw `behavior-videos/`), which are **not** in the current
  `DEFAULT_CAMERAS` probe list — add them (or drive discovery from the cache
  manifest).

## Rollout

Because the fix is shared, precompute the frame-time sidecars for **all**
affected assets (mFISH + Dynamic Routing + camstim Dynamic Foraging) in one
cache pass, then land the single `playback-video.js` change that consumes them.
Until then, camstim videos play with approximate (start-time + nominal-fps)
sync, which is visibly good enough for coarse review but not frame-accurate.

## Open questions

1. What defines session-clock zero in the existing NWB alignment for these
   assets (so the sidecar uses the identical offset)? Confirm against the
   pipeline that wrote the NWB timestamps.
2. Sidecar format: raw Float32 blob vs parquet vs JSON typed arrays?
3. Do we also want `vsync_stim` / `stim_photodiode`-derived precise stimulus
   times cached, or is the NWB `grating_presentations` table already the
   aligned source of truth? (For mFISH the NWB interval table is aligned, so
   probably not needed.)
