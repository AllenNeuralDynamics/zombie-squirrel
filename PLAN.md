# Zombie-Squirrel Backend — Foraging Sessions Cache Table Plan

## Context

This plan describes a new cache table to be added to the zombie-squirrel backend (the repository responsible for periodically building and uploading pre-computed parquet files to `s3://allen-data-views/data-asset-cache/`). These parquet files are then queried directly by the zombie web app via DuckDB-WASM.

The data being cached comes from AIND's dynamic foraging behavioral experiments. A reference implementation of the full statistical pipeline exists at https://github.com/arielleleon/aind_dashboard — a standalone Dash/Plotly Python app that runs this same pipeline at startup (which takes 5+ minutes). The goal here is to run that pipeline offline, on a schedule, and store the results so the browser gets pre-scored data with no startup delay.

The pipeline pulls raw session data from S3 via the `aind_analysis_arch_result_access` Python package, groups subjects into experimental condition cohorts ("strata") for fair comparison, computes outlier-weighted exponential-decay rolling averages of behavioral metrics, and scores each session with a within-strata percentile rank plus Wilson confidence intervals. The output is a single flat parquet file — one row per session — with both the raw measurements and all computed scores.

---

## Step 1 — Add `aind-analysis-arch-result-access` dependency

Add `aind-analysis-arch-result-access>=0.7.4` to the zombie `pyproject.toml` dependencies is a new cache extra dependency group, necessary when running force_update for this new acorn.

## Step 2 — Create the cache builder script

Create `scripts/build_foraging_cache.py`. This script:

1. **Loads raw session data**:
   ```python
   from aind_analysis_arch_result_access.han_pipeline import get_session_table
   df = get_session_table(if_load_bpod=False)
   ```

2. **Assigns strata** to each session based on curriculum type, stage, and version:
   - **Curriculum types**: Map `curriculum_name` → `Uncoupled Baiting`, `Coupled Baiting`, `Uncoupled Without Baiting`, `Coupled Without Baiting`
   - **Stage simplification**: Map `current_stage_actual` → `BEGINNER` (stages 1–2), `INTERMEDIATE` (stages 3–4), `ADVANCED` (stage final, graduated)
   - **Version grouping**: Map curriculum version → `v1`, `v2`, `v3`
   - **Strata ID**: `"{curriculum}_{stage}_{version}"` (e.g. `"Uncoupled Baiting_ADVANCED_v3"`)

   Implement this logic from scratch (or copy from the reference implementation at https://github.com/arielleleon/aind_dashboard — see `app_utils/strata_utils.py` and `app_utils/app_analysis/pipeline_manager.py`). The mappings are fully specified above.

3. **Runs the reference processor** (feature standardization + outlier detection):
   - For each of the 5 core features (`finished_trials`, `ignore_rate`, `total_trials`, `foraging_performance`, `abs(bias_naive)`):
     - Detect outlier sessions via IQR method (factor=1.5)
     - Assign `outlier_weight` (0.5 for outliers, 1.0 otherwise)
     - Set `is_outlier` flag

4. **Computes exponential-decay rolling averages** per subject per feature:
   - For each feature, compute a weighted rolling average across sessions (ordered by date) using exponential decay weighting
   - Write to `{feature}_processed_rolling_avg`

5. **Computes within-strata percentiles** using Wilson confidence intervals:
   - For each feature, within each strata group, compute percentile ranks of the rolling averages
   - Compute Wilson CI bounds for each percentile estimate
   - Write `{feature}_session_percentile`, `{feature}_session_percentile_ci_lower`, `{feature}_session_percentile_ci_upper`

6. **Computes overall percentile** per session:
   - Average of all 5 feature percentiles → `session_overall_percentile`
   - Similarly compute overall rolling avg and CIs
   - Assign `overall_percentile_category`:
     - `< 6.5` → `SB`, `6.5–28` → `B`, `28–72` → `N`, `72–93.5` → `G`, `> 93.5` → `SG`
   - Assign `{feature}_category` per feature using the same thresholds

7. **Computes certainty scores** per feature:
   - `ci_width / abs(point_estimate)` ratio
   - `≤ 0.30` → `certain`, `0.30–0.60` → `intermediate`, `≥ 0.60` → `uncertain`

8. **Writes parquet to S3**:
   Using the standard acorn pattern

## Step 3 — Register the acorn in squirrel.json

```json
{
  "name": "foraging_sessions",
  "location": "s3://allen-data-views/data-asset-cache/zs_foraging_sessions.pqt",
  "type": "metadata",
  "columns": [
    "subject_id", "session_date", "session", "strata",
    "current_stage_actual", "curriculum_name", "PI", "trainer", "rig",
    "session_overall_percentile", "overall_percentile_category",
    "session_overall_rolling_avg",
    "finished_trials", "ignore_rate", "total_trials",
    "foraging_performance", "abs(bias_naive)",
    "water_day_total", "base_weight", "target_weight", "weight_after",
    "finished_rate", "outlier_weight", "is_outlier"
  ]
}
```

(The `columns` array is for metadata/documentation — DuckDB reads all columns from the parquet regardless.)

## Step 4 — Test the cache output

After running the builder once:
1. Download the parquet and inspect schema with `scripts/inspect_qc_parquet.py` (adapt the key)
2. Verify all expected columns are present
3. Verify `session_overall_percentile` values are 0–100
4. Verify `overall_percentile_category` values are in `{SB, B, N, G, SG, NS}`
5. Verify per-feature percentile and CI columns are populated
6. Verify row count matches expected subject×session count
