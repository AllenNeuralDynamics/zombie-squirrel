"""Build and upload the foraging sessions cache parquet to S3."""

import logging
import os

import numpy as np
import pandas as pd
from aind_analysis_arch_result_access.han_pipeline import get_session_table

os.environ["FOREST_TYPE"] = "s3"

import zombie_squirrel.acorns as acorns  # noqa: E402
from zombie_squirrel.utils import setup_logging  # noqa: E402

setup_logging()

FEATURES = [
    "finished_trials",
    "ignore_rate",
    "total_trials",
    "foraging_performance",
    "abs(bias_naive)",
]

STAGE_MAP = {
    "STAGE_1": "BEGINNER",
    "STAGE_1_WARMUP": "BEGINNER",
    "STAGE_2": "BEGINNER",
    "STAGE_3": "INTERMEDIATE",
    "STAGE_4": "INTERMEDIATE",
    "STAGE_FINAL": "ADVANCED",
    "GRADUATED": "ADVANCED",
}

CURRICULUM_MAP = {
    "Uncoupled Baiting": "Uncoupled Baiting",
    "Coupled Baiting": "Coupled Baiting",
    "Uncoupled Without Baiting": "Uncoupled Without Baiting",
    "Coupled Without Baiting": "Coupled Without Baiting",
}

PERCENTILE_THRESHOLDS = [
    (6.5, "SB"),
    (28.0, "B"),
    (72.0, "N"),
    (93.5, "G"),
]


def assign_strata(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["_stage_simplified"] = df["current_stage_actual"].map(STAGE_MAP)
    df["_curriculum_clean"] = df["curriculum_name"].map(CURRICULUM_MAP)
    df["strata"] = df.apply(
        lambda r: (
            f"{r['_curriculum_clean']}_{r['_stage_simplified']}_{r['curriculum_version_group']}"
            if pd.notna(r["_curriculum_clean"])
            and pd.notna(r["_stage_simplified"])
            and pd.notna(r["curriculum_version_group"])
            else None
        ),
        axis=1,
    )
    df = df.drop(columns=["_stage_simplified", "_curriculum_clean"])
    return df


def detect_outliers_iqr(series: pd.Series, factor: float = 1.5) -> pd.Series:
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - factor * iqr
    upper = q3 + factor * iqr
    return (series < lower) | (series > upper)


def compute_rolling_avg(group: pd.DataFrame, feature: str) -> pd.Series:
    group = group.sort_values("session_date")
    values = group[feature].values
    weights = group["outlier_weight"].values
    n = len(values)
    result = np.full(n, np.nan)
    for i in range(n):
        decay = np.exp(-np.arange(i, -1, -1))
        w = weights[: i + 1] * decay
        v = values[: i + 1]
        mask = ~np.isnan(v)
        if mask.sum() > 0:
            result[i] = np.average(v[mask], weights=w[mask])
    return pd.Series(result, index=group.index)


def wilson_ci(p: float, n: int, z: float = 1.96):
    if n == 0:
        return np.nan, np.nan
    denom = 1 + z**2 / n
    center = (p + z**2 / (2 * n)) / denom
    spread = z * np.sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / denom
    return max(0.0, (center - spread) * 100), min(100.0, (center + spread) * 100)


def compute_percentiles_with_ci(rolling_vals: pd.Series) -> pd.DataFrame:
    valid = rolling_vals.dropna()
    n = len(valid)
    pct = rolling_vals.rank(pct=True) * 100
    ci_lower = pd.Series(np.nan, index=rolling_vals.index)
    ci_upper = pd.Series(np.nan, index=rolling_vals.index)
    for idx in rolling_vals.index:
        if pd.isna(rolling_vals[idx]):
            continue
        p = pct[idx] / 100
        lo, hi = wilson_ci(p, n)
        ci_lower[idx] = lo
        ci_upper[idx] = hi
    return pd.DataFrame({"pct": pct, "ci_lower": ci_lower, "ci_upper": ci_upper})


def percentile_to_category(p: float) -> str:
    if pd.isna(p):
        return "NS"
    if p < 6.5:
        return "SB"
    if p < 28.0:
        return "B"
    if p < 72.0:
        return "N"
    if p < 93.5:
        return "G"
    return "SG"


def certainty(ci_lower: float, ci_upper: float, point: float) -> str:
    if pd.isna(ci_lower) or pd.isna(ci_upper) or pd.isna(point) or point == 0:
        return "uncertain"
    ratio = (ci_upper - ci_lower) / abs(point)
    if ratio <= 0.30:
        return "certain"
    if ratio <= 0.60:
        return "intermediate"
    return "uncertain"


def main():
    logging.info("Loading session table...")
    df = get_session_table(if_load_bpod=False)
    logging.info(f"Loaded {len(df)} sessions")

    df = assign_strata(df)

    df["outlier_weight"] = 1.0
    df["is_outlier"] = False

    for feature in FEATURES:
        if feature not in df.columns:
            logging.warning(f"Feature {feature} not found in session table, skipping outlier detection")
            continue
        mask = detect_outliers_iqr(df[feature].dropna())
        outlier_idx = df[feature].dropna().index[mask]
        df.loc[outlier_idx, "outlier_weight"] = 0.5
        df.loc[outlier_idx, "is_outlier"] = True

    logging.info("Computing rolling averages...")
    df = df.sort_values(["subject_id", "session_date"]).copy()
    for feature in FEATURES:
        if feature not in df.columns:
            continue
        col = f"{feature}_processed_rolling_avg"
        df[col] = np.nan
        for _, grp in df.groupby("subject_id"):
            result = compute_rolling_avg(grp, feature)
            df.loc[result.index, col] = result.values

    logging.info("Computing within-strata percentiles...")
    for feature in FEATURES:
        if feature not in df.columns:
            continue
        rolling_col = f"{feature}_processed_rolling_avg"
        pct_col = f"{feature}_session_percentile"
        lo_col = f"{feature}_session_percentile_ci_lower"
        hi_col = f"{feature}_session_percentile_ci_upper"
        df[pct_col] = np.nan
        df[lo_col] = np.nan
        df[hi_col] = np.nan
        for strata, grp in df.groupby("strata"):
            result = compute_percentiles_with_ci(grp[rolling_col])
            df.loc[result.index, pct_col] = result["pct"].values
            df.loc[result.index, lo_col] = result["ci_lower"].values
            df.loc[result.index, hi_col] = result["ci_upper"].values

    logging.info("Computing overall percentile...")
    pct_cols = [f"{f}_session_percentile" for f in FEATURES if f in df.columns]
    df["session_overall_percentile"] = df[pct_cols].mean(axis=1)
    df["session_overall_rolling_avg"] = df[[f"{f}_processed_rolling_avg" for f in FEATURES if f in df.columns]].mean(
        axis=1
    )

    ci_lower_cols = [f"{f}_session_percentile_ci_lower" for f in FEATURES if f in df.columns]
    ci_upper_cols = [f"{f}_session_percentile_ci_upper" for f in FEATURES if f in df.columns]
    df["session_overall_percentile_ci_lower"] = df[ci_lower_cols].mean(axis=1)
    df["session_overall_percentile_ci_upper"] = df[ci_upper_cols].mean(axis=1)

    df["overall_percentile_category"] = df["session_overall_percentile"].apply(percentile_to_category)

    for feature in FEATURES:
        if feature not in df.columns:
            continue
        df[f"{feature}_category"] = df[f"{feature}_session_percentile"].apply(percentile_to_category)

    logging.info("Computing certainty scores...")
    for feature in FEATURES:
        if feature not in df.columns:
            continue
        lo_col = f"{feature}_session_percentile_ci_lower"
        hi_col = f"{feature}_session_percentile_ci_upper"
        pct_col = f"{feature}_session_percentile"
        df[f"{feature}_certainty"] = df.apply(lambda r: certainty(r[lo_col], r[hi_col], r[pct_col]), axis=1)

    logging.info("Uploading to S3...")
    acorns.TREE.hide("foraging_sessions", df)
    logging.info("Done.")


if __name__ == "__main__":
    main()
