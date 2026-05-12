"""Foraging sessions acorn column definitions."""

from zombie_squirrel.squirrel import Column


def foraging_sessions_columns() -> list[Column]:
    return [
        Column(name="subject_id", description="Subject identifier"),
        Column(name="session_date", description="Date of the session"),
        Column(name="session", description="Session number"),
        Column(name="strata", description="Strata assignment (curriculum_stage_version)"),
        Column(name="current_stage_actual", description="Actual curriculum stage"),
        Column(name="curriculum_name", description="Curriculum name"),
        Column(name="curriculum_version_group", description="Curriculum version group (v1/v2/v3)"),
        Column(name="PI", description="Principal investigator"),
        Column(name="trainer", description="Trainer name"),
        Column(name="rig", description="Rig identifier"),
        Column(name="water_day_total", description="Total water for the day"),
        Column(name="base_weight", description="Base weight of subject"),
        Column(name="target_weight", description="Target weight of subject"),
        Column(name="weight_after", description="Weight after session"),
        Column(name="finished_trials", description="Number of finished trials"),
        Column(name="ignore_rate", description="Rate of ignored trials"),
        Column(name="total_trials", description="Total number of trials"),
        Column(name="foraging_performance", description="Foraging performance metric"),
        Column(name="abs(bias_naive)", description="Absolute naive bias"),
        Column(name="finished_rate", description="Rate of finished trials"),
        Column(name="outlier_weight", description="Weight assigned for outlier status (0.5 if outlier, 1.0 otherwise)"),
        Column(name="is_outlier", description="Whether the session was flagged as an outlier"),
        Column(name="session_overall_percentile", description="Overall session percentile rank within strata"),
        Column(name="overall_percentile_category", description="Category based on overall percentile (SB/B/N/G/SG)"),
        Column(name="session_overall_rolling_avg", description="Rolling average of overall percentile"),
    ]
