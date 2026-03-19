"""Raw to derived mapping helper."""

from zombie_squirrel.acorn_helpers.asset_basics import asset_basics
from zombie_squirrel.acorn_helpers.source_data import source_data


def raw_to_derived(
    asset_name: str | list[str],
    latest: bool = False,
    modality: str | None = None,
) -> list[str]:
    """Return derived asset names for a given raw asset name or list of names.

    Args:
        asset_name: The raw asset name(s) to look up derived assets for.
        latest: If True, for each unique (source_data, pipeline_name) pair
            return only the most recent derived asset by processing_time.
        modality: If provided, only return derived assets whose modalities
            include this exact modality abbreviation (e.g. "ecephys").

    Returns:
        List of derived asset names.

    """
    df = source_data()
    if isinstance(asset_name, list):
        matches = df[df["source_data"].isin(asset_name)].copy()
    else:
        matches = df[df["source_data"] == asset_name].copy()
    if modality is not None and not matches.empty:
        basics = asset_basics()[["name", "modalities"]]
        matches = matches.merge(basics, on="name", how="left")
        matches = matches[matches["modalities"].apply(lambda x: modality in (x or "").split(", "))]
    if latest and not matches.empty:
        matches = (
            matches.sort_values("processing_time", ascending=False)
            .groupby(["source_data", "pipeline_name"], as_index=False)
            .first()
        )
    return matches["name"].tolist()
