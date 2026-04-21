"""Raw to derived mapping helper."""

from zombie_squirrel.acorn_helpers.asset_basics import asset_basics
from zombie_squirrel.acorn_helpers.source_data import source_data


def raw_to_derived(
    asset_name: str, latest: bool = False, modality: str = None, force_update: bool = False
) -> list[str]:
    """Return derived asset names for a given raw asset name.

    Args:
        asset_name: The raw asset name to look up derived assets for.
        latest: If True, for each unique pipeline_name return only the most
            recent derived asset by processing_time.
        modality: If provided, filter derived assets to those with this modality.
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        List of derived asset names.

    """
    df = source_data(force_update=force_update)
    matches = df[df["source_data"] == asset_name].copy()
    if modality is not None:
        basics = asset_basics()
        modality_names = basics[basics["modalities"].str.contains(modality, na=False)]["name"]
        matches = matches[matches["name"].isin(modality_names)]
    if latest and not matches.empty:
        matches = (
            matches.sort_values("processing_time", ascending=False).groupby("pipeline_name", as_index=False).first()
        )
    return matches["name"].tolist()
