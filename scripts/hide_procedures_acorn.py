"""Run the procedures and brain_injections hide_acorn for all subjects in one pass."""

from zombie_squirrel.acorns import ACORN_REGISTRY, NAMES


def main():
    """Hide procedures and brain_injections acorns for all subjects."""
    print("Fetching procedures data for all subjects...")
    ACORN_REGISTRY[NAMES["procedures"]](force_update=True)
    print("Procedures cache update complete.")


if __name__ == "__main__":
    main()
