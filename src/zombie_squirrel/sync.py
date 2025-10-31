"""Sync all acorns"""
from .squirrels import SQUIRREL_REGISTRY


def hide_acorns():
    for squirrel in SQUIRREL_REGISTRY.values():
        squirrel(force_update=True)
