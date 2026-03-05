"""Pydantic models for Squirrel metadata."""

from enum import Enum

from pydantic import BaseModel


class AcornType(str, Enum):
    """Enumeration of acorn data types."""

    metadata = "metadata"
    asset = "asset"
    event = "event"
    realtime = "realtime"


class Column(BaseModel):
    """Column definition for acorn metadata."""

    name: str
    description: str


class Acorn(BaseModel):
    """Acorn metadata definition."""

    name: str
    description: str
    location: str
    partitioned: bool
    partition_key: str | None = None
    type: AcornType
    columns: list[Column] = []


class Squirrel(BaseModel):
    """Squirrel metadata container for acorns."""

    acorns: list[Acorn]
