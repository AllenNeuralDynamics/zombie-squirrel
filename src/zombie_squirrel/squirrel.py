"""Pydantic models for Squirrel metadata."""

from enum import Enum

from pydantic import BaseModel


class AcornType(str, Enum):
    metadata = "metadata"
    asset = "asset"
    event = "event"
    realtime = "realtime"


class Acorn(BaseModel):
    name: str
    location: str
    partitioned: bool
    partition_key: str | None = None
    type: AcornType
    columns: list[str]


class Squirrel(BaseModel):
    acorns: list[Acorn]
