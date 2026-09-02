"""Pydantic models for the cache registry."""

from enum import Enum

from pydantic import BaseModel, Field


class CacheTableType(str, Enum):
    """Enumeration of cache table data types."""

    metadata = "metadata"
    asset = "asset"
    event = "event"
    realtime = "realtime"
    platform = "platform"


class Column(BaseModel):
    """Column definition for a cache table."""

    name: str
    description: str


class CacheTable(BaseModel):
    """Cache table metadata definition."""

    name: str
    description: str
    location: str
    partitioned: bool
    partition_key: str | None = None
    type: CacheTableType
    columns: list[Column] = Field(default_factory=list)


class CacheRegistry(BaseModel):
    """Registry of cache tables."""

    tables: list[CacheTable]
