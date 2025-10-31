# forest_cache/acorns.py
from abc import ABC, abstractmethod
import pandas as pd
import os

from aind_data_access_api.rds_tables import Client, RDSCredentials


def prefix_table_name(table_name: str) -> str:
    return "zs_" + table_name


class Acorn(ABC):
    """Base class for a storage backend (the cache)."""

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def hide(self, table_name: str, data: pd.DataFrame) -> None:
        """Store records in the cache."""
        pass

    @abstractmethod
    def scurry(self, table_name: str) -> pd.DataFrame:
        """Fetch records from the cache."""
        pass


class RedshiftAcorn(Acorn):
    """Stores and retrieves caches using aind-data-access-api
    Redshift Client"""

    def __init__(self) -> None:
        REDSHIFT_SECRETS = os.getenv("REDSHIFT_SECRETS", "/aind/prod/redshift/credentials/readwrite")
        self.rds_client = Client(
            credentials=RDSCredentials(aws_secrets_name=REDSHIFT_SECRETS),
        )

    def hide(self, table_name: str, data: pd.DataFrame) -> None:
        self.rds_client.overwrite_table_with_df(
            df=data,
            table_name=prefix_table_name(table_name),
        )

    def scurry(self, table_name: str) -> pd.DataFrame:
        return self.rds_client.read_table(table_name=prefix_table_name(table_name))


class MemoryAcorn(Acorn):
    """A simple in-memory backend for testing or local development."""
    def __init__(self) -> None:
        super().__init__()
        self._store: dict[str, pd.DataFrame] = {}

    def hide(self, table_name: str, data: pd.DataFrame) -> None:
        self._store[table_name] = data

    def scurry(self, table_name: str) -> pd.DataFrame:
        return self._store.get(table_name, pd.DataFrame())
