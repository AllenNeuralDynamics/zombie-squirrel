"""Storage Lens weekly report cache table."""

import logging
import os
import ssl
import tempfile
import urllib.request
from importlib.resources import files

import pandas as pd

import biodata_cache.registry as registry
from biodata_cache.models import Column
from biodata_cache.utils import CacheLogMessage, setup_logging

_SECRET_NAME = os.getenv(
    "STORAGE_LENS_SECRET_NAME",
    "/aind/prod/rds/storage-lens-metrics/credentials/readonly",
)
_CERT_URL = "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"
_CERT_PATH = os.path.join(tempfile.gettempdir(), "global-bundle.pem")


def _get_ssl_cert() -> str:
    """Return a filesystem path to the RDS global CA bundle.

    Resolution order:
    1. STORAGE_LENS_SSL_CERT env var (explicit override)
    2. Cert bundled with this package (always present after pip install)
    3. Download from AWS truststore (fallback for editable installs)
    """
    env_path = os.getenv("STORAGE_LENS_SSL_CERT")
    if env_path and os.path.exists(env_path):
        return env_path
    # Try the cert bundled as package data (works in Docker / any standard install)
    try:
        cert_bytes = files("biodata_cache").joinpath("global-bundle.pem").read_bytes()
        with open(_CERT_PATH, "wb") as fh:
            fh.write(cert_bytes)
        return _CERT_PATH
    except (FileNotFoundError, TypeError, ModuleNotFoundError):
        pass
    # Last resort: download from AWS
    if not os.path.exists(_CERT_PATH):
        try:
            import certifi
            ctx = ssl.create_default_context(cafile=certifi.where())
        except ImportError:
            ctx = ssl._create_unverified_context()
        with urllib.request.urlopen(_CERT_URL, context=ctx) as resp:
            with open(_CERT_PATH, "wb") as fh:
                fh.write(resp.read())
    return _CERT_PATH


def _fetch_storage_lens() -> pd.DataFrame:
    from aind_data_access_api.rds_tables import RDSCredentials
    from aind_data_access_api.secrets import get_secret
    from sqlalchemy import create_engine, engine, text

    secret = get_secret(_SECRET_NAME)
    allowed = {"username", "password", "host", "port", "dbname", "database"}
    creds = RDSCredentials(**{k: v for k, v in secret.items() if k in allowed})

    connection_url = engine.URL.create(
        drivername="postgresql",
        username=creds.username,
        password=creds.password.get_secret_value(),
        host=creds.host,
        database=creds.database,
        port=creds.port,
    )
    eng = create_engine(
        connection_url,
        connect_args={
            "sslmode": "verify-full",
            "sslrootcert": _get_ssl_cert(),
        },
    )
    with eng.connect() as conn:
        chunks = pd.read_sql_query(
            text("SELECT * FROM weekly_report"),
            conn,
            chunksize=50_000,
        )
        return pd.concat(chunks, ignore_index=True)


@registry.register_table(registry.NAMES["storage_lens"])
def storage_lens(force_update: bool = False) -> pd.DataFrame:
    df = registry.BACKEND.read(registry.NAMES["storage_lens"])

    if df.empty and not force_update:
        raise ValueError("Cache is empty. Use force_update=True to fetch data from database.")

    if df.empty or force_update:
        setup_logging()
        logging.info(
            CacheLogMessage(
                backend=registry.BACKEND.__class__.__name__,
                table=registry.NAMES["storage_lens"],
                message="Updating cache",
            ).to_json()
        )
        df = _fetch_storage_lens()
        registry.BACKEND.write(registry.NAMES["storage_lens"], df)

    return df


def storage_lens_columns() -> list[Column]:
    return [
        Column(name="prefix", description="S3 object prefix (often a Code Ocean dataset UUID)"),
        Column(name="bucket", description="S3 bucket name"),
        Column(name="subprefix", description="More specific S3 subpath within the prefix"),
        Column(name="storage_class", description="S3 storage class (e.g. STANDARD, INTELLIGENT_TIERING)"),
        Column(
            name="intelligent_tiering_access_tier",
            description="Intelligent Tiering access tier if applicable",
        ),
        Column(name="size_in_bytes", description="Total storage size in bytes"),
        Column(name="number_of_files", description="Number of files in the prefix"),
        Column(name="project_name", description="Associated project name if available"),
        Column(name="report_date", description="ISO timestamp of the weekly Storage Lens report"),
    ]
