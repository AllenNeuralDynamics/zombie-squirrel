"""Explore the storage lens RDS table.

Usage:
    source ~/.zshrc && switch prod
    source .venv/bin/activate && python scripts/explore_storage_lens.py
"""

from pathlib import Path

import pandas as pd
from aind_data_access_api.rds_tables import RDSCredentials
from aind_data_access_api.secrets import get_secret
from sqlalchemy import create_engine, engine, text

SECRET_NAME = "/aind/prod/rds/storage-lens-metrics/credentials/readonly"
SSL_CERT = str(Path(__file__).parent.parent / "global-bundle.pem")


def get_engine(creds: RDSCredentials):
    connection_url = engine.URL.create(
        drivername="postgresql",
        username=creds.username,
        password=creds.password.get_secret_value(),
        host=creds.host,
        database=creds.database,
        port=creds.port,
    )
    return create_engine(
        connection_url,
        connect_args={
            "sslmode": "verify-full",
            "sslrootcert": SSL_CERT,
        },
    )


def main():
    print(f"Fetching credentials from {SECRET_NAME} ...")
    secret = get_secret(SECRET_NAME)
    print("Secret keys:", list(secret.keys()))

    allowed = {"username", "password", "host", "port", "dbname", "database"}
    creds = RDSCredentials(**{k: v for k, v in secret.items() if k in allowed})
    print(f"Connecting to {creds.host}:{creds.port}/{creds.database} as {creds.username}")

    eng = get_engine(creds)

    with eng.connect() as conn:
        # List all tables
        tables = pd.read_sql_query(
            text(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                "ORDER BY table_schema, table_name"
            ),
            conn,
        )
        print("\n=== Tables ===")
        print(tables.to_string())

        # Read all tables
        for _, row in tables.iterrows():
            schema = row["table_schema"]
            tname = row["table_name"]
            print(f"\n=== {schema}.{tname} (first 5 rows) ===")
            df = pd.read_sql_query(
                text(f'SELECT * FROM "{schema}"."{tname}" LIMIT 5'),
                conn,
            )
            print(df.to_string())
            print(f"\nShape: {df.shape}")
            print(f"Columns: {list(df.columns)}")


if __name__ == "__main__":
    main()
