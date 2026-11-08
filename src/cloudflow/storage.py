"""DuckDB schemas and atomic writes shared by the local pipeline."""

from collections.abc import Iterator
from contextlib import contextmanager

import duckdb

from cloudflow.contracts import (
    GOLD_COLUMNS, GOLD_KEYS, METADATA_COLUMNS, RAW_COLUMNS, SILVER_COLUMNS, SILVER_KEYS,
)


@contextmanager
def transaction(connection: duckdb.DuckDBPyConnection) -> Iterator[None]:
    connection.execute("BEGIN TRANSACTION")
    try:
        yield
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


def create_table(connection, name, columns, keys):
    definitions = [f'"{column}" {sql_type}' for column, sql_type in columns.items()]
    definitions.append(f"PRIMARY KEY ({', '.join(keys)})")
    connection.execute(f"CREATE TABLE IF NOT EXISTS {name} ({', '.join(definitions)})")


def insert_frame(connection, name, frame, *, ignore_conflicts=False):
    if frame.empty:
        return
    columns = ", ".join(f'"{column}"' for column in frame.columns)
    conflict_clause = " ON CONFLICT DO NOTHING" if ignore_conflicts else ""
    connection.register("_cloudflow_input", frame)
    try:
        connection.execute(
            f"INSERT INTO {name} ({columns}) SELECT {columns} FROM _cloudflow_input{conflict_clause}"
        )
    finally:
        connection.unregister("_cloudflow_input")


def initialize_database(connection):
    connection.execute("SET TimeZone = 'UTC'")
    for schema in ("bronze", "silver", "staging", "gold", "observability"):
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    connection.execute("""
        CREATE TABLE IF NOT EXISTS bronze.batches (
            batch_id VARCHAR PRIMARY KEY,
            source VARCHAR NOT NULL,
            checksums JSON NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            input_path VARCHAR NOT NULL,
            first_run_id VARCHAR NOT NULL
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS bronze.files (
            batch_id VARCHAR NOT NULL,
            entity VARCHAR NOT NULL,
            source VARCHAR NOT NULL,
            checksum VARCHAR NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            raw_content BLOB NOT NULL,
            PRIMARY KEY (batch_id, entity)
        )
    """)
    for name, columns in RAW_COLUMNS.items():
        create_table(
            connection, f"bronze.{name}",
            {**{column: "VARCHAR NOT NULL" for column in columns}, **METADATA_COLUMNS},
            ("batch_id", "source_row"),
        )
    for schema in ("silver", "staging"):
        for name, columns in SILVER_COLUMNS.items():
            create_table(connection, f"{schema}.{name}", columns, SILVER_KEYS[name])
    for schema in ("gold", "staging"):
        for name, columns in GOLD_COLUMNS.items():
            create_table(connection, f"{schema}.{name}", columns, GOLD_KEYS[name])
    connection.execute("""
        CREATE TABLE IF NOT EXISTS silver.quarantine (
            batch_id VARCHAR NOT NULL,
            entity VARCHAR NOT NULL,
            source_row BIGINT NOT NULL,
            source VARCHAR NOT NULL,
            rule_id VARCHAR NOT NULL,
            reason VARCHAR NOT NULL,
            raw_record JSON NOT NULL,
            first_seen_run_id VARCHAR NOT NULL,
            quarantined_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (batch_id, entity, source_row, rule_id)
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS observability.runs (
            run_id VARCHAR PRIMARY KEY,
            batch_id VARCHAR,
            input_path VARCHAR NOT NULL,
            retry_of VARCHAR,
            status VARCHAR NOT NULL CHECK (status IN ('running', 'published', 'blocked', 'failed')),
            started_at TIMESTAMPTZ NOT NULL,
            finished_at TIMESTAMPTZ,
            row_counts JSON NOT NULL,
            check_results JSON NOT NULL,
            publication_id VARCHAR,
            error VARCHAR
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS observability.check_results (
            run_id VARCHAR NOT NULL,
            rule_id VARCHAR NOT NULL,
            severity VARCHAR NOT NULL CHECK (severity IN ('blocking', 'warning')),
            passed BOOLEAN NOT NULL,
            details JSON NOT NULL,
            PRIMARY KEY (run_id, rule_id)
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS observability.publications (
            publication_id VARCHAR PRIMARY KEY,
            run_id VARCHAR NOT NULL,
            batch_id VARCHAR NOT NULL,
            published_at TIMESTAMPTZ NOT NULL
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS observability.publication (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            publication_id VARCHAR NOT NULL,
            run_id VARCHAR NOT NULL,
            batch_id VARCHAR NOT NULL,
            published_at TIMESTAMPTZ NOT NULL
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS observability.table_contracts (
            table_name VARCHAR PRIMARY KEY,
            grain VARCHAR NOT NULL,
            definition VARCHAR NOT NULL
        )
    """)
    connection.executemany("""
        INSERT INTO observability.table_contracts VALUES (?, ?, ?)
        ON CONFLICT DO NOTHING
    """, [
        ("gold.fact_orders", "one row per order_id", "BRL integer cents; delivered merchandise only is revenue"),
        ("gold.dim_date", "one row per purchase date", "America/Sao_Paulo calendar; YYYYMMDD date_id"),
    ])
