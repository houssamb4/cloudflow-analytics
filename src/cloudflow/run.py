"""Run the local Bronze -> Silver -> Gold pipeline with atomic publication."""

import argparse
from dataclasses import dataclass, field
import json
import math
from pathlib import Path
from uuid import uuid4

import duckdb

from cloudflow.contracts import GOLD_COLUMNS, RAW_COLUMNS, SILVER_COLUMNS
from cloudflow.ingestion import ingest_batch, read_batch
from cloudflow.observability.metadata import (
    current_publication, finish_run, record_publication, save_quarantine, start_run, validate_retry,
)
from cloudflow.quality import CheckResult, blocks_publication, evaluate_checks
from cloudflow.storage import initialize_database, insert_frame, transaction
from cloudflow.transforms import build_gold, build_silver


@dataclass
class RunResult:
    run_id: str
    status: str = "running"
    batch_id: str | None = None
    row_counts: dict = field(default_factory=dict)
    checks: list[CheckResult] = field(default_factory=list)
    publication_id: str | None = None
    error: str | None = None

    def as_dict(self):
        return {
            "run_id": self.run_id, "status": self.status, "batch_id": self.batch_id,
            "row_counts": self.row_counts, "checks": [check.as_dict() for check in self.checks],
            "publication_id": self.publication_id, "error": self.error,
        }


TABLES = {"silver": SILVER_COLUMNS, "gold": GOLD_COLUMNS}


def _counts(connection, *, staged=False):
    return {
        f"{schema}.{name}": connection.execute(
            f"SELECT COUNT(*) FROM {'staging' if staged else schema}.{name}"
        ).fetchone()[0]
        for schema, tables in TABLES.items() for name in tables
    }


def _stage(connection, silver, gold):
    with transaction(connection):
        for frames in (silver, gold):
            for name, frame in frames.items():
                connection.execute(f"DELETE FROM staging.{name}")
                insert_frame(connection, f"staging.{name}", frame)


def _staging_matches_publication(connection):
    for schema, tables in TABLES.items():
        for name in tables:
            changed = connection.execute(f"""
                SELECT EXISTS (
                    (SELECT * FROM staging.{name} EXCEPT ALL SELECT * FROM {schema}.{name})
                    UNION ALL
                    (SELECT * FROM {schema}.{name} EXCEPT ALL SELECT * FROM staging.{name})
                )
            """).fetchone()[0]
            if changed:
                return False
    return True


def _publish(connection, result):
    existing = current_publication(connection)
    if existing is not None and _staging_matches_publication(connection):
        return existing
    for schema, tables in TABLES.items():
        for name in tables:
            connection.execute(f"DELETE FROM {schema}.{name}")
            connection.execute(f"INSERT INTO {schema}.{name} SELECT * FROM staging.{name}")
    return record_publication(connection, result.run_id, result.batch_id)


def run_pipeline(
    input_dir: Path,
    database: Path,
    *,
    retry_of: str | None = None,
    drift_threshold: float = 0.5,
) -> RunResult:
    if not math.isfinite(drift_threshold) or drift_threshold < 0:
        raise ValueError("drift_threshold must be a finite, nonnegative fraction")
    input_dir = Path(input_dir).resolve()
    database = Path(database)
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database))
    result = RunResult(run_id=uuid4().hex)
    try:
        initialize_database(connection)
        validate_retry(connection, retry_of)
        start_run(connection, result.run_id, input_dir, retry_of)
        try:
            batch = read_batch(input_dir)
            result.batch_id = batch.batch_id
            result.row_counts["input"] = {name: len(frame) for name, frame in batch.frames.items()}
            bronze = ingest_batch(connection, batch, result.run_id)
            result.row_counts["bronze"] = {
                name: connection.execute(f"SELECT COUNT(*) FROM bronze.{name}").fetchone()[0]
                for name in RAW_COLUMNS
            }
            previous_publication = current_publication(connection)
            previous_count = connection.execute("SELECT COUNT(*) FROM gold.fact_orders").fetchone()[0]
            existing = {
                name: connection.execute(f"SELECT * FROM silver.{name}").fetchdf() for name in SILVER_COLUMNS
            }
            candidate = build_silver(bronze, existing)
            with transaction(connection):
                save_quarantine(connection, candidate.quarantine, result.run_id)
            result.row_counts["quarantine"] = {
                "batch_rows": len(candidate.quarantine.drop_duplicates(["entity", "source_row"])),
                "batch_violations": len(candidate.quarantine),
                "total_violations": connection.execute("SELECT COUNT(*) FROM silver.quarantine").fetchone()[0],
            }
            _stage(connection, candidate.frames, build_gold(candidate.frames))
            staged_silver = {
                name: connection.execute(f"SELECT * FROM staging.{name}").fetchdf() for name in SILVER_COLUMNS
            }
            staged_gold = {
                name: connection.execute(f"SELECT * FROM staging.{name}").fetchdf() for name in GOLD_COLUMNS
            }
            result.row_counts["candidate"] = _counts(connection, staged=True)
            result.checks = evaluate_checks(
                bronze, staged_silver, staged_gold, candidate.quarantine,
                previous_count if previous_publication is not None else None, drift_threshold,
            )
            with transaction(connection):
                if blocks_publication(result.checks):
                    result.status = "blocked"
                    result.publication_id = previous_publication
                else:
                    result.publication_id = _publish(connection, result)
                    result.status = "published"
                result.row_counts["published"] = _counts(connection)
                finish_run(connection, result)
        except Exception as error:
            result.status = "failed"
            result.error = f"{type(error).__name__}: {error}"
            result.publication_id = current_publication(connection)
            result.row_counts["published"] = _counts(connection)
            with transaction(connection):
                finish_run(connection, result)
        return result
    finally:
        connection.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("tests/fixtures/clean"))
    parser.add_argument("--database", type=Path, default=Path("local/cloudflow.duckdb"))
    parser.add_argument("--retry-of", help="Link corrected input to a blocked or failed run ID")
    parser.add_argument("--drift-threshold", type=float, default=0.5)
    arguments = parser.parse_args(argv)
    try:
        result = run_pipeline(
            arguments.input_dir, arguments.database,
            retry_of=arguments.retry_of, drift_threshold=arguments.drift_threshold,
        )
    except (ValueError, duckdb.Error) as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    return {"published": 0, "blocked": 2, "failed": 1}[result.status]


if __name__ == "__main__":
    raise SystemExit(main())
