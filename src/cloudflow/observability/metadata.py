"""Keep every run attempt while retaining immutable input and publication references."""

from datetime import datetime, timezone
import json
from uuid import uuid4

from cloudflow.storage import insert_frame


def start_run(connection, run_id, input_path, retry_of):
    connection.execute("""
        INSERT INTO observability.runs
            (run_id, input_path, retry_of, status, started_at, row_counts, check_results)
        VALUES (?, ?, ?, 'running', ?, '{}', '[]')
    """, [run_id, str(input_path), retry_of, datetime.now(timezone.utc)])


def validate_retry(connection, retry_of):
    if retry_of is None:
        return
    previous = connection.execute(
        "SELECT status FROM observability.runs WHERE run_id = ?", [retry_of]
    ).fetchone()
    if previous is None or previous[0] not in {"blocked", "failed"}:
        raise ValueError("retry_of must identify a blocked or failed run in this database")


def finish_run(connection, result):
    checks = [check.as_dict() for check in result.checks]
    for check in result.checks:
        connection.execute("""
            INSERT INTO observability.check_results VALUES (?, ?, ?, ?, ?)
        """, [result.run_id, check.rule_id, check.severity, check.passed, json.dumps(check.details)])
    connection.execute("""
        UPDATE observability.runs
        SET batch_id = ?, status = ?, finished_at = ?, row_counts = ?, check_results = ?,
            publication_id = ?, error = ?
        WHERE run_id = ?
    """, [
        result.batch_id, result.status, datetime.now(timezone.utc), json.dumps(result.row_counts),
        json.dumps(checks), result.publication_id, result.error, result.run_id,
    ])


def save_quarantine(connection, quarantine, run_id):
    frame = quarantine.copy()
    frame["first_seen_run_id"] = run_id
    frame["quarantined_at"] = datetime.now(timezone.utc)
    insert_frame(connection, "silver.quarantine", frame, ignore_conflicts=True)


def current_publication(connection):
    row = connection.execute(
        "SELECT publication_id FROM observability.publication WHERE singleton = 1"
    ).fetchone()
    return row[0] if row else None


def record_publication(connection, run_id, batch_id):
    publication_id = uuid4().hex
    published_at = datetime.now(timezone.utc)
    connection.execute(
        "INSERT INTO observability.publications VALUES (?, ?, ?, ?)",
        [publication_id, run_id, batch_id, published_at],
    )
    connection.execute("""
        INSERT INTO observability.publication VALUES (1, ?, ?, ?, ?)
        ON CONFLICT (singleton) DO UPDATE SET
            publication_id = excluded.publication_id,
            run_id = excluded.run_id,
            batch_id = excluded.batch_id,
            published_at = excluded.published_at
    """, [publication_id, run_id, batch_id, published_at])
    return publication_id
