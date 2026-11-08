"""Read the synthetic Olist subset without losing raw field values."""

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path

import pandas as pd

from cloudflow.contracts import RAW_COLUMNS, SOURCE
from cloudflow.storage import insert_frame, transaction


@dataclass(frozen=True)
class Batch:
    batch_id: str
    source: str
    input_path: Path
    checksums: dict[str, str]
    frames: dict[str, pd.DataFrame]
    contents: dict[str, bytes]
    ingested_at: datetime


def read_batch(input_dir: Path, source: str = SOURCE) -> Batch:
    input_dir = Path(input_dir).resolve()
    frames = {}
    checksums = {}
    contents = {}
    for entity, columns in RAW_COLUMNS.items():
        content = (input_dir / f"{entity}.csv").read_bytes()
        reader = csv.reader(io.StringIO(content.decode("utf-8-sig"), newline=""), strict=True)
        header = next(reader, None)
        if header != list(columns):
            raise ValueError(f"{entity}.csv must have columns {list(columns)} in that order")
        rows = []
        for source_row, values in enumerate(reader, start=1):
            if len(values) != len(columns):
                raise ValueError(f"{entity}.csv row {source_row} has {len(values)} fields, expected {len(columns)}")
            rows.append(dict(zip(columns, values, strict=True)))
        frames[entity] = pd.DataFrame(rows, columns=list(columns), dtype=str)
        checksums[entity] = hashlib.sha256(content).hexdigest()
        contents[entity] = content
    manifest = json.dumps({"source": source, "checksums": checksums}, sort_keys=True).encode()
    return Batch(
        batch_id=hashlib.sha256(manifest).hexdigest(),
        source=source,
        input_path=input_dir,
        checksums=checksums,
        frames=frames,
        contents=contents,
        ingested_at=datetime.now(timezone.utc),
    )


def ingest_batch(connection, batch: Batch, run_id: str) -> dict[str, pd.DataFrame]:
    with transaction(connection):
        connection.execute("""
            INSERT INTO bronze.batches VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
        """, [
            batch.batch_id, batch.source, json.dumps(batch.checksums), batch.ingested_at,
            str(batch.input_path), run_id,
        ])
        ingested_at = connection.execute(
            "SELECT ingested_at FROM bronze.batches WHERE batch_id = ?", [batch.batch_id]
        ).fetchone()[0]
        for entity, raw in batch.frames.items():
            connection.execute("""
                INSERT INTO bronze.files VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
            """, [
                batch.batch_id, entity, batch.source, batch.checksums[entity], ingested_at,
                batch.contents[entity],
            ])
            frame = raw.copy()
            frame["source"] = batch.source
            frame["batch_id"] = batch.batch_id
            frame["checksum"] = batch.checksums[entity]
            frame["ingested_at"] = ingested_at
            frame["source_row"] = range(1, len(frame) + 1)
            insert_frame(connection, f"bronze.{entity}", frame, ignore_conflicts=True)
    return {
        entity: connection.execute(
            f"SELECT * FROM bronze.{entity} WHERE batch_id = ? ORDER BY source_row", [batch.batch_id]
        ).fetchdf()
        for entity in RAW_COLUMNS
    }
