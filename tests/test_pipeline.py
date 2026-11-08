import csv
from datetime import datetime, timezone
import hashlib
import json
import subprocess
import sys

import pytest

from cloudflow.run import main, run_pipeline


def read_csv(path):
    with path.open(newline="") as file:
        return list(csv.DictReader(file))


def write_csv(path, rows):
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def published_snapshot(query):
    return {
        name: query(f"SELECT * FROM {name} ORDER BY ALL")
        for name in (
            "silver.orders", "silver.order_items", "silver.customers", "silver.products",
            "gold.fact_orders", "gold.dim_date", "observability.publication",
        )
    }


def test_clean_pipeline_preserves_raw_values_and_builds_declared_grains(fixture_dir, database, query):
    result = run_pipeline(fixture_dir / "clean", database)
    assert result.status == "published", result.error
    assert all(check.passed for check in result.checks)
    assert result.row_counts["published"]["gold.fact_orders"] == 3
    assert query("SELECT price FROM bronze.order_items WHERE order_id = 'order_001' AND order_item_id = '2'") == [("25,50",)]
    assert query("SELECT customer_zip_code_prefix FROM bronze.customers WHERE customer_id = 'customer_001'") == [("01000",)]
    assert query("SELECT customer_state FROM silver.customers WHERE customer_id = 'customer_001'") == [("SP",)]
    assert query("SELECT order_status FROM bronze.orders WHERE order_id = 'order_003'") == [("cancelled",)]
    assert query("SELECT order_status FROM silver.orders WHERE order_id = 'order_003'") == [("canceled",)]
    assert query("SELECT price_cents, currency FROM silver.order_items WHERE order_id = 'order_001' AND order_item_id = 2") == [(2550, "BRL")]
    assert query("SELECT order_purchase_timestamp FROM silver.orders WHERE order_id = 'order_001'") == [
        (datetime(2018, 1, 1, 12, tzinfo=timezone.utc),)
    ]
    assert query("SELECT purchase_date_id FROM gold.fact_orders WHERE order_id = 'order_002'") == [(20180102,)]
    assert query("SELECT item_count, merchandise_total_cents, order_total_cents, recognized_revenue_cents FROM gold.fact_orders WHERE order_id = 'order_001'") == [(2, 12550, 13800, 12550)]
    assert query("SELECT order_total_cents, recognized_revenue_cents FROM gold.fact_orders WHERE order_id = 'order_003'") == [(2200, 0)]
    assert query("SELECT COUNT(*), COUNT(DISTINCT order_id) FROM gold.fact_orders") == [(3, 3)]
    assert query("SELECT COUNT(*), COUNT(DISTINCT date_id) FROM gold.dim_date") == [(3, 3)]
    assert query("SELECT grain FROM observability.table_contracts WHERE table_name = 'gold.fact_orders'") == [("one row per order_id",)]
    orders_bytes = (fixture_dir / "clean/orders.csv").read_bytes()
    assert query("SELECT raw_content FROM bronze.files WHERE entity = 'orders'") == [(orders_bytes,)]
    assert query("SELECT DISTINCT source, batch_id, checksum, ingested_at IS NOT NULL FROM bronze.orders") == [
        ("synthetic://olist", result.batch_id, hashlib.sha256(orders_bytes).hexdigest(), True)
    ]
    assert query("SELECT status, publication_id FROM observability.runs") == [("published", result.publication_id)]
    assert query("SELECT COUNT(*) FROM observability.check_results WHERE run_id = ?", [result.run_id]) == [(6,)]


def test_identical_rerun_is_idempotent_in_every_layer(fixture_dir, database, query):
    first = run_pipeline(fixture_dir / "clean", database)
    assert first.status == "published", first.error
    snapshot = published_snapshot(query)
    second = run_pipeline(fixture_dir / "clean", database)
    assert second.status == "published", second.error
    assert second.run_id != first.run_id
    assert second.batch_id == first.batch_id
    assert second.publication_id == first.publication_id
    assert published_snapshot(query) == snapshot
    assert query("SELECT COUNT(*) FROM bronze.batches") == [(1,)]
    assert query("SELECT COUNT(*) FROM bronze.files") == [(4,)]
    assert query("SELECT COUNT(*) FROM bronze.orders") == [(3,)]
    assert query("SELECT COUNT(*) FROM bronze.order_items") == [(4,)]
    assert query("SELECT COUNT(*) FROM observability.publications") == [(1,)]
    assert query("SELECT COUNT(*) FROM observability.runs") == [(2,)]
    counts, checks = query("SELECT row_counts, check_results FROM observability.runs WHERE run_id = ?", [second.run_id])[0]
    assert json.loads(counts)["published"]["gold.fact_orders"] == 3
    assert len(json.loads(checks)) == 6


def test_blocking_checks_retain_previous_snapshot_and_quarantine_is_idempotent(fixture_dir, database, query):
    clean = run_pipeline(fixture_dir / "clean", database)
    assert clean.status == "published", clean.error
    snapshot = published_snapshot(query)
    failed = run_pipeline(fixture_dir / "invalid", database)
    assert failed.status == "blocked", failed.error
    assert {check.rule_id for check in failed.checks if not check.passed and check.severity == "blocking"} == {
        "DUPLICATE_ORDER_IDS", "INVALID_ROWS",
    }
    assert failed.publication_id == clean.publication_id
    assert published_snapshot(query) == snapshot
    assert query("SELECT order_status FROM staging.orders WHERE order_id = 'order_002'") == [("delivered",)]
    assert query("SELECT rule_id FROM silver.quarantine ORDER BY rule_id") == [("ITEM_REQUIRED_PRICE",), ("ORDER_INVALID_ITEMS",)]
    assert query("SELECT price FROM bronze.order_items WHERE batch_id = ? AND order_id = 'order_004'", [failed.batch_id]) == [("",)]
    repeat = run_pipeline(fixture_dir / "invalid", database, retry_of=failed.run_id)
    assert repeat.status == "blocked", repeat.error
    assert repeat.batch_id == failed.batch_id
    assert published_snapshot(query) == snapshot
    assert query("SELECT COUNT(*) FROM silver.quarantine") == [(2,)]
    assert query("SELECT COUNT(*) FROM bronze.orders") == [(7,)]
    assert query("SELECT DISTINCT first_seen_run_id FROM silver.quarantine") == [(failed.run_id,)]


def test_first_blocked_run_has_no_validated_publication(fixture_dir, database, query):
    result = run_pipeline(fixture_dir / "invalid", database)
    assert result.status == "blocked", result.error
    assert result.publication_id is None
    assert query("SELECT COUNT(*) FROM gold.fact_orders") == [(0,)]
    assert query("SELECT COUNT(*) FROM silver.orders") == [(0,)]
    assert query("SELECT COUNT(*) FROM staging.fact_orders") == [(2,)]
    assert query("SELECT COUNT(*) FROM observability.publication") == [(0,)]
    assert query("SELECT status FROM observability.runs") == [("blocked",)]


def test_corrected_retry_publishes_late_update_and_warns_without_blocking(fixture_dir, database, query):
    initial = run_pipeline(fixture_dir / "clean", database)
    assert initial.status == "published", initial.error
    failed = run_pipeline(fixture_dir / "invalid", database)
    assert failed.status == "blocked", failed.error
    failed_bronze = query("SELECT * FROM bronze.orders WHERE batch_id = ? ORDER BY source_row", [failed.batch_id])
    corrected = run_pipeline(fixture_dir / "corrected", database, retry_of=failed.run_id)
    assert corrected.status == "published", corrected.error
    assert corrected.batch_id != failed.batch_id
    assert corrected.publication_id != initial.publication_id
    warning = next(check for check in corrected.checks if check.rule_id == "ROW_COUNT_DRIFT")
    assert warning.severity == "warning"
    assert not warning.passed
    assert warning.details["relative_drift"] == pytest.approx(2 / 3)
    assert all(check.passed for check in corrected.checks if check.severity == "blocking")
    assert query("SELECT order_status, recognized_revenue_cents FROM gold.fact_orders WHERE order_id = 'order_002'") == [("delivered", 5000)]
    assert query("SELECT COUNT(*), COUNT(DISTINCT order_id) FROM gold.fact_orders") == [(5, 5)]
    assert query("SELECT SUM(recognized_revenue_cents) FROM gold.fact_orders") == [(24550,)]
    assert query("SELECT retry_of FROM observability.runs WHERE run_id = ?", [corrected.run_id]) == [(failed.run_id,)]
    assert query("SELECT * FROM bronze.orders WHERE batch_id = ? ORDER BY source_row", [failed.batch_id]) == failed_bronze
    assert query("SELECT COUNT(*) FROM silver.quarantine") == [(2,)]
    snapshot = published_snapshot(query)
    repeated = run_pipeline(fixture_dir / "corrected", database)
    assert repeated.status == "published", repeated.error
    assert repeated.publication_id == corrected.publication_id
    assert published_snapshot(query) == snapshot
    assert query("SELECT COUNT(*) FROM bronze.batches") == [(3,)]
    assert query("SELECT COUNT(*) FROM observability.publications") == [(2,)]


def test_stale_arrival_cannot_regress_a_newer_order(fixture_dir, database, query, copy_batch):
    assert run_pipeline(fixture_dir / "clean", database).status == "published"
    assert run_pipeline(fixture_dir / "corrected", database).status == "published"
    current = query("SELECT * FROM gold.fact_orders WHERE order_id = 'order_002'")
    stale = copy_batch()
    rows = read_csv(stale / "orders.csv")
    rows[1]["order_updated_at"] = "2018-01-05 09:00:00"
    write_csv(stale / "orders.csv", rows)
    items = read_csv(stale / "order_items.csv")
    items[2]["price"] = "999.00"
    write_csv(stale / "order_items.csv", items)
    result = run_pipeline(stale, database)
    assert result.status == "published", result.error
    assert query("SELECT * FROM gold.fact_orders WHERE order_id = 'order_002'") == current
    assert query("SELECT price_cents FROM silver.order_items WHERE order_id = 'order_002'") == [(5000,)]


def test_new_order_version_replaces_its_complete_item_set(fixture_dir, database, query, copy_batch):
    assert run_pipeline(fixture_dir / "clean", database).status == "published"
    updated = copy_batch()
    rows = read_csv(updated / "orders.csv")
    rows[0]["order_updated_at"] = "2018-01-07 12:00:00"
    write_csv(updated / "orders.csv", rows)
    items = read_csv(updated / "order_items.csv")
    write_csv(updated / "order_items.csv", [row for row in items if not (row["order_id"] == "order_001" and row["order_item_id"] == "2")])
    result = run_pipeline(updated, database)
    assert result.status == "published", result.error
    assert query("SELECT item_count, merchandise_total_cents FROM gold.fact_orders WHERE order_id = 'order_001'") == [(1, 10000)]
    assert query("SELECT COUNT(*) FROM silver.order_items WHERE order_id = 'order_001'") == [(1,)]


def test_duplicate_order_check_blocks_even_when_dedupe_and_other_checks_pass(fixture_dir, database, query, copy_batch):
    duplicate = copy_batch()
    rows = read_csv(duplicate / "orders.csv")
    rows.append(dict(rows[0]))
    write_csv(duplicate / "orders.csv", rows)
    result = run_pipeline(duplicate, database)
    assert result.status == "blocked", result.error
    assert [check.rule_id for check in result.checks if not check.passed] == ["DUPLICATE_ORDER_IDS"]
    assert query("SELECT COUNT(*) FROM staging.orders") == [(3,)]
    assert query("SELECT COUNT(*) FROM gold.fact_orders") == [(0,)]


@pytest.mark.parametrize("column,value,rule_id", [
    ("price", "", "ITEM_REQUIRED_PRICE"),
    ("price", "-1.00", "ITEM_MONEY"),
    ("price", "1.005", "ITEM_MONEY"),
    ("price", "NaN", "ITEM_MONEY"),
    ("freight_value", "", "ITEM_MONEY"),
    ("product_id", "unknown", "ITEM_PRODUCT_REFERENCE"),
])
def test_invalid_items_never_publish_partial_order_totals(column, value, rule_id, copy_batch, database, query):
    batch = copy_batch()
    rows = read_csv(batch / "order_items.csv")
    rows[0][column] = value
    write_csv(batch / "order_items.csv", rows)
    result = run_pipeline(batch, database)
    assert result.status == "blocked", result.error
    assert query("SELECT COUNT(*) FROM staging.orders WHERE order_id = 'order_001'") == [(0,)]
    assert query("SELECT COUNT(*) FROM staging.order_items WHERE order_id = 'order_001'") == [(0,)]
    assert query("SELECT COUNT(*) FROM silver.quarantine WHERE rule_id = ?", [rule_id]) == [(1,)]


def test_schema_failure_is_recorded_and_can_be_retried_after_correction(copy_batch, fixture_dir, database, query):
    malformed = copy_batch()
    original = (malformed / "orders.csv").read_text()
    (malformed / "orders.csv").write_text(original.replace("order_updated_at", "unknown_field", 1))
    failed = run_pipeline(malformed, database)
    assert failed.status == "failed"
    assert "must have columns" in failed.error
    assert query("SELECT status, error IS NOT NULL FROM observability.runs") == [("failed", True)]
    assert query("SELECT COUNT(*) FROM bronze.batches") == [(0,)]
    fixed = run_pipeline(fixture_dir / "clean", database, retry_of=failed.run_id)
    assert fixed.status == "published", fixed.error
    assert query("SELECT retry_of FROM observability.runs WHERE run_id = ?", [fixed.run_id]) == [(failed.run_id,)]


def test_retry_requires_existing_failed_run(fixture_dir, database):
    with pytest.raises(ValueError, match="blocked or failed"):
        run_pipeline(fixture_dir / "clean", database, retry_of="does-not-exist")


def test_cli_exit_codes_and_json_output(fixture_dir, database, capsys):
    assert main(["--input-dir", str(fixture_dir / "clean"), "--database", str(database)]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "published"
    assert main(["--input-dir", str(fixture_dir / "invalid"), "--database", str(database)]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "blocked"
    assert main(["--input-dir", str(fixture_dir / "missing"), "--database", str(database)]) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "failed"


@pytest.mark.parametrize("timestamp", [
    "not-a-timestamp", "2018-01-01", "9999-01-01 12:00:00",
    "2018-11-04 00:30:00", "2018-02-17 23:30:00",
])
def test_invalid_or_ambiguous_timestamps_are_quarantined(timestamp, copy_batch, database, query):
    batch = copy_batch()
    rows = read_csv(batch / "orders.csv")
    rows[0]["order_purchase_timestamp"] = timestamp
    write_csv(batch / "orders.csv", rows)
    result = run_pipeline(batch, database)
    assert result.status == "blocked", result.error
    assert query("SELECT COUNT(*) FROM silver.quarantine WHERE rule_id = 'ORDER_TIMESTAMP'") == [(1,)]


def test_explicit_timestamp_offset_is_normalized(copy_batch, database, query):
    batch = copy_batch()
    rows = read_csv(batch / "orders.csv")
    rows[0]["order_purchase_timestamp"] = "2018-01-01T12:00:00Z"
    write_csv(batch / "orders.csv", rows)
    result = run_pipeline(batch, database)
    assert result.status == "published", result.error
    assert query("SELECT order_purchase_timestamp FROM silver.orders WHERE order_id = 'order_001'") == [
        (datetime(2018, 1, 1, 12, tzinfo=timezone.utc),)
    ]


def test_canceled_order_can_have_no_items(copy_batch, database, query):
    batch = copy_batch()
    rows = read_csv(batch / "order_items.csv")
    write_csv(batch / "order_items.csv", [row for row in rows if row["order_id"] != "order_003"])
    result = run_pipeline(batch, database)
    assert result.status == "published", result.error
    assert query("SELECT item_count, order_total_cents, recognized_revenue_cents FROM gold.fact_orders WHERE order_id = 'order_003'") == [(0, 0, 0)]


def test_identical_input_at_different_path_has_same_batch_id(fixture_dir, copy_batch, database, query):
    first = run_pipeline(fixture_dir / "clean", database)
    second = run_pipeline(copy_batch(), database)
    assert second.status == "published", second.error
    assert second.batch_id == first.batch_id
    assert second.publication_id == first.publication_id
    assert query("SELECT COUNT(*) FROM bronze.batches") == [(1,)]


@pytest.mark.parametrize("item_id", ["1", "01"])
def test_duplicate_item_keys_block_publication(item_id, copy_batch, database, query):
    batch = copy_batch()
    rows = read_csv(batch / "order_items.csv")
    rows.append({**rows[0], "order_item_id": item_id})
    write_csv(batch / "order_items.csv", rows)
    result = run_pipeline(batch, database)
    assert result.status == "blocked", result.error
    assert [check.rule_id for check in result.checks if not check.passed] == ["DUPLICATE_ITEM_KEYS"]
    assert query("SELECT COUNT(*) FROM gold.fact_orders") == [(0,)]


def test_missing_item_set_is_quarantined(copy_batch, database, query):
    batch = copy_batch()
    rows = read_csv(batch / "order_items.csv")
    write_csv(batch / "order_items.csv", [row for row in rows if row["order_id"] != "order_001"])
    result = run_pipeline(batch, database)
    assert result.status == "blocked", result.error
    assert query("SELECT COUNT(*) FROM silver.quarantine WHERE rule_id = 'ORDER_REQUIRED_ITEMS'") == [(1,)]


@pytest.mark.parametrize("threshold", [-0.1, float("nan"), float("inf")])
def test_invalid_drift_threshold_is_rejected(threshold, fixture_dir, database):
    with pytest.raises(ValueError, match="finite, nonnegative"):
        run_pipeline(fixture_dir / "clean", database, drift_threshold=threshold)


def test_publication_failure_rolls_back_every_curated_table(monkeypatch, fixture_dir, database, query):
    from cloudflow import run

    clean = run_pipeline(fixture_dir / "clean", database)
    assert clean.status == "published", clean.error
    snapshot = published_snapshot(query)

    def fail_publication(*args):
        raise RuntimeError("simulated publication metadata failure")

    monkeypatch.setattr(run, "record_publication", fail_publication)
    result = run_pipeline(fixture_dir / "corrected", database)
    assert result.status == "failed"
    assert "simulated publication metadata failure" in result.error
    assert published_snapshot(query) == snapshot
    assert query("SELECT status FROM observability.runs WHERE run_id = ?", [result.run_id]) == [("failed",)]


def test_malformed_csv_row_is_rejected_before_ingestion(copy_batch, database, query):
    batch = copy_batch()
    original = (batch / "order_items.csv").read_text()
    (batch / "order_items.csv").write_text(original.replace("100.00,10.00", "100.00,10.00,extra", 1))
    result = run_pipeline(batch, database)
    assert result.status == "failed"
    assert "fields, expected" in result.error
    assert query("SELECT COUNT(*) FROM bronze.batches") == [(0,)]


def test_empty_input_can_publish_an_explicit_validated_zero_snapshot(copy_batch, database, query):
    batch = copy_batch()
    for path in batch.glob("*.csv"):
        path.write_text(path.read_text().splitlines()[0] + "\n")
    result = run_pipeline(batch, database)
    assert result.status == "published", result.error
    assert all(check.passed for check in result.checks)
    assert result.publication_id is not None
    assert query("SELECT COUNT(*) FROM observability.publication") == [(1,)]
    assert query("SELECT COUNT(*) FROM gold.fact_orders") == [(0,)]
    repeated = run_pipeline(batch, database)
    assert repeated.status == "published", repeated.error
    assert repeated.publication_id == result.publication_id


def test_module_entrypoint(fixture_dir, database):
    result = subprocess.run(
        [sys.executable, "-m", "cloudflow.run", "--input-dir", str(fixture_dir / "clean"), "--database", str(database)],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["status"] == "published"
