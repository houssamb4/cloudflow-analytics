import duckdb
import pytest

from cloudflow.contracts import SILVER_COLUMNS
from cloudflow.ingestion import ingest_batch, read_batch
from cloudflow.quality import blocks_publication, evaluate_checks
from cloudflow.storage import initialize_database
from cloudflow.transforms import build_gold, build_silver


@pytest.mark.parametrize("corruption", ["swapped_totals", "wrong_status", "wrong_item_count"])
def test_reconciliation_detects_per_order_errors_even_when_global_totals_match(corruption, fixture_dir):
    with duckdb.connect(":memory:") as connection:
        initialize_database(connection)
        bronze = ingest_batch(connection, read_batch(fixture_dir / "clean"), "unit-run")
        existing = {
            name: connection.execute(f"SELECT * FROM silver.{name}").fetchdf()
            for name in SILVER_COLUMNS
        }
        silver = build_silver(bronze, existing)
        gold = build_gold(silver.frames)
        facts = gold["fact_orders"]
        delivered_index = facts.index[facts["order_id"] == "order_001"][0]
        shipped_index = facts.index[facts["order_id"] == "order_002"][0]
        if corruption == "swapped_totals":
            columns = ["merchandise_total_cents", "freight_total_cents", "order_total_cents"]
            facts.loc[[delivered_index, shipped_index], columns] = facts.loc[[shipped_index, delivered_index], columns].to_numpy()
        elif corruption == "wrong_status":
            facts.loc[delivered_index, "order_status"] = "canceled"
        else:
            facts.loc[delivered_index, "item_count"] = 99
        checks = evaluate_checks(bronze, silver.frames, gold, silver.quarantine, None)
    reconciliation = next(check for check in checks if check.rule_id == "GOLD_RECONCILIATION")
    assert not reconciliation.passed
    assert not reconciliation.details["orders_reconciled"]
    assert blocks_publication(checks)


def test_drift_is_warning_only_and_exact_threshold_passes(fixture_dir):
    with duckdb.connect(":memory:") as connection:
        initialize_database(connection)
        bronze = ingest_batch(connection, read_batch(fixture_dir / "clean"), "unit-run")
        existing = {
            name: connection.execute(f"SELECT * FROM silver.{name}").fetchdf()
            for name in SILVER_COLUMNS
        }
        silver = build_silver(bronze, existing)
        gold = build_gold(silver.frames)
        checks = evaluate_checks(bronze, silver.frames, gold, silver.quarantine, 2, drift_threshold=0.5)
        warning_checks = evaluate_checks(bronze, silver.frames, gold, silver.quarantine, 1, drift_threshold=0.5)
    assert next(check for check in checks if check.rule_id == "ROW_COUNT_DRIFT").passed
    warning = next(check for check in warning_checks if check.rule_id == "ROW_COUNT_DRIFT")
    assert warning.severity == "warning"
    assert not warning.passed
    assert not blocks_publication(warning_checks)
