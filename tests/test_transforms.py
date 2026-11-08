from datetime import datetime, timezone

import duckdb
import pandas as pd
import pytest

from cloudflow.contracts import SILVER_COLUMNS
from cloudflow.ingestion import ingest_batch, read_batch
from cloudflow.storage import initialize_database
from cloudflow.transforms import build_silver, dedupe_orders


def test_dedupe_latest_update_wins_independent_of_input_order():
    orders = pd.DataFrame([
        {"order_id": "a", "order_updated_at": pd.Timestamp("2018-01-06T12:00:00Z"),
         "ingested_at": pd.Timestamp("2025-01-01T00:00:00Z"), "batch_id": "a", "source_row": 1},
        {"order_id": "a", "order_updated_at": pd.Timestamp("2018-01-04T12:00:00Z"),
         "ingested_at": pd.Timestamp("2025-01-02T00:00:00Z"), "batch_id": "b", "source_row": 2},
        {"order_id": "b", "order_updated_at": pd.Timestamp("2018-01-05T12:00:00Z"),
         "ingested_at": pd.Timestamp("2025-01-01T00:00:00Z"), "batch_id": "a", "source_row": 3},
    ])
    result = dedupe_orders(orders)
    assert len(result) == 2
    assert result.set_index("order_id").loc["a", "batch_id"] == "a"
    pd.testing.assert_frame_equal(result, dedupe_orders(orders.iloc[::-1]))


def test_bronze_to_silver_dedupes_and_quarantines_invalid_price(fixture_dir):
    with duckdb.connect(":memory:") as connection:
        initialize_database(connection)
        batch = read_batch(fixture_dir / "invalid")
        bronze = ingest_batch(connection, batch, "unit-run")
        existing = {
            name: connection.execute(f"SELECT * FROM silver.{name}").fetchdf()
            for name in SILVER_COLUMNS
        }
        result = build_silver(bronze, existing)
    orders = result.frames["orders"].set_index("order_id")
    assert len(orders) == 2
    assert orders.loc["order_002", "order_status"] == "delivered"
    assert orders.loc["order_002", "order_updated_at"] == datetime(2018, 1, 6, 13, tzinfo=timezone.utc)
    assert "order_004" not in orders.index
    assert set(result.quarantine["rule_id"]) == {"ITEM_REQUIRED_PRICE", "ORDER_INVALID_ITEMS"}
    rejected_item = result.quarantine[result.quarantine["entity"] == "order_items"].iloc[0]
    assert rejected_item["batch_id"] == batch.batch_id
    assert '"price": ""' in rejected_item["raw_record"]


@pytest.mark.parametrize("missing_column", ["order_id", "customer_id", "order_status", "order_purchase_timestamp"])
def test_missing_required_order_fields_are_quarantined(missing_column, fixture_dir):
    with duckdb.connect(":memory:") as connection:
        initialize_database(connection)
        bronze = ingest_batch(connection, read_batch(fixture_dir / "clean"), "unit-run")
        bronze["orders"].loc[0, missing_column] = ""
        existing = {
            name: connection.execute(f"SELECT * FROM silver.{name}").fetchdf()
            for name in SILVER_COLUMNS
        }
        result = build_silver(bronze, existing)
    assert "ORDER_REQUIRED_FIELDS" in set(result.quarantine["rule_id"])
    assert len(result.frames["orders"]) == 2
