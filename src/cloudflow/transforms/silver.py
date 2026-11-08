"""Validate rows and merge complete order versions into a Silver candidate."""

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import json
import re
from zoneinfo import ZoneInfo

import pandas as pd

from cloudflow.contracts import (
    CURRENCY, METADATA_COLUMNS, ORDER_STATUSES, RAW_COLUMNS, SILVER_COLUMNS, SILVER_KEYS, SOURCE_TIMEZONE,
)

QUARANTINE_COLUMNS = ["batch_id", "entity", "source_row", "source", "rule_id", "reason", "raw_record"]


@dataclass
class SilverResult:
    frames: dict[str, pd.DataFrame]
    quarantine: pd.DataFrame


def _frame(entity, rows):
    frame = pd.DataFrame(rows, columns=list(SILVER_COLUMNS[entity]))
    if entity == "orders":
        for column in ("order_purchase_timestamp", "order_updated_at"):
            frame[column] = pd.to_datetime(frame[column], utc=True)
    return frame


def _combine(*frames):
    populated = [frame for frame in frames if not frame.empty]
    return pd.concat(populated, ignore_index=True) if populated else frames[0].copy()


def dedupe_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """Latest event time wins; persisted ingestion metadata breaks ties deterministically."""
    return orders.sort_values(
        ["order_updated_at", "ingested_at", "batch_id", "source_row"], kind="stable"
    ).drop_duplicates("order_id", keep="last").reset_index(drop=True)


def _merge_references(entity, existing, incoming):
    return _combine(existing, incoming).sort_values(
        ["ingested_at", "batch_id", "source_row"], kind="stable"
    ).drop_duplicates(list(SILVER_KEYS[entity]), keep="last").reset_index(drop=True)


def _timestamp(value):
    value = value.strip()
    if not re.match(r"[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}", value):
        raise ValueError("expected an ISO 8601 timestamp with seconds")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        zone = ZoneInfo(SOURCE_TIMEZONE)
        local = parsed.replace(tzinfo=zone)
        if local.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None) != parsed:
            raise ValueError("nonexistent local timestamp; provide an explicit UTC offset")
        if local.utcoffset() != parsed.replace(tzinfo=zone, fold=1).utcoffset():
            raise ValueError("ambiguous local timestamp; provide an explicit UTC offset")
        parsed = local
    normalized = parsed.astimezone(timezone.utc)
    pd.Timestamp(normalized).as_unit("ns")
    return normalized


def _cents(value):
    value = value.strip()
    if not re.fullmatch(r"[0-9]+(?:[.,][0-9]{1,2})?", value):
        raise ValueError("expected a nonnegative amount with at most two decimal places")
    cents = int(Decimal(value.replace(",", ".")) * 100)
    if cents > 99_999_999_999:
        raise ValueError("line amount exceeds the contract limit of BRL 999,999,999.99")
    return cents


def build_silver(bronze: dict[str, pd.DataFrame], existing: dict[str, pd.DataFrame]) -> SilverResult:
    rejected = []

    def reject(entity, row, rule_id, reason):
        rejected.append({
            "batch_id": row["batch_id"], "entity": entity, "source_row": int(row["source_row"]),
            "source": row["source"], "rule_id": rule_id, "reason": reason,
            "raw_record": json.dumps({column: row[column] for column in RAW_COLUMNS[entity]}, sort_keys=True),
        })

    def metadata(row):
        return {column: row[column] for column in METADATA_COLUMNS}

    incoming = {}
    for entity, required in (("customers", ("customer_id", "customer_unique_id")), ("products", ("product_id",))):
        rows = []
        for row in bronze[entity].to_dict("records"):
            normalized = {column: row[column].strip() for column in RAW_COLUMNS[entity]}
            if any(not normalized[column] for column in required):
                reject(entity, row, "REFERENCE_REQUIRED_FIELDS", f"required fields: {', '.join(required)}")
                continue
            if entity == "customers":
                normalized["customer_state"] = normalized["customer_state"].upper()
            rows.append({**normalized, **metadata(row)})
        incoming[entity] = _frame(entity, rows)
    references = {
        entity: _merge_references(entity, existing[entity], incoming[entity])
        for entity in ("customers", "products")
    }
    customer_ids = set(references["customers"]["customer_id"])
    product_ids = set(references["products"]["product_id"])

    orders = []
    order_raw = {}
    for row in bronze["orders"].to_dict("records"):
        order_id = row["order_id"].strip()
        customer_id = row["customer_id"].strip()
        status = row["order_status"].strip().lower().replace("cancelled", "canceled")
        if any(not row[column].strip() for column in RAW_COLUMNS["orders"]):
            reject("orders", row, "ORDER_REQUIRED_FIELDS", "all order fields are required")
            continue
        if status not in ORDER_STATUSES:
            reject("orders", row, "ORDER_STATUS", f"unsupported order status: {status}")
            continue
        try:
            purchased = _timestamp(row["order_purchase_timestamp"])
            updated = _timestamp(row["order_updated_at"])
            if updated < purchased:
                raise ValueError("update precedes purchase")
        except (ValueError, OverflowError) as error:
            reject("orders", row, "ORDER_TIMESTAMP", str(error))
            continue
        if customer_id not in customer_ids:
            reject("orders", row, "ORDER_CUSTOMER_REFERENCE", "customer_id does not identify a valid customer")
            continue
        orders.append({
            "order_id": order_id, "customer_id": customer_id, "order_status": status,
            "order_purchase_timestamp": purchased, "order_updated_at": updated,
            "currency": CURRENCY, **metadata(row),
        })
        order_raw[(order_id, row["source_row"])] = row
    shaped_orders = dedupe_orders(_frame("orders", orders))
    incoming_order_ids = set(shaped_orders["order_id"])

    items = []
    item_raw = {}
    invalid_item_orders = set()
    for row in bronze["order_items"].to_dict("records"):
        order_id = row["order_id"].strip()
        product_id = row["product_id"].strip()
        item_id = row["order_item_id"].strip()
        violations = []
        if not order_id or not product_id or not re.fullmatch(r"[0-9]+", item_id):
            violations.append(("ITEM_REQUIRED_FIELDS", "order_id, product_id and positive integer order_item_id are required"))
        elif not 1 <= int(item_id) <= 2**63 - 1:
            violations.append(("ITEM_REQUIRED_FIELDS", "order_item_id must be a positive BIGINT"))
        if not row["price"].strip():
            violations.append(("ITEM_REQUIRED_PRICE", "price is required"))
        amounts = {}
        for column in ("price", "freight_value"):
            if column == "price" and not row[column].strip():
                continue
            try:
                amounts[column] = _cents(row[column])
            except ValueError as error:
                violations.append(("ITEM_MONEY", f"{column}: {error}"))
        if order_id not in incoming_order_ids:
            violations.append(("ITEM_ORDER_REFERENCE", "each item requires a valid order version in the same batch"))
        if product_id not in product_ids:
            violations.append(("ITEM_PRODUCT_REFERENCE", "product_id does not identify a valid product"))
        if violations:
            invalid_item_orders.add(order_id)
            for rule_id, reason in violations:
                reject("order_items", row, rule_id, reason)
            continue
        items.append({
            "order_id": order_id, "order_item_id": int(item_id), "product_id": product_id,
            "price_cents": amounts["price"], "freight_value_cents": amounts["freight_value"],
            "currency": CURRENCY, **metadata(row),
        })
        item_raw[(order_id, row["source_row"])] = row
    shaped_items = _frame("order_items", items).drop_duplicates(["order_id", "order_item_id"], keep="last")
    orders_with_items = set(shaped_items["order_id"])
    valid_orders = []
    for row in shaped_orders.to_dict("records"):
        raw = order_raw[(row["order_id"], row["source_row"])]
        if row["order_id"] in invalid_item_orders:
            reject("orders", raw, "ORDER_INVALID_ITEMS", "one or more line items failed validation; never publish partial totals")
        elif row["order_id"] not in orders_with_items and row["order_status"] not in {"canceled", "unavailable"}:
            reject("orders", raw, "ORDER_REQUIRED_ITEMS", "non-canceled, available orders require at least one valid item")
        else:
            valid_orders.append(row)
    valid_orders = _frame("orders", valid_orders)
    valid_ids = set(valid_orders["order_id"])
    for row in shaped_items.to_dict("records"):
        if row["order_id"] not in valid_ids:
            reject("order_items", item_raw[(row["order_id"], row["source_row"])], "ITEM_INVALID_ORDER", "parent order failed validation")
    shaped_items = shaped_items[shaped_items["order_id"].isin(valid_ids)]

    merged_orders = dedupe_orders(_combine(existing["orders"], valid_orders))
    candidate_versions = {(row.order_id, row.batch_id, row.source_row) for row in valid_orders.itertuples()}
    winning_ids = {
        row.order_id for row in merged_orders.itertuples()
        if (row.order_id, row.batch_id, row.source_row) in candidate_versions
    }
    merged_items = _combine(
        existing["order_items"][~existing["order_items"]["order_id"].isin(winning_ids)],
        shaped_items[shaped_items["order_id"].isin(winning_ids)],
    )
    frames = {"orders": merged_orders, "order_items": merged_items, **references}
    return SilverResult(frames, pd.DataFrame(rejected, columns=QUARANTINE_COLUMNS))
