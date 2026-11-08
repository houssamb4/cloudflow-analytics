"""Evaluate the input batch separately from the deduplicated candidate snapshot."""

from dataclasses import asdict, dataclass

import pandas as pd


@dataclass(frozen=True)
class CheckResult:
    rule_id: str
    severity: str
    passed: bool
    details: dict

    def as_dict(self):
        return asdict(self)


def blocks_publication(checks: list[CheckResult]) -> bool:
    return any(check.severity == "blocking" and not check.passed for check in checks)


def evaluate_checks(
    bronze: dict[str, pd.DataFrame],
    silver: dict[str, pd.DataFrame],
    gold: dict[str, pd.DataFrame],
    quarantine: pd.DataFrame,
    previous_order_count: int | None,
    drift_threshold: float = 0.5,
) -> list[CheckResult]:
    order_ids = bronze["orders"]["order_id"].str.strip()
    duplicate_ids = sorted(set(order_ids[order_ids.ne("") & order_ids.duplicated(keep=False)]))
    item_keys = bronze["order_items"][["order_id", "order_item_id"]].apply(lambda column: column.str.strip())
    item_keys["order_item_id"] = item_keys["order_item_id"].str.lstrip("0")
    duplicate_items = int(item_keys.duplicated(keep=False).sum())
    invalid_rows = len(quarantine.drop_duplicates(["entity", "source_row"]))
    facts = gold["fact_orders"]
    dates = gold["dim_date"]
    expected_merchandise = sum(int(value) for value in silver["order_items"]["price_cents"])
    expected_freight = sum(int(value) for value in silver["order_items"]["freight_value_cents"])
    actual_merchandise = sum(int(value) for value in facts["merchandise_total_cents"])
    actual_freight = sum(int(value) for value in facts["freight_total_cents"])
    actual_total = sum(int(value) for value in facts["order_total_cents"])
    item_totals = {}
    for item in silver["order_items"].itertuples():
        count, merchandise, freight = item_totals.get(item.order_id, (0, 0, 0))
        item_totals[item.order_id] = (count + 1, merchandise + int(item.price_cents), freight + int(item.freight_value_cents))
    expected_orders = {}
    for order in silver["orders"].itertuples():
        count, merchandise, freight = item_totals.get(order.order_id, (0, 0, 0))
        revenue = merchandise if order.order_status == "delivered" else 0
        expected_orders[order.order_id] = (order.order_status, count, merchandise, freight, merchandise + freight, revenue)
    actual_orders = {
        order.order_id: (
            order.order_status, int(order.item_count), int(order.merchandise_total_cents),
            int(order.freight_total_cents), int(order.order_total_cents), int(order.recognized_revenue_cents),
        )
        for order in facts.itertuples()
    }
    expected_revenue = sum(values[-1] for values in expected_orders.values())
    actual_revenue = sum(int(value) for value in facts["recognized_revenue_cents"])
    orders_match = (
        not facts["order_id"].duplicated().any()
        and set(facts["order_id"]) == set(silver["orders"]["order_id"])
    )
    dates_match = (
        not dates["date_id"].duplicated().any()
        and set(facts["purchase_date_id"]) == set(dates["date_id"])
    )
    checks = [
        CheckResult("DUPLICATE_ORDER_IDS", "blocking", not duplicate_ids, {"duplicate_ids": duplicate_ids}),
        CheckResult("DUPLICATE_ITEM_KEYS", "blocking", duplicate_items == 0, {"duplicate_rows": duplicate_items}),
        CheckResult("INVALID_ROWS", "blocking", invalid_rows == 0, {
            "quarantined_rows": invalid_rows, "rule_counts": quarantine["rule_id"].value_counts().to_dict(),
        }),
        CheckResult("GOLD_GRAIN", "blocking", bool(orders_match and dates_match), {
            "orders_match_silver": bool(orders_match), "date_keys_match": bool(dates_match),
        }),
        CheckResult("GOLD_RECONCILIATION", "blocking", (
            expected_merchandise == actual_merchandise
            and expected_freight == actual_freight
            and actual_total == expected_merchandise + expected_freight
            and actual_revenue == expected_revenue
            and actual_orders == expected_orders
        ), {
            "silver_merchandise_cents": expected_merchandise, "gold_merchandise_cents": actual_merchandise,
            "silver_freight_cents": expected_freight, "gold_freight_cents": actual_freight,
            "gold_order_total_cents": actual_total, "silver_revenue_cents": expected_revenue,
            "gold_revenue_cents": actual_revenue, "orders_reconciled": actual_orders == expected_orders,
        }),
    ]
    current_count = len(facts)
    relative_drift = None
    if previous_order_count is not None:
        relative_drift = abs(current_count - previous_order_count) / max(previous_order_count, 1)
    checks.append(CheckResult(
        "ROW_COUNT_DRIFT", "warning", relative_drift is None or relative_drift <= drift_threshold,
        {"previous": previous_order_count, "candidate": current_count,
         "relative_drift": relative_drift, "threshold": drift_threshold},
    ))
    return checks
