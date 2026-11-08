"""Gold's grain is one row per order and one row per referenced purchase date."""

from zoneinfo import ZoneInfo

import pandas as pd

from cloudflow.contracts import GOLD_COLUMNS, SOURCE_TIMEZONE


def build_gold(silver: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    grouped_items = {
        order_id: group for order_id, group in silver["order_items"].groupby("order_id", sort=False)
    }
    facts = []
    dates = set()
    for order in silver["orders"].itertuples(index=False):
        date = order.order_purchase_timestamp.astimezone(ZoneInfo(SOURCE_TIMEZONE)).date()
        dates.add(date)
        items = grouped_items.get(order.order_id)
        merchandise = sum(int(value) for value in items["price_cents"]) if items is not None else 0
        freight = sum(int(value) for value in items["freight_value_cents"]) if items is not None else 0
        facts.append({
            "order_id": order.order_id, "customer_id": order.customer_id, "order_status": order.order_status,
            "order_purchase_timestamp": order.order_purchase_timestamp, "order_updated_at": order.order_updated_at,
            "purchase_date_id": int(date.strftime("%Y%m%d")), "item_count": 0 if items is None else len(items),
            "merchandise_total_cents": merchandise, "freight_total_cents": freight,
            "order_total_cents": merchandise + freight,
            "recognized_revenue_cents": merchandise if order.order_status == "delivered" else 0,
            "currency": order.currency, "source_batch_id": order.batch_id,
        })
    date_rows = [{
        "date_id": int(date.strftime("%Y%m%d")), "date": date, "calendar_year": date.year,
        "calendar_month": date.month, "day_of_month": date.day, "iso_day_of_week": date.isoweekday(),
    } for date in sorted(dates)]
    return {
        "fact_orders": pd.DataFrame(facts, columns=list(GOLD_COLUMNS["fact_orders"])),
        "dim_date": pd.DataFrame(date_rows, columns=list(GOLD_COLUMNS["dim_date"])),
    }
