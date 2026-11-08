"""The selected Olist CSV subset and typed local table contracts."""

SOURCE = "synthetic://olist"
SOURCE_TIMEZONE = "America/Sao_Paulo"
CURRENCY = "BRL"
ORDER_STATUSES = frozenset(
    {"created", "approved", "invoiced", "processing", "shipped", "delivered", "unavailable", "canceled"}
)

RAW_COLUMNS = {
    "orders": (
        "order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_updated_at",
    ),
    "order_items": ("order_id", "order_item_id", "product_id", "price", "freight_value"),
    "customers": (
        "customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state",
    ),
    "products": ("product_id", "product_category_name"),
}

METADATA_COLUMNS = {
    "source": "VARCHAR NOT NULL",
    "batch_id": "VARCHAR NOT NULL",
    "checksum": "VARCHAR NOT NULL",
    "ingested_at": "TIMESTAMPTZ NOT NULL",
    "source_row": "BIGINT NOT NULL",
}

SILVER_COLUMNS = {
    "orders": {
        "order_id": "VARCHAR NOT NULL",
        "customer_id": "VARCHAR NOT NULL",
        "order_status": "VARCHAR NOT NULL",
        "order_purchase_timestamp": "TIMESTAMPTZ NOT NULL",
        "order_updated_at": "TIMESTAMPTZ NOT NULL",
        "currency": "VARCHAR NOT NULL",
        **METADATA_COLUMNS,
    },
    "order_items": {
        "order_id": "VARCHAR NOT NULL",
        "order_item_id": "BIGINT NOT NULL",
        "product_id": "VARCHAR NOT NULL",
        "price_cents": "BIGINT NOT NULL",
        "freight_value_cents": "BIGINT NOT NULL",
        "currency": "VARCHAR NOT NULL",
        **METADATA_COLUMNS,
    },
    "customers": {**{column: "VARCHAR" for column in RAW_COLUMNS["customers"]}, **METADATA_COLUMNS},
    "products": {**{column: "VARCHAR" for column in RAW_COLUMNS["products"]}, **METADATA_COLUMNS},
}

SILVER_KEYS = {
    "orders": ("order_id",),
    "order_items": ("order_id", "order_item_id"),
    "customers": ("customer_id",),
    "products": ("product_id",),
}

GOLD_COLUMNS = {
    "fact_orders": {
        "order_id": "VARCHAR NOT NULL",
        "customer_id": "VARCHAR NOT NULL",
        "order_status": "VARCHAR NOT NULL",
        "order_purchase_timestamp": "TIMESTAMPTZ NOT NULL",
        "order_updated_at": "TIMESTAMPTZ NOT NULL",
        "purchase_date_id": "INTEGER NOT NULL",
        "item_count": "BIGINT NOT NULL",
        "merchandise_total_cents": "BIGINT NOT NULL",
        "freight_total_cents": "BIGINT NOT NULL",
        "order_total_cents": "BIGINT NOT NULL",
        "recognized_revenue_cents": "BIGINT NOT NULL",
        "currency": "VARCHAR NOT NULL",
        "source_batch_id": "VARCHAR NOT NULL",
    },
    "dim_date": {
        "date_id": "INTEGER NOT NULL",
        "date": "DATE NOT NULL",
        "calendar_year": "INTEGER NOT NULL",
        "calendar_month": "INTEGER NOT NULL",
        "day_of_month": "INTEGER NOT NULL",
        "iso_day_of_week": "INTEGER NOT NULL",
    },
}

GOLD_KEYS = {"fact_orders": ("order_id",), "dim_date": ("date_id",)}
