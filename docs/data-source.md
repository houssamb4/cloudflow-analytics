# Data source and local data contract

## Selected dataset and license

- **Dataset:** Brazilian E-Commerce Public Dataset by Olist.
- **Publisher:** Olist; requested as `olist/brazilian-ecommerce`, published under the canonical Kaggle identifier `olistbr/brazilian-ecommerce`.
- **Source:** https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
- **Dataset license:** Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International (**CC BY-NC-SA 4.0**).
- **License text:** https://creativecommons.org/licenses/by-nc-sa/4.0/
- **Attribution:** Olist, *Brazilian E-Commerce Public Dataset by Olist*, published on Kaggle; local schema adaptation and synthetic fixtures by CloudFlow Analytics contributors.

The source describes approximately 100,000 Brazilian marketplace orders from 2016–2018. It is a historical release, not a daily change feed. Confirm the license and current release terms before any real-data use. Real copies/adaptations require attribution, a license link, indication of changes, noncommercial use, and ShareAlike as applicable. Public availability does not grant unrestricted commercial use. The repository's code license remains unselected and is separate from the dataset license.

**No Olist data has been downloaded or committed.** Every CSV in `tests/fixtures/` is independently authored, deterministic synthetic data. No actual customer identifiers, purchases, or personal data are present. The dataset is the selected schema/use-case reference, not the bytes ingested by this milestone. There is no Kaggle client, network ingestion, credential requirement, or AWS call.

## Source schema versus the local subset

Olist distributes separate CSVs for orders, order items, customers, products, sellers, payments, reviews, geolocation, and category translations. This milestone implements only the following subset. The runner intentionally accepts these exact headers, in this order; the original full Kaggle CSVs are **not** drop-in inputs.

| Local file | Required headers, in order | Declared source grain |
| --- | --- | --- |
| `orders.csv` | `order_id,customer_id,order_status,order_purchase_timestamp,order_updated_at` | One order version per `order_id` in a batch |
| `order_items.csv` | `order_id,order_item_id,product_id,price,freight_value` | One line per `(order_id, order_item_id)` |
| `customers.csv` | `customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state` | One row per `customer_id` |
| `products.csv` | `product_id,product_category_name` | One row per `product_id` |

Real Olist orders include approval, carrier-delivery, customer-delivery, and estimated-delivery timestamps; there is **no `order_updated_at` CDC field**. That field is an explicit synthetic extension for the late-arrival demonstration. The subset omits sellers, shipping deadlines, product measurements, payments, reviews, and geolocation. Revenue here is based on item prices, not payment settlement.

### Fixture batches

| Directory | Orders / items / customers / products | Scenario |
| --- | --- | --- |
| `tests/fixtures/clean/` | 3 / 4 / 3 / 3 | Delivered multi-line order, shipped order, canceled order; decimal-comma amount and leading-zero postal prefix |
| `tests/fixtures/invalid/` | 4 / 3 / 3 / 3 | Duplicate `order_002` versions, missing price for `order_004`, clean `order_005` |
| `tests/fixtures/corrected/` | 3 / 3 / 3 / 3 | Remove duplicate, supply missing price, deliver the late-arriving update of `order_002`, add orders 004 and 005 |

Example source records:

```csv
order_id,customer_id,order_status,order_purchase_timestamp,order_updated_at
order_002,customer_002,shipped,2018-01-02 23:30:00,2018-01-04 09:00:00
```

```csv
order_id,order_item_id,product_id,price,freight_value
order_001,2,product_002,"25,50",2.50
```

The corrected version of `order_002` retains its original purchase timestamp but changes status to `delivered` with `order_updated_at=2018-01-06 11:00:00`. These batches simulate arrivals from static historical data; they do not claim a production daily extraction or source watermark.

## Ingestion and Bronze contract

Inputs are four UTF-8 (optional BOM) CSVs. Headers must match the subset exactly; malformed headers or field counts fail the run. Empty fields are empty strings, never silently inferred as nulls; leading zeros and original casing are retained. Bronze stores every parsed field as text and also stores the original file bytes in `bronze.files`.

Each Bronze record carries:

| Field | Meaning |
| --- | --- |
| `source` | `synthetic://olist` — explicitly labels the synthetic source |
| `batch_id` | SHA-256 of the canonical source + per-file checksum manifest |
| `checksum` | SHA-256 of the original bytes of that row's CSV |
| `ingested_at` | UTC time of the **first** ingestion of this content-addressed batch |
| `source_row` | One-based data-record position, excluding the header |

`bronze.batches` records the manifest, first run, first ingestion timestamp, and input path. `bronze.files` preserves the immutable byte snapshots. A corrected file creates a new batch ID without replacing the original input. Identical bytes under a different path have the same batch ID. Bronze's `(batch_id, source_row)` primary keys prevent duplicates when retrying even a blocked batch.

## Silver data contract

Silver is a typed candidate snapshot, merged with the last successfully published Silver snapshot. Validated candidates are written to `staging.*`; only successful publication replaces `silver.*` and `gold.*` together.

- Strip surrounding whitespace from identifiers; identifiers are case-sensitive.
- All five order fields are required. Customer identifiers and unique-customer identifiers are required; product identifiers are required. Other reference attributes may be empty. Normalize customer state to uppercase.
- Allowed statuses: `created`, `approved`, `invoiced`, `processing`, `shipped`, `delivered`, `unavailable`, `canceled`. Normalize case and the fixture spelling `cancelled` to `canceled`.
- Require valid ISO 8601 timestamps with seconds and `order_updated_at >= order_purchase_timestamp`. Offset-free timestamps mean **America/Sao_Paulo**, including historical DST; store UTC `TIMESTAMPTZ`. Reject ambiguous or nonexistent local times unless an explicit offset is supplied. Reject timestamps outside pandas' nanosecond range rather than crashing the transform. Purchase dates in Gold use the Brazilian local calendar, not UTC dates.
- Money is **BRL**. Require nonnegative finite values, no thousands separators, and at most two fractional digits. Accept a decimal point or a quoted decimal comma. Never round silently or use floating-point monetary arithmetic. Store `price_cents` and `freight_value_cents` as integer cents. Each line amount is capped at BRL 999,999,999.99; missing prices/freight, negatives, and extra precision are invalid.
- Order-item IDs must be positive BIGINT integers. Customer and product references must resolve to valid candidate/reference rows.
- Each order update includes its **complete current item set**. Every incoming item must reference a valid order version in the same batch. Non-canceled, available orders require at least one valid item. Canceled/unavailable orders may have no items; supplied items still require valid prices.
- If any line item is invalid, quarantine its parent order and exclude **all** of its lines from the candidate. Partial totals must never be published.
- Deduplicate orders at `order_id`: largest `order_updated_at` wins, with first `ingested_at`, `batch_id`, then `source_row` as deterministic tie-breakers. Duplicate IDs **within one input batch still block publication**, even though Silver can construct a deduplicated candidate. Versions across different batches are expected updates, not duplicate failures.
- An older arrival cannot replace a newer published order or its items. An accepted new order version replaces that order's entire item set, including removed lines. An absent order is not a delete; this is a simulated upsert feed, not full-source snapshot deletion.
- Customer/product rows merge by ID, using persisted ingestion time as a tie-breaker. They have no source CDC watermark; more sophisticated reference history is outside milestone 1.

### Quarantine and rule IDs

`silver.quarantine` retains `batch_id`, `entity`, `source_row`, `source`, `rule_id`, human-readable `reason`, raw-record JSON, first run ID, and UTC quarantine time. Multiple violations can refer to one row. `(batch_id, entity, source_row, rule_id)` is unique, so reruns do not duplicate rejected-row records. Bronze + batch/checksum links retain the full rejected input. History remains after successful correction.

| Row validation rule ID | Rejection |
| --- | --- |
| `REFERENCE_REQUIRED_FIELDS` | Missing customer/product identifiers |
| `ORDER_REQUIRED_FIELDS` | Missing required order fields |
| `ORDER_STATUS` | Unsupported status |
| `ORDER_TIMESTAMP` | Invalid, ambiguous, nonexistent, out-of-range, or reversed timestamps |
| `ORDER_CUSTOMER_REFERENCE` | Unknown/invalid customer |
| `ITEM_REQUIRED_FIELDS` | Missing IDs or invalid item sequence |
| `ITEM_REQUIRED_PRICE` | Empty price |
| `ITEM_MONEY` | Malformed, missing freight, negative, over-limit, or over-precise monetary value |
| `ITEM_ORDER_REFERENCE` / `ITEM_PRODUCT_REFERENCE` | Invalid parent/product reference |
| `ORDER_INVALID_ITEMS` | Parent of an invalid item; exclude partial totals |
| `ORDER_REQUIRED_ITEMS` | Required complete item set is absent |
| `ITEM_INVALID_ORDER` | Otherwise valid line excluded because its parent failed |

## Quality and publication contract

Quality results are stored in both `observability.check_results` and the run's `check_results` JSON. Checks run against the input and the **stored staging tables**, before publication.

| Check ID | Severity | Meaning |
| --- | --- | --- |
| `DUPLICATE_ORDER_IDS` | Blocking | Repeated normalized, nonempty order ID within an incoming batch |
| `DUPLICATE_ITEM_KEYS` | Blocking | Repeated `(order_id, order_item_id)` within an incoming batch |
| `INVALID_ROWS` | Blocking | Any row quarantined in this batch |
| `GOLD_GRAIN` | Blocking | Unique fact/date keys; fact order IDs equal Silver order IDs; date keys match facts |
| `GOLD_RECONCILIATION` | Blocking | Merchandise, freight, total, and revenue reconcile to the Silver-derived model |
| `ROW_COUNT_DRIFT` | Warning | Candidate fact count changes by more than the configured fraction of the last published count |

Default drift threshold is **0.50**, with `abs(candidate - previous) / max(previous, 1)`; exactly 50% passes. The first run has no baseline and passes with `relative_drift=null`. Only successful publication provides a new baseline. The clean-to-corrected growth from 3 to 5 orders produces a 66.67% warning and **still publishes**. Override with `--drift-threshold` (finite nonnegative fraction).

Any blocking failure leaves the complete previously published Silver/Gold snapshot and publication timestamp unchanged. If no snapshot has ever passed, `observability.publication` has no row: outputs are **unavailable**, not validated zero totals. Staging and Bronze/quarantine remain inspectable. Curated replacement, publication metadata, and final successful run metadata are one DuckDB transaction; a write error rolls all of them back.

`observability.runs` records a unique `run_id` per attempt, batch ID, input path, optional `retry_of`, status (`running`, `published`, `blocked`, `failed`), start/finish times, row counts, check results, publication ID, and error. Counts distinguish input, cumulative Bronze, candidate, current publication, and quarantine rows versus violations. `--retry-of` must name a blocked/failed run; corrected input automatically has a new immutable batch ID. This is a local CLI audit link, **not** an authenticated recovery service.

`observability.publications` keeps successful publication versions; `observability.publication` is the singleton current version. A retry producing identical Silver/Gold content reuses the existing publication ID and timestamp, while still recording a new run attempt. Reference tables carry Bronze batch metadata; facts carry `source_batch_id`. `observability.table_contracts` declares the Gold grains.

## Gold model and metric definitions

- **`gold.fact_orders`: one row per `order_id`.** Aggregate items **before** joining them to orders. Fields include customer/status, UTC purchase/update timestamps, local `purchase_date_id`, item count, merchandise/freight/order totals in cents, recognized revenue in cents, currency, and source batch.
- **`gold.dim_date`: one row per distinct referenced local purchase date.** `date_id=YYYYMMDD`; date, calendar year/month/day, and ISO weekday (Monday=1). It is not a continuous calendar spine.
- **Order count:** all valid orders, including canceled/unavailable.
- **Order total:** merchandise plus freight, including the original value of canceled orders if items are supplied; this is order value, **not** recognized revenue.
- **Recognized revenue:** merchandise only for `delivered` orders. Shipped/pending, canceled, and unavailable orders contribute zero. Freight is not revenue. This is a documented demonstration definition, not an accounting claim.
- **Cancellation rate:** `canceled` order count / all valid orders; `unavailable` is a separate status.
- **Average order value:** if implemented in a later dashboard, average delivered order value, with explicit inclusion/exclusion of freight. No dashboard is implemented here.
- Refund/payment data is absent; do not infer refunds or net settled revenue. Returns/refund handling requires a future source contract and is not silently modeled as negative prices.

Expected clean snapshot: 3 orders, 4 items, 3 dates, BRL 215.00 order value, BRL 125.50 recognized revenue. After clean + corrected: 5 orders, 6 items, 5 dates, BRL 291.00 order value, BRL 245.50 recognized revenue. The failed batch never contributes to the published snapshot.

## Local boundary

Run from the repository root after installing the package; see [README setup and recovery commands](../README.md#getting-started-locally). One DuckDB file contains all layer and metadata schemas. This milestone deliberately has no AWS calls, Airflow, dashboard, secrets, authentication, production watermark, distributed concurrency, or remote warehouse integration. Use a single local writer; DuckDB's file lock rejects another process trying to write the same file. Staging is the latest attempted candidate, not a per-run historical snapshot. Bronze and metadata are the immutable audit trail for this local demonstration.
