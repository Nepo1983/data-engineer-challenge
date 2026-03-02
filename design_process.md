# Design Process

## 1. Data Exploration

Before designing anything, I explored the source systems to understand shape, quality, and quirks.

### Source DB1 — `ecommerce_orders`
| Table | Rows | Notes |
|---|---|---|
| `customers` | 50 | Global customer base (20+ countries) |
| `orders` | ~360 | Jan–Dec 2024; multi-currency |
| `order_items` | ~360+ | Some orders have multiple line items |

**Key findings:**
- Currencies present: `USD`, `EUR`, `GBP` (valid) + `XYZ`, `ABC`, `QWE` (invalid/test data)
- `order_items.product_id` ranges up to **110**, but the product catalog only has IDs 1–100 — 10 orphaned product IDs

### Source DB2 — `ecommerce_products`
| Table | Rows | Notes |
|---|---|---|
| `product_descriptions` | 100 | 10 categories, prices in USD |

### External API — Currency Conversion
- Endpoint: `https://api.exchangerate-api.com/v4/latest/USD`
- Returns current rates (free tier does not support historical dates)
- Response: `rates["EUR"] = 0.92` means 1 USD = 0.92 EUR
- Conversion formula: `amount_usd = amount_foreign / rates[currency]`

---

## 2. Data Warehouse Schema Design

### Approach: Star Schema

I chose a **star schema** over a normalized 3NF schema for three reasons:

1. **Query performance**: analytics tools like Tableau issue GROUP BY queries that involve few large aggregations. Star schemas reduce JOIN depth to one hop (fact → dim), which PostgreSQL can execute efficiently.
2. **Clarity**: the business questions map directly to the model — product performance lives on `dim_product`, promotion timing lives on `order_hour` in the fact table.
3. **Extensibility**: new business questions (e.g. revenue by country, by month) can be answered by adding columns to existing dims or building new views — no schema redesign needed.

### Grain Decision

The fact table grain is **one row per order line item** (`fact_order_items`). This is the most granular useful level:
- For Q1 (product performance): group by `product_key`, sum `quantity` and `line_total_usd`
- For Q2 (promotion timing): group by `order_hour`, count distinct `order_id`

A coarser grain (e.g. one row per order) would lose product-level detail; a finer grain doesn't exist in the source data.

### Table Overview

```
staging_customers        staging_orders       staging_order_items
staging_products         staging_currency_rates

        dim_product ─────────────────────────────────┐
        dim_customer ──────────── fact_order_items ───┤
        dim_date ────────────────────────────────────┘
```

### Data Quality Handling

| Issue | Approach |
|---|---|
| Invalid currencies (XYZ, ABC, QWE) | Rows are kept; `is_currency_valid = FALSE`, USD amounts = NULL. Volume metrics still count them; revenue metrics exclude them via `WHERE is_currency_valid = TRUE`. |
| Missing product descriptions (IDs 101–110) | Placeholder `dim_product` rows are auto-created with name `'Unknown Product N'` and `category = 'Unknown'`. Foreign key integrity is preserved. |
| API unavailability | Fallback hardcoded rates (2024 averages) are used so the pipeline never fails on a transient network error. |
| Idempotent re-runs | Staging tables are TRUNCATEd each run. Dims use `INSERT … ON CONFLICT DO UPDATE`. Facts use `TRUNCATE … RESTART IDENTITY` + full reload. |

---

## 3. ETL Pipeline Design

### Task Graph

```
create_schema
    ├── extract_db1 ──→ fetch_currency_rates ──┐
    └── extract_db2 ────────────────────────── load_dimensions ──→ load_facts
```

`extract_db1` and `extract_db2` run **in parallel**, reducing wall-clock time. `fetch_currency_rates` waits on `extract_db1` because it reads distinct currencies from `staging_orders` (avoiding hardcoded assumptions about what currencies exist).

### Staging Table Strategy

Data flows through staging tables (in the warehouse DB) rather than Airflow XCom. Reasons:
- XCom is stored in the Airflow metadata DB and has a ~48 KB limit by default — not suitable for hundreds of rows
- Staging tables allow each task to fail and retry independently without re-extracting from the source
- The pattern mirrors real production pipelines (ELT landing zones)

### Currency Conversion

The API returns `rates[currency] = USD/foreign` (i.e. how many foreign units equal 1 USD).

```python
rate_to_usd = 1.0 / api_rates[currency]
amount_usd  = amount_foreign * rate_to_usd
```

`rate_to_usd` is stored in `staging_currency_rates` and carried through to `fact_order_items.exchange_rate` for full auditability.

---

## 4. Business Question Answers

Both questions are directly answered by views in the warehouse.

### Q1: Top Products by Sales Volume and Revenue

```sql
SELECT * FROM vw_product_performance
ORDER BY total_revenue_usd DESC
LIMIT 10;
```

The view joins `fact_order_items` → `dim_product`, aggregates `SUM(quantity)` for volume and `SUM(line_total_usd)` for revenue (USD-normalised), and filters `WHERE is_currency_valid = TRUE` to exclude rows where conversion was not possible.

### Q2: Optimal Time of Day for Promotions

```sql
SELECT * FROM vw_hourly_order_patterns
ORDER BY total_orders DESC
LIMIT 5;
```

The view groups by `order_hour` (0–23), counting distinct orders and summing revenue. The hours with the highest order count and revenue indicate when customers are most actively purchasing — those windows are the best targets for promotions.

---

## 5. Trade-offs and What I'd Add with More Time

| Area | Current approach | Production enhancement |
|---|---|---|
| Currency rates | Current rates (free API) | Historical rates by date (paid API or open-source dataset) |
| Dimension history | Full overwrite on each run | SCD Type 2 to track customer country / product category changes over time |
| Loading strategy | Full reload (truncate + insert) | Incremental load using `order_date > last_loaded_at` watermark |
| Data quality checks | `is_currency_valid` flag | Great Expectations suite with row-count, null-rate, and referential integrity tests |
| Orchestration | `@daily` schedule | Event-driven trigger after source DB transaction batches complete |
| Dashboard | Described in views | Tableau / Metabase workbook with product ranking bar chart and hourly heatmap |

---

## 6. Deliverable Files

| File | Purpose |
|---|---|
| `database/warehouse_schema.sql` | Standalone SQL to create all DWH tables and views |
| `airflow/dags/ecommerce_etl_dag.py` | Airflow DAG with 6 tasks |
| `design_process.md` | This document |
