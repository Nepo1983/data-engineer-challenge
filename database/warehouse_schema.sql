-- ============================================================
-- Data Warehouse Schema
-- eCommerce Analytics — Senior Data Engineer Challenge
-- ============================================================
-- Star schema targeting two business questions:
--   1. Top products by sales volume and revenue
--   2. Optimal time of day for promotions
--
-- Design:
--   Staging tables  -> cleared every ETL run, hold raw extracted data
--   Dimension tables -> slowly-changing, upserted on each run
--   Fact table      -> fully reloaded each run (small dataset)
--   Analytics views -> pre-built answers for the business questions
-- ============================================================


-- ─── Staging Tables (truncated each ETL run) ───────────────────────────────

CREATE TABLE IF NOT EXISTS staging_customers (
    customer_id       INTEGER,
    name              VARCHAR(100),
    email             VARCHAR(150),
    registration_date TIMESTAMP,
    country           VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS staging_orders (
    order_id     INTEGER,
    customer_id  INTEGER,
    order_date   TIMESTAMP,
    total_amount DECIMAL(10,2),
    currency     VARCHAR(3),
    status       VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS staging_order_items (
    order_item_id INTEGER,
    order_id      INTEGER,
    product_id    INTEGER,
    quantity      INTEGER,
    unit_price    DECIMAL(10,2),
    currency      VARCHAR(3)
);

CREATE TABLE IF NOT EXISTS staging_products (
    product_id  INTEGER,
    name        VARCHAR(200),
    category    VARCHAR(100),
    description TEXT,
    base_price  DECIMAL(10,2),
    currency    VARCHAR(3)
);

-- Populated by the currency conversion API task.
-- rate_to_usd = multiply this by a foreign amount to get USD.
CREATE TABLE IF NOT EXISTS staging_currency_rates (
    currency_code VARCHAR(3)    NOT NULL,
    rate_to_usd   DECIMAL(10,6) NOT NULL,
    fetched_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ─── Dimension: Product ────────────────────────────────────────────────────
-- Sourced from DB2 (product_descriptions).
-- Placeholder rows are inserted for product_ids found in order_items
-- but absent from the catalog (ids 101-110 in the sample data).

CREATE TABLE IF NOT EXISTS dim_product (
    product_key    SERIAL PRIMARY KEY,
    product_id     INTEGER       NOT NULL,
    name           VARCHAR(200)  NOT NULL,
    category       VARCHAR(100)  NOT NULL,
    description    TEXT,
    base_price_usd DECIMAL(10,2),
    CONSTRAINT uq_dim_product_id UNIQUE (product_id)
);


-- ─── Dimension: Customer ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key      SERIAL PRIMARY KEY,
    customer_id       INTEGER      NOT NULL,
    name              VARCHAR(100) NOT NULL,
    country           VARCHAR(50)  NOT NULL,
    registration_date DATE,
    CONSTRAINT uq_dim_customer_id UNIQUE (customer_id)
);


-- ─── Dimension: Date ───────────────────────────────────────────────────────
-- Date-level granularity only.
-- The hour of day lives in the fact table because orders carry a time
-- component and we need it for the promotion-timing business question.

CREATE TABLE IF NOT EXISTS dim_date (
    date_key    INTEGER      PRIMARY KEY,  -- YYYYMMDD integer for fast joins
    full_date   DATE         NOT NULL,
    year        INTEGER      NOT NULL,
    quarter     INTEGER      NOT NULL,
    month       INTEGER      NOT NULL,
    month_name  VARCHAR(20)  NOT NULL,
    day         INTEGER      NOT NULL,
    day_of_week INTEGER      NOT NULL,    -- PostgreSQL DOW: 0=Sunday … 6=Saturday
    day_name    VARCHAR(20)  NOT NULL,
    is_weekend  BOOLEAN      NOT NULL
);


-- ─── Fact: Order Items ─────────────────────────────────────────────────────
-- Grain: one row per order line item.
-- All prices are normalised to USD via exchange rates from the API.
-- Rows with unrecognised currencies (XYZ, ABC, QWE) are retained but
-- flagged is_currency_valid = FALSE with NULL USD amounts, so they
-- are excluded from revenue sums without silently losing volume data.

CREATE TABLE IF NOT EXISTS fact_order_items (
    order_item_key    SERIAL       PRIMARY KEY,
    order_item_id     INTEGER      NOT NULL,
    order_id          INTEGER      NOT NULL,
    product_key       INTEGER      NOT NULL REFERENCES dim_product(product_key),
    customer_key      INTEGER      NOT NULL REFERENCES dim_customer(customer_key),
    date_key          INTEGER      NOT NULL REFERENCES dim_date(date_key),
    order_hour        INTEGER      NOT NULL,  -- 0–23, used for promotion analysis
    quantity          INTEGER      NOT NULL,
    unit_price_usd    DECIMAL(12,4),          -- NULL when currency unrecognised
    line_total_usd    DECIMAL(12,4),          -- quantity × unit_price_usd
    source_currency   VARCHAR(3)   NOT NULL,
    exchange_rate     DECIMAL(10,6),          -- rate_to_usd applied
    is_currency_valid BOOLEAN      NOT NULL DEFAULT TRUE,
    order_status      VARCHAR(20)  NOT NULL,
    loaded_at         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_order_item_id UNIQUE (order_item_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_product_key ON fact_order_items(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer_key ON fact_order_items(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_date_key ON fact_order_items(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_hour ON fact_order_items(order_hour);


-- ─── Analytics Views ───────────────────────────────────────────────────────

-- Q1: Top products by sales volume and revenue
CREATE OR REPLACE VIEW vw_product_performance AS
SELECT
    p.product_id,
    p.name                                           AS product_name,
    p.category,
    COUNT(f.order_item_key)                          AS total_line_items,
    SUM(f.quantity)                                  AS total_units_sold,
    ROUND(SUM(f.line_total_usd)::NUMERIC, 2)         AS total_revenue_usd,
    ROUND(AVG(f.unit_price_usd)::NUMERIC, 2)         AS avg_unit_price_usd
FROM fact_order_items f
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.is_currency_valid = TRUE
GROUP BY p.product_id, p.name, p.category
ORDER BY total_revenue_usd DESC;


-- Q2: Order volume and revenue by hour of day (promotion timing)
CREATE OR REPLACE VIEW vw_hourly_order_patterns AS
SELECT
    order_hour,
    COUNT(DISTINCT order_id)                         AS total_orders,
    SUM(quantity)                                    AS total_units_sold,
    ROUND(SUM(line_total_usd)::NUMERIC, 2)           AS total_revenue_usd,
    ROUND(AVG(line_total_usd)::NUMERIC, 2)           AS avg_line_value_usd
FROM fact_order_items
WHERE is_currency_valid = TRUE
GROUP BY order_hour
ORDER BY order_hour;
