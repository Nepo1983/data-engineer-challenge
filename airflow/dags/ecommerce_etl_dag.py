"""
eCommerce ETL Pipeline
======================
Extracts data from two PostgreSQL source systems and a currency conversion API,
transforms it, and loads a star-schema data warehouse.

DAG task flow:
    create_schema
        ├── extract_db1  (orders, customers, order_items  → staging)
        │       └── fetch_currency_rates  (API → staging_currency_rates)
        └── extract_db2  (products → staging)
                    └── (both branches merge here)
                            └── load_dimensions  (dim_product, dim_customer, dim_date)
                                        └── load_facts  (fact_order_items)

Business questions answered by the warehouse:
    Q1: Which products are top performers by sales volume and revenue?
        → SELECT * FROM vw_product_performance
    Q2: What is the optimal time of day to run promotions?
        → SELECT * FROM vw_hourly_order_patterns
"""

import logging
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ─── Connection Configuration ────────────────────────────────────────────────
# Service names match docker-compose.yml; accessible inside the Airflow
# containers because all services share the `data_pipeline` Docker network.

DB1_CONFIG = {
    "host": "postgres_db1",
    "database": "ecommerce_orders",
    "user": "postgres",
    "password": "postgres",
    "port": 5432,
}

DB2_CONFIG = {
    "host": "postgres_db2",
    "database": "ecommerce_products",
    "user": "postgres",
    "password": "postgres",
    "port": 5432,
}

DWH_CONFIG = {
    "host": "postgres_warehouse",
    "database": "data_warehouse",
    "user": "postgres",
    "password": "postgres",
    "port": 5432,
}

# Free tier endpoint — returns latest rates, base = USD.
# Example: rates["EUR"] = 0.92 means 1 USD = 0.92 EUR
# Conversion to USD: amount_usd = amount_foreign / rates[currency]
CURRENCY_API_URL = "https://api.exchangerate-api.com/v4/latest/USD"

# Fallback rates if the API is unreachable (approximate 2024 averages)
FALLBACK_RATES_FROM_USD = {"USD": 1.0, "EUR": 0.92, "GBP": 0.79}


# ─── DAG Definition ──────────────────────────────────────────────────────────

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ecommerce_etl_pipeline",
    default_args=default_args,
    description=(
        "ETL: extract from eCommerce source DBs + currency API, "
        "transform, load star-schema data warehouse."
    ),
    schedule_interval="@daily",
    catchup=False,
    tags=["ecommerce", "etl", "data_warehouse"],
)


# ─── Helper ──────────────────────────────────────────────────────────────────

def _connect(config: dict):
    return psycopg2.connect(**config)


# ─── Task 1: Create Schema ────────────────────────────────────────────────────
# Uses CREATE TABLE IF NOT EXISTS / CREATE OR REPLACE VIEW throughout, so
# this task is fully idempotent — safe to re-run against an existing warehouse.

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS staging_customers (
    customer_id INTEGER, name VARCHAR(100), email VARCHAR(150),
    registration_date TIMESTAMP, country VARCHAR(50)
);
CREATE TABLE IF NOT EXISTS staging_orders (
    order_id INTEGER, customer_id INTEGER, order_date TIMESTAMP,
    total_amount DECIMAL(10,2), currency VARCHAR(3), status VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS staging_order_items (
    order_item_id INTEGER, order_id INTEGER, product_id INTEGER,
    quantity INTEGER, unit_price DECIMAL(10,2), currency VARCHAR(3)
);
CREATE TABLE IF NOT EXISTS staging_products (
    product_id INTEGER, name VARCHAR(200), category VARCHAR(100),
    description TEXT, base_price DECIMAL(10,2), currency VARCHAR(3)
);
CREATE TABLE IF NOT EXISTS staging_currency_rates (
    currency_code VARCHAR(3) NOT NULL,
    rate_to_usd   DECIMAL(10,6) NOT NULL,
    fetched_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key    SERIAL PRIMARY KEY,
    product_id     INTEGER      NOT NULL,
    name           VARCHAR(200) NOT NULL,
    category       VARCHAR(100) NOT NULL,
    description    TEXT,
    base_price_usd DECIMAL(10,2),
    CONSTRAINT uq_dim_product_id UNIQUE (product_id)
);
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key      SERIAL PRIMARY KEY,
    customer_id       INTEGER      NOT NULL,
    name              VARCHAR(100) NOT NULL,
    country           VARCHAR(50)  NOT NULL,
    registration_date DATE,
    CONSTRAINT uq_dim_customer_id UNIQUE (customer_id)
);
CREATE TABLE IF NOT EXISTS dim_date (
    date_key    INTEGER     PRIMARY KEY,
    full_date   DATE        NOT NULL,
    year        INTEGER     NOT NULL,
    quarter     INTEGER     NOT NULL,
    month       INTEGER     NOT NULL,
    month_name  VARCHAR(20) NOT NULL,
    day         INTEGER     NOT NULL,
    day_of_week INTEGER     NOT NULL,
    day_name    VARCHAR(20) NOT NULL,
    is_weekend  BOOLEAN     NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_order_items (
    order_item_key    SERIAL       PRIMARY KEY,
    order_item_id     INTEGER      NOT NULL,
    order_id          INTEGER      NOT NULL,
    product_key       INTEGER      NOT NULL REFERENCES dim_product(product_key),
    customer_key      INTEGER      NOT NULL REFERENCES dim_customer(customer_key),
    date_key          INTEGER      NOT NULL REFERENCES dim_date(date_key),
    order_hour        INTEGER      NOT NULL,
    quantity          INTEGER      NOT NULL,
    unit_price_usd    DECIMAL(12,4),
    line_total_usd    DECIMAL(12,4),
    source_currency   VARCHAR(3)   NOT NULL,
    exchange_rate     DECIMAL(10,6),
    is_currency_valid BOOLEAN      NOT NULL DEFAULT TRUE,
    order_status      VARCHAR(20)  NOT NULL,
    loaded_at         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_order_item_id UNIQUE (order_item_id)
);
CREATE INDEX IF NOT EXISTS idx_fact_product_key  ON fact_order_items(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer_key ON fact_order_items(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_date_key     ON fact_order_items(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_hour   ON fact_order_items(order_hour);

CREATE OR REPLACE VIEW vw_product_performance AS
SELECT
    p.product_id,
    p.name                                     AS product_name,
    p.category,
    COUNT(f.order_item_key)                    AS total_line_items,
    SUM(f.quantity)                            AS total_units_sold,
    ROUND(SUM(f.line_total_usd)::NUMERIC, 2)   AS total_revenue_usd,
    ROUND(AVG(f.unit_price_usd)::NUMERIC, 2)   AS avg_unit_price_usd
FROM fact_order_items f
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.is_currency_valid = TRUE
GROUP BY p.product_id, p.name, p.category
ORDER BY total_revenue_usd DESC;

CREATE OR REPLACE VIEW vw_hourly_order_patterns AS
SELECT
    order_hour,
    COUNT(DISTINCT order_id)                   AS total_orders,
    SUM(quantity)                              AS total_units_sold,
    ROUND(SUM(line_total_usd)::NUMERIC, 2)     AS total_revenue_usd,
    ROUND(AVG(line_total_usd)::NUMERIC, 2)     AS avg_line_value_usd
FROM fact_order_items
WHERE is_currency_valid = TRUE
GROUP BY order_hour
ORDER BY order_hour;
"""


def create_schema():
    logger.info("Creating data warehouse schema…")
    conn = _connect(DWH_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(_SCHEMA_SQL)
        conn.commit()
        logger.info("Schema created/verified successfully.")
    finally:
        conn.close()


# ─── Task 2: Extract DB1 ─────────────────────────────────────────────────────
# Reads customers, orders, and order_items from the transactional source DB
# and lands them in staging tables in the warehouse.

def extract_db1():
    logger.info("Extracting from DB1 (customers, orders, order_items)…")
    src = _connect(DB1_CONFIG)
    dwh = _connect(DWH_CONFIG)
    try:
        with src.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, name, email, registration_date, country FROM customers")
            customers = cur.fetchall()

            cur.execute(
                "SELECT id, customer_id, order_date, total_amount, currency, status FROM orders"
            )
            orders = cur.fetchall()

            cur.execute(
                "SELECT id, order_id, product_id, quantity, unit_price, currency FROM order_items"
            )
            order_items = cur.fetchall()

        with dwh.cursor() as cur:
            cur.execute(
                "TRUNCATE TABLE staging_customers, staging_orders, staging_order_items"
            )

            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO staging_customers VALUES %s",
                [
                    (r["id"], r["name"], r["email"], r["registration_date"], r["country"])
                    for r in customers
                ],
            )
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO staging_orders VALUES %s",
                [
                    (r["id"], r["customer_id"], r["order_date"],
                     r["total_amount"], r["currency"], r["status"])
                    for r in orders
                ],
            )
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO staging_order_items VALUES %s",
                [
                    (r["id"], r["order_id"], r["product_id"],
                     r["quantity"], r["unit_price"], r["currency"])
                    for r in order_items
                ],
            )

        dwh.commit()
        logger.info(
            "DB1 extracted: %d customers, %d orders, %d order items.",
            len(customers), len(orders), len(order_items),
        )
    finally:
        src.close()
        dwh.close()


# ─── Task 3: Extract DB2 ─────────────────────────────────────────────────────

def extract_db2():
    logger.info("Extracting from DB2 (product_descriptions)…")
    src = _connect(DB2_CONFIG)
    dwh = _connect(DWH_CONFIG)
    try:
        with src.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, name, category, description, base_price, currency "
                "FROM product_descriptions"
            )
            products = cur.fetchall()

        with dwh.cursor() as cur:
            cur.execute("TRUNCATE TABLE staging_products")
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO staging_products VALUES %s",
                [
                    (r["id"], r["name"], r["category"],
                     r["description"], r["base_price"], r["currency"])
                    for r in products
                ],
            )

        dwh.commit()
        logger.info("DB2 extracted: %d products.", len(products))
    finally:
        src.close()
        dwh.close()


# ─── Task 4: Fetch Currency Rates ─────────────────────────────────────────────
# Queries the staging table to discover all distinct currencies actually
# present in the data, then fetches conversion rates from the API.
# Unknown currencies (XYZ, ABC, QWE) are intentionally left out of the
# staging_currency_rates table; the fact-loading SQL will detect the missing
# join and set is_currency_valid = FALSE for those rows.

def fetch_currency_rates():
    logger.info("Fetching currency conversion rates…")

    # Discover currencies present in source data
    dwh = _connect(DWH_CONFIG)
    try:
        with dwh.cursor() as cur:
            cur.execute("SELECT DISTINCT currency FROM staging_orders")
            source_currencies = {row[0] for row in cur.fetchall()}
    finally:
        dwh.close()

    logger.info("Currencies found in source data: %s", source_currencies)

    # Fetch rates from external API (base = USD)
    try:
        resp = requests.get(CURRENCY_API_URL, timeout=30)
        resp.raise_for_status()
        api_rates = resp.json().get("rates", {})
        api_rates["USD"] = 1.0  # ensure USD maps to itself
        logger.info("Currency API responded successfully.")
    except requests.exceptions.RequestException as exc:
        logger.warning("Currency API unavailable (%s). Using fallback rates.", exc)
        api_rates = FALLBACK_RATES_FROM_USD

    # Build rate_to_usd for each recognised currency.
    # api_rates[currency] = how many foreign units per 1 USD
    # → to convert N foreign to USD: N / api_rates[currency]
    # → stored as: rate_to_usd = 1 / api_rates[currency]
    rates_to_load = []
    for currency in source_currencies:
        if currency in api_rates:
            rate_to_usd = 1.0 / api_rates[currency]
            rates_to_load.append((currency, rate_to_usd))
            logger.info("  %s → rate_to_usd = %.6f", currency, rate_to_usd)
        else:
            logger.warning(
                "  %s: unrecognised currency — rows will be flagged is_currency_valid=FALSE.",
                currency,
            )

    dwh = _connect(DWH_CONFIG)
    try:
        with dwh.cursor() as cur:
            cur.execute("TRUNCATE TABLE staging_currency_rates")
            if rates_to_load:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO staging_currency_rates (currency_code, rate_to_usd) VALUES %s",
                    rates_to_load,
                )
        dwh.commit()
        logger.info("Loaded %d exchange rates into staging.", len(rates_to_load))
    finally:
        dwh.close()


# ─── Task 5: Load Dimensions ──────────────────────────────────────────────────
# Uses INSERT … ON CONFLICT DO UPDATE (upsert) so re-runs are safe and
# existing surrogate keys are preserved.

def load_dimensions():
    logger.info("Loading dimension tables…")
    conn = _connect(DWH_CONFIG)
    try:
        with conn.cursor() as cur:

            # dim_product — known products from DB2 catalog
            cur.execute("""
                INSERT INTO dim_product (product_id, name, category, description, base_price_usd)
                SELECT product_id, name, category, description, base_price
                FROM staging_products
                ON CONFLICT (product_id) DO UPDATE SET
                    name           = EXCLUDED.name,
                    category       = EXCLUDED.category,
                    description    = EXCLUDED.description,
                    base_price_usd = EXCLUDED.base_price_usd
            """)

            # dim_product — placeholder rows for product_ids in orders but not in catalog
            # (the sample data references product_ids 101–110 which have no description)
            cur.execute("""
                INSERT INTO dim_product (product_id, name, category, description, base_price_usd)
                SELECT DISTINCT
                    oi.product_id,
                    'Unknown Product ' || oi.product_id,
                    'Unknown',
                    'Product not found in catalog',
                    NULL
                FROM staging_order_items oi
                LEFT JOIN dim_product dp ON dp.product_id = oi.product_id
                WHERE dp.product_key IS NULL
                ON CONFLICT (product_id) DO NOTHING
            """)

            # dim_customer
            cur.execute("""
                INSERT INTO dim_customer (customer_id, name, country, registration_date)
                SELECT customer_id, name, country, registration_date::DATE
                FROM staging_customers
                ON CONFLICT (customer_id) DO UPDATE SET
                    name              = EXCLUDED.name,
                    country           = EXCLUDED.country,
                    registration_date = EXCLUDED.registration_date
            """)

            # dim_date — one row per unique order date; DISTINCT collapses
            # multiple orders on the same day to a single calendar entry
            cur.execute("""
                INSERT INTO dim_date (
                    date_key, full_date, year, quarter, month, month_name,
                    day, day_of_week, day_name, is_weekend
                )
                SELECT DISTINCT
                    TO_CHAR(order_date, 'YYYYMMDD')::INTEGER,
                    order_date::DATE,
                    EXTRACT(YEAR    FROM order_date)::INTEGER,
                    EXTRACT(QUARTER FROM order_date)::INTEGER,
                    EXTRACT(MONTH   FROM order_date)::INTEGER,
                    TO_CHAR(order_date, 'Month'),
                    EXTRACT(DAY     FROM order_date)::INTEGER,
                    EXTRACT(DOW     FROM order_date)::INTEGER,
                    TO_CHAR(order_date, 'Day'),
                    EXTRACT(DOW     FROM order_date) IN (0, 6)
                FROM staging_orders
                ON CONFLICT (date_key) DO NOTHING
            """)

        conn.commit()
        logger.info("Dimension tables loaded successfully.")
    finally:
        conn.close()


# ─── Task 6: Load Facts ───────────────────────────────────────────────────────
# Full reload strategy: TRUNCATE then INSERT.
# The LEFT JOIN on staging_currency_rates means rows with unknown currencies
# produce NULL for rate_to_usd, making is_currency_valid = FALSE automatically.

def load_facts():
    logger.info("Loading fact_order_items…")
    conn = _connect(DWH_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE fact_order_items RESTART IDENTITY")

            cur.execute("""
                INSERT INTO fact_order_items (
                    order_item_id, order_id,
                    product_key, customer_key, date_key, order_hour,
                    quantity,
                    unit_price_usd, line_total_usd,
                    source_currency, exchange_rate, is_currency_valid,
                    order_status
                )
                SELECT
                    oi.order_item_id,
                    oi.order_id,
                    dp.product_key,
                    dc.customer_key,
                    TO_CHAR(o.order_date, 'YYYYMMDD')::INTEGER          AS date_key,
                    EXTRACT(HOUR FROM o.order_date)::INTEGER             AS order_hour,
                    oi.quantity,
                    CASE
                        WHEN cr.rate_to_usd IS NOT NULL
                        THEN ROUND((oi.unit_price * cr.rate_to_usd)::NUMERIC, 4)
                        ELSE NULL
                    END                                                  AS unit_price_usd,
                    CASE
                        WHEN cr.rate_to_usd IS NOT NULL
                        THEN ROUND((oi.quantity * oi.unit_price * cr.rate_to_usd)::NUMERIC, 4)
                        ELSE NULL
                    END                                                  AS line_total_usd,
                    oi.currency                                          AS source_currency,
                    cr.rate_to_usd                                       AS exchange_rate,
                    cr.rate_to_usd IS NOT NULL                          AS is_currency_valid,
                    o.status                                             AS order_status
                FROM staging_order_items oi
                JOIN staging_orders      o  ON oi.order_id   = o.order_id
                JOIN dim_product         dp ON oi.product_id = dp.product_id
                JOIN dim_customer        dc ON o.customer_id  = dc.customer_id
                LEFT JOIN staging_currency_rates cr ON oi.currency = cr.currency_code
            """)

            cur.execute(
                "SELECT COUNT(*), SUM(CASE WHEN is_currency_valid THEN 1 ELSE 0 END) "
                "FROM fact_order_items"
            )
            total, valid = cur.fetchone()

        conn.commit()
        logger.info(
            "Fact table loaded: %d rows total — %d valid currency, %d flagged.",
            total, valid, total - valid,
        )
    finally:
        conn.close()


# ─── Task Definitions ─────────────────────────────────────────────────────────

t_create_schema = PythonOperator(
    task_id="create_schema",
    python_callable=create_schema,
    dag=dag,
)
t_extract_db1 = PythonOperator(
    task_id="extract_db1",
    python_callable=extract_db1,
    dag=dag,
)
t_extract_db2 = PythonOperator(
    task_id="extract_db2",
    python_callable=extract_db2,
    dag=dag,
)
t_fetch_rates = PythonOperator(
    task_id="fetch_currency_rates",
    python_callable=fetch_currency_rates,
    dag=dag,
)
t_load_dims = PythonOperator(
    task_id="load_dimensions",
    python_callable=load_dimensions,
    dag=dag,
)
t_load_facts = PythonOperator(
    task_id="load_facts",
    python_callable=load_facts,
    dag=dag,
)

# ─── Task Dependencies ────────────────────────────────────────────────────────
#
#   create_schema
#       ├── extract_db1 ── fetch_currency_rates ──┐
#       └── extract_db2 ─────────────────────────── load_dimensions ── load_facts

t_create_schema >> [t_extract_db1, t_extract_db2]
t_extract_db1 >> t_fetch_rates
[t_fetch_rates, t_extract_db2] >> t_load_dims
t_load_dims >> t_load_facts
