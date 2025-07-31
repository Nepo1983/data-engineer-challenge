--drop table fact_sales;
--drop table dim_time;
--drop table dim_product;
--drop table dim_customer;
--drop table dim_currency_rate;

CREATE TABLE dim_product (
    product_id        INTEGER PRIMARY KEY,
    name              TEXT     NOT NULL,
    description       TEXT,
    category          text
);

CREATE TABLE dim_customer (
    customer_id       INTEGER PRIMARY KEY,
    first_name        TEXT,
    last_name         TEXT,
    email             TEXT,
    signup_date       DATE
);

CREATE TABLE dim_time (
    time_id           SERIAL PRIMARY KEY,
    sale_date         DATE     NOT NULL,
    sale_hour         INTEGER  NOT NULL,
    day_of_week       TEXT,
    month             INTEGER,
    year              INTEGER
);

CREATE TABLE dim_currency_rate (
    rate_date         DATE     NOT NULL,
    currency_from     CHAR(3)  NOT NULL,
    currency_to       CHAR(3)  NOT NULL,
    rate              NUMERIC(18,6) NOT NULL
);

-- Fact table
CREATE TABLE fact_sales (
    sale_id           SERIAL PRIMARY KEY,
    order_id          INTEGER  NOT NULL,
    customer_id       INTEGER  NOT NULL REFERENCES dim_customer(customer_id),
    product_id        INTEGER  NOT NULL REFERENCES dim_product(product_id),
    time_id           INTEGER  NOT NULL REFERENCES dim_time(time_id),
    quantity          INTEGER  NOT NULL,
    unit_price        NUMERIC(18,4) NOT NULL,
    revenue_usd       NUMERIC(18,4) NOT NULL
);
