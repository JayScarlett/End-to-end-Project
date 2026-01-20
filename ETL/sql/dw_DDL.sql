-- =========================================
-- DW DDL — Olist Star Schema
-- Fact grain: one row per order item (order_id, order_item_id)
-- =========================================
DROP SCHEMA IF EXISTS dw CASCADE;

CREATE SCHEMA dw;

-- 1) Dimensions

CREATE TABLE dw.dim_date (
    date_key    INT PRIMARY KEY,      -- yyyymmdd
    full_date   DATE NOT NULL UNIQUE,
    year        SMALLINT NOT NULL,
    quarter     SMALLINT NOT NULL,
    month       SMALLINT NOT NULL,
    day         SMALLINT NOT NULL
);

CREATE TABLE dw.dim_customer (
    customer_key        BIGSERIAL PRIMARY KEY,
    customer_id         VARCHAR NOT NULL UNIQUE,
    customer_unique_id  VARCHAR,
    customer_city       VARCHAR,
    customer_state      CHAR(2),
    customer_zip_prefix INT
);

CREATE TABLE dw.dim_seller (
    seller_key        BIGSERIAL PRIMARY KEY,
    seller_id         VARCHAR NOT NULL UNIQUE,
    seller_city       VARCHAR,
    seller_state      CHAR(2),
    seller_zip_prefix INT
);

CREATE TABLE dw.dim_product (
    product_key                   BIGSERIAL PRIMARY KEY,
    product_id                    VARCHAR NOT NULL UNIQUE,
    product_category_name         VARCHAR,
    product_category_name_english VARCHAR
);

-- 2) Fact

CREATE TABLE dw.fact_order_item (
    order_item_fact_key BIGSERIAL PRIMARY KEY,
    order_id            VARCHAR NOT NULL,
    order_item_id       INT NOT NULL,

    customer_key        BIGINT NOT NULL REFERENCES dw.dim_customer,
    seller_key          BIGINT NOT NULL REFERENCES dw.dim_seller,
    product_key         BIGINT NOT NULL REFERENCES dw.dim_product,

    purchase_date_key           INT REFERENCES dw.dim_date,
    shipping_limit_date_key     INT REFERENCES dw.dim_date,
    delivered_customer_date_key INT REFERENCES dw.dim_date,
    estimated_delivery_date_key INT REFERENCES dw.dim_date,

    order_status        VARCHAR,

    item_price          NUMERIC(10,2) CHECK (item_price >= 0),
    freight_value       NUMERIC(10,2) CHECK (freight_value >= 0),
    item_count          SMALLINT NOT NULL DEFAULT 1,

    delivery_days       SMALLINT,
    late_delivery_flag  BOOLEAN,

    UNIQUE (order_id, order_item_id)
);


-- 3) Indexes

CREATE INDEX IF NOT EXISTS idx_foi_purchase_date_key
    ON dw.fact_order_item (purchase_date_key);

CREATE INDEX IF NOT EXISTS idx_foi_customer_key
    ON dw.fact_order_item (customer_key);

CREATE INDEX IF NOT EXISTS idx_foi_product_key
    ON dw.fact_order_item (product_key);

CREATE INDEX IF NOT EXISTS idx_foi_seller_key
    ON dw.fact_order_item (seller_key);
