-- =========================================
-- 1. Analytics schema
-- =========================================
CREATE SCHEMA IF NOT EXISTS analytics;

-- =========================================
-- 2. Dimension tables
-- =========================================

-- 2.1 Date dimension
-- One row per calendar date.
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key        DATE PRIMARY KEY,          -- Surrogate key, same as the calendar date
    year            INT NOT NULL,
    quarter         INT NOT NULL,              -- 1 to 4
    month           INT NOT NULL,              -- 1 to 12
    month_name      VARCHAR NOT NULL,          -- e.g. 'January'
    day             INT NOT NULL,              -- day of month, 1 to 31
    day_of_week     INT NOT NULL,              -- 1 to 7 (ISO, Monday is 1)
    day_name        VARCHAR NOT NULL,          -- e.g. 'Monday'
    week_of_year    INT NOT NULL,              -- 1 to 53
    is_weekend      BOOLEAN NOT NULL           -- TRUE if Saturday or Sunday
);

-- 2.2 Customer dimension
-- Copy of core.customers, used as the customer dimension.
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_id            VARCHAR PRIMARY KEY,
    customer_unique_id     VARCHAR,
    customer_zip_code_prefix INT,
    customer_city          VARCHAR,
    customer_state         CHAR(2)
);

-- 2.3 Product dimension
-- Product attributes plus category translation.
CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_id                 VARCHAR PRIMARY KEY,
    product_category_name      VARCHAR,
    product_category_name_english VARCHAR,
    product_name_lenght        INT,
    product_description_lenght INT,
    product_photos_qty         INT,
    product_weight_g           INT,
    product_length_cm          INT,
    product_height_cm          INT,
    product_width_cm           INT
);

-- 2.4 Seller dimension
-- Seller attributes.
CREATE TABLE IF NOT EXISTS analytics.dim_seller (
    seller_id             VARCHAR PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city           VARCHAR,
    seller_state          CHAR(2)
);

-- 2.5 Geolocation dimension (optional but useful for geographic analysis)
CREATE TABLE IF NOT EXISTS analytics.dim_geolocation (
    geolocation_zip_code_prefix INT PRIMARY KEY,
    geolocation_lat             NUMERIC(9,6),
    geolocation_lng             NUMERIC(9,6),
    geolocation_city            VARCHAR,
    geolocation_state           CHAR(2),
    geolocation_source          VARCHAR(20)
);

-- =========================================
-- 3. Fact tables
-- =========================================

-- 3.1 fact_orders
-- Grain: one row per order.
-- Contains order level financials and delivery metrics.
CREATE TABLE IF NOT EXISTS analytics.fact_orders (
    order_id              VARCHAR PRIMARY KEY,        -- Business key, from core.orders
    customer_id           VARCHAR NOT NULL,           -- FK to dim_customer
    date_key              DATE NOT NULL,              -- FK to dim_date (order date)
    items_value           NUMERIC(12,2),              -- Sum of item prices for the order
    freight_value         NUMERIC(12,2),              -- Sum of freight for the order
    total_payment_value   NUMERIC(12,2),              -- Sum of payments for the order
    distinct_products     INT,                        -- Count of distinct products in the order
    total_items           INT,                        -- Count of items in the order
    delivery_days         INT,                        -- Delivered date minus purchase date
    is_late_delivery      BOOLEAN,                    -- TRUE if delivered after estimated date
    avg_review_score      NUMERIC(3,2),               -- Average review score for the order

    CONSTRAINT fk_fact_orders_customer
        FOREIGN KEY (customer_id)
        REFERENCES analytics.dim_customer (customer_id),

    CONSTRAINT fk_fact_orders_date
        FOREIGN KEY (date_key)
        REFERENCES analytics.dim_date (date_key)
);

-- 3.2 fact_order_items
-- Grain: one row per order item.
-- Allows detailed product and seller level analysis.
CREATE TABLE IF NOT EXISTS analytics.fact_order_items (
    order_id             VARCHAR NOT NULL,
    order_item_id        INT NOT NULL,
    product_id           VARCHAR,
    seller_id            VARCHAR,
    date_key             DATE,                       -- Order date, FK to dim_date
    price                NUMERIC(10,2),
    freight_value        NUMERIC(10,2),
    shipping_limit_date  TIMESTAMP,

    CONSTRAINT pk_fact_order_items PRIMARY KEY (order_id, order_item_id),

    CONSTRAINT fk_fact_order_items_product
        FOREIGN KEY (product_id)
        REFERENCES analytics.dim_product (product_id),

    CONSTRAINT fk_fact_order_items_seller
        FOREIGN KEY (seller_id)
        REFERENCES analytics.dim_seller (seller_id),

    CONSTRAINT fk_fact_order_items_date
        FOREIGN KEY (date_key)
        REFERENCES analytics.dim_date (date_key)
);

-- 3.3 fact_daily_sales
-- Grain: one row per day.
-- Aggregated daily financial metrics.
CREATE TABLE IF NOT EXISTS analytics.fact_daily_sales (
    date_key             DATE PRIMARY KEY,           -- FK to dim_date
    total_orders         INT,
    total_items_value    NUMERIC(12,2),
    total_freight_value  NUMERIC(12,2),
    total_payment_value  NUMERIC(12,2),
    avg_order_value      NUMERIC(12,2),

    CONSTRAINT fk_fact_daily_sales_date
        FOREIGN KEY (date_key)
        REFERENCES analytics.dim_date (date_key)
);

-- =========================================
-- 4. Indexes for common joins
-- =========================================

-- Dimensions
CREATE INDEX IF NOT EXISTS idx_dim_customer_state
    ON analytics.dim_customer (customer_state);

CREATE INDEX IF NOT EXISTS idx_dim_product_category
    ON analytics.dim_product (product_category_name);

CREATE INDEX IF NOT EXISTS idx_dim_seller_state
    ON analytics.dim_seller (seller_state);

-- Facts
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer_id
    ON analytics.fact_orders (customer_id);

CREATE INDEX IF NOT EXISTS idx_fact_orders_date_key
    ON analytics.fact_orders (date_key);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_product_id
    ON analytics.fact_order_items (product_id);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_seller_id
    ON analytics.fact_order_items (seller_id);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_date_key
    ON analytics.fact_order_items (date_key);
