BEGIN;

-- =========================================
-- 1) Date dimension
-- =========================================
INSERT INTO dw.dim_date (
    date_key,
    full_date,
    year,
    quarter,
    month,
    day
)
SELECT DISTINCT
    TO_CHAR(d::date, 'YYYYMMDD')::int AS date_key,
    d::date                           AS full_date,
    EXTRACT(YEAR FROM d)::smallint,
    EXTRACT(QUARTER FROM d)::smallint,
    EXTRACT(MONTH FROM d)::smallint,
    EXTRACT(DAY FROM d)::smallint
FROM (
    SELECT order_purchase_timestamp::date AS d FROM raw.orders
    UNION
    SELECT order_delivered_customer_date::date FROM raw.orders
    UNION
    SELECT order_estimated_delivery_date::date FROM raw.orders
    UNION
    SELECT shipping_limit_date::date FROM raw.order_items
) t
WHERE d IS NOT NULL;

-- =========================================
-- 2) Customer dimension
-- =========================================
INSERT INTO dw.dim_customer (
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    customer_zip_prefix
)
SELECT DISTINCT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    customer_zip_code_prefix::int
FROM raw.customers;

-- =========================================
-- 3) Seller dimension
-- =========================================
INSERT INTO dw.dim_seller (
    seller_id,
    seller_city,
    seller_state,
    seller_zip_prefix
)
SELECT DISTINCT
    seller_id,
    seller_city,
    seller_state,
    seller_zip_code_prefix::int
FROM raw.sellers;

-- =========================================
-- 4) Product dimension
-- =========================================
INSERT INTO dw.dim_product (
    product_id,
    product_category_name,
    product_category_name_english
)
SELECT DISTINCT
    p.product_id,
    p.product_category_name,
    t.product_category_name_english
FROM raw.products p
LEFT JOIN raw.product_category_translation t
  ON p.product_category_name = t.product_category_name;

-- =========================================
-- 5) Fact: order item
-- =========================================
INSERT INTO dw.fact_order_item (
    order_id,
    order_item_id,
    customer_key,
    seller_key,
    product_key,
    purchase_date_key,
    shipping_limit_date_key,
    order_status,
    item_price,
    freight_value
)
SELECT
    oi.order_id,
    oi.order_item_id::int,

    dc.customer_key,
    ds.seller_key,
    dp.product_key,

    TO_CHAR(o.order_purchase_timestamp::date, 'YYYYMMDD')::int,
    TO_CHAR(oi.shipping_limit_date::date, 'YYYYMMDD')::int,

    o.order_status,

    oi.price::numeric(10,2),
    oi.freight_value::numeric(10,2)
FROM raw.order_items oi
JOIN raw.orders o
  ON oi.order_id = o.order_id
JOIN dw.dim_customer dc
  ON o.customer_id = dc.customer_id
JOIN dw.dim_seller ds
  ON oi.seller_id = ds.seller_id
JOIN dw.dim_product dp
  ON oi.product_id = dp.product_id;

COMMIT;
