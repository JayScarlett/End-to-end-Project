-- =========================================
-- 1) Date dimension (continuous calendar, no gaps)
-- =========================================
TRUNCATE TABLE dw.fact_order_item, dw.dim_date;

INSERT INTO dw.dim_date (
    date_key,
    full_date,
    year,
    quarter,
    month,
    day
)
SELECT
    TO_CHAR(d::date, 'YYYYMMDD')::int AS date_key,
    d::date                           AS full_date,
    EXTRACT(YEAR FROM d)::smallint    AS year,
    EXTRACT(QUARTER FROM d)::smallint AS quarter,
    EXTRACT(MONTH FROM d)::smallint   AS month,
    EXTRACT(DAY FROM d)::smallint     AS day
FROM generate_series(
    (
      SELECT LEAST(
        (SELECT MIN(order_purchase_timestamp)::date FROM raw.orders),
        (SELECT MIN(order_delivered_customer_date)::date FROM raw.orders),
        (SELECT MIN(order_estimated_delivery_date)::date FROM raw.orders),
        (SELECT MIN(shipping_limit_date)::date FROM raw.order_items)
      )
    ),
    (
      SELECT GREATEST(
        (SELECT MAX(order_purchase_timestamp)::date FROM raw.orders),
        (SELECT MAX(order_delivered_customer_date)::date FROM raw.orders),
        (SELECT MAX(order_estimated_delivery_date)::date FROM raw.orders),
        (SELECT MAX(shipping_limit_date)::date FROM raw.order_items)
      )
    ),
    INTERVAL '1 day'
) AS d
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
    delivered_customer_date_key,
    estimated_delivery_date_key,
    order_status,
    item_price,
    freight_value,
    item_count,
    delivery_days,
    late_delivery_flag
)
SELECT
    oi.order_id,
    oi.order_item_id::int,

    dc.customer_key,
    ds.seller_key,
    dp.product_key,

    TO_CHAR(o.order_purchase_timestamp::date, 'YYYYMMDD')::int,
    TO_CHAR(oi.shipping_limit_date::date, 'YYYYMMDD')::int,

    TO_CHAR(o.order_delivered_customer_date::date, 'YYYYMMDD')::int,
    TO_CHAR(o.order_estimated_delivery_date::date, 'YYYYMMDD')::int,

    o.order_status,

    oi.price::numeric(10,2),
    oi.freight_value::numeric(10,2),

    1 AS item_count,

    (o.order_delivered_customer_date::date
     - o.order_purchase_timestamp::date) AS delivery_days,

   (o.order_status = 'delivered'
 AND o.order_delivered_customer_date::date >
     o.order_estimated_delivery_date::date)
 AS late_delivery_flag
FROM raw.order_items oi
JOIN raw.orders o
  ON oi.order_id = o.order_id
JOIN dw.dim_customer dc
  ON o.customer_id = dc.customer_id
JOIN dw.dim_seller ds
  ON oi.seller_id = ds.seller_id
JOIN dw.dim_product dp
  ON oi.product_id = dp.product_id;

