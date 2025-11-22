-- =========================================
-- Load analytics star schema from core schema
-- Idempotent: safe to run multiple times
-- =========================================

BEGIN;

-- =========================================
-- 1. Clear existing data in analytics tables
-- Facts first, then dimensions, to respect foreign keys.
-- =========================================

TRUNCATE TABLE
    analytics.fact_order_items,
    analytics.fact_orders,
    analytics.fact_daily_sales,
    analytics.dim_customer,
    analytics.dim_product,
    analytics.dim_seller,
    analytics.dim_geolocation,
    analytics.dim_date;

-- =========================================
-- 2. Populate dimension tables
-- =========================================

-- 2.1 dim_date
-- Build a continuous date range from the minimum to maximum order date.
INSERT INTO analytics.dim_date (
    date_key,
    year,
    quarter,
    month,
    month_name,
    day,
    day_of_week,
    day_name,
    week_of_year,
    is_weekend
)
SELECT
    d::date AS date_key,
    EXTRACT(YEAR FROM d)::int AS year,
    EXTRACT(QUARTER FROM d)::int AS quarter,
    EXTRACT(MONTH FROM d)::int AS month,
    TO_CHAR(d, 'TMMonth') AS month_name,
    EXTRACT(DAY FROM d)::int AS day,
    EXTRACT(ISODOW FROM d)::int AS day_of_week,
    TO_CHAR(d, 'TMDay') AS day_name,
    EXTRACT(WEEK FROM d)::int AS week_of_year,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series(
    (SELECT MIN(order_purchase_timestamp)::date FROM core.orders),
    (SELECT MAX(order_purchase_timestamp)::date FROM core.orders),
    INTERVAL '1 day'
) AS d;

-- 2.2 dim_customer
-- Direct copy of core.customers.
INSERT INTO analytics.dim_customer (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
)
SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state
FROM core.customers c;

-- 2.3 dim_product
-- Join products with category translation to include english category name.
INSERT INTO analytics.dim_product (
    product_id,
    product_category_name,
    product_category_name_english,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
SELECT
    p.product_id,
    p.product_category_name,
    t.product_category_name_english,
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM core.products p
LEFT JOIN core.product_category_translation t
    ON p.product_category_name = t.product_category_name;

-- 2.4 dim_seller
-- Direct copy of core.sellers.
INSERT INTO analytics.dim_seller (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
)
SELECT
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state
FROM core.sellers s;

-- 2.5 dim_geolocation
-- Direct copy of core.geolocation.
INSERT INTO analytics.dim_geolocation (
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    geolocation_source
)
SELECT
    g.geolocation_zip_code_prefix,
    g.geolocation_lat,
    g.geolocation_lng,
    g.geolocation_city,
    g.geolocation_state,
    g.geolocation_source
FROM core.geolocation g;

-- =========================================
-- 3. Populate fact tables
-- =========================================

-- 3.1 fact_orders
-- Source:
--   core.v_order_summary (financials and item counts)
--   core.v_order_delivery_metrics (delivery and reviews)
INSERT INTO analytics.fact_orders (
    order_id,
    customer_id,
    date_key,
    items_value,
    freight_value,
    total_payment_value,
    distinct_products,
    total_items,
    delivery_days,
    is_late_delivery,
    avg_review_score
)
SELECT
    os.order_id,
    os.customer_id,
    os.order_purchase_timestamp::date AS date_key,
    os.items_value,
    os.freight_value,
    os.total_payment_value,
    os.distinct_products,
    os.total_items,
    odm.delivery_days,
    odm.is_late_delivery,
    odm.avg_review_score
FROM core.v_order_summary os
LEFT JOIN core.v_order_delivery_metrics odm
    ON odm.order_id = os.order_id;

-- 3.2 fact_order_items
-- Source:
--   core.order_items for item level financials
--   core.orders for order date
INSERT INTO analytics.fact_order_items (
    order_id,
    order_item_id,
    product_id,
    seller_id,
    date_key,
    price,
    freight_value,
    shipping_limit_date
)
SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    o.order_purchase_timestamp::date AS date_key,
    oi.price,
    oi.freight_value,
    oi.shipping_limit_date
FROM core.order_items oi
JOIN core.orders o
    ON o.order_id = oi.order_id;

-- 3.3 fact_daily_sales
-- Source:
--   core.v_daily_sales which already aggregates daily metrics.
INSERT INTO analytics.fact_daily_sales (
    date_key,
    total_orders,
    total_items_value,
    total_freight_value,
    total_payment_value,
    avg_order_value
)
SELECT
    order_date AS date_key,
    total_orders,
    total_items_value,
    total_freight_value,
    total_payment_value,
    avg_order_value
FROM core.v_daily_sales;

COMMIT;
