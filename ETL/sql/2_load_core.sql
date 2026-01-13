-- =========================================
-- Load core schema from raw schema
-- Idempotent: safe to run multiple times
-- =========================================

BEGIN;

-- 1. Clear existing data in core tables
TRUNCATE TABLE
    core.order_items,
    core.payments,
    core.reviews,
    core.orders,
    core.closed_deals,
    core.marketing_qualified_leads,
    core.products,
    core.sellers,
    core.customers,
    core.product_category_translation,
    core.geolocation
RESTART IDENTITY;

-- =========================================
-- 2. Dimension / reference tables
-- =========================================

-- 2.1 Geolocation
-- Build geolocation from raw.geolocation, plus any prefixes
-- that appear in customers/sellers but not in geolocation. (To resolve DDL conflict)

INSERT INTO core.geolocation (
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
)
-- A) prefixes that exist in raw.geolocation (with real data)
SELECT
    g.geolocation_zip_code_prefix,
    AVG(g.geolocation_lat) AS geolocation_lat,
    AVG(g.geolocation_lng) AS geolocation_lng,
    MIN(g.geolocation_city) AS geolocation_city,
    MIN(g.geolocation_state) AS geolocation_state
FROM raw.geolocation g
GROUP BY g.geolocation_zip_code_prefix

UNION

-- B) prefixes from customers not present in raw.geolocation
SELECT DISTINCT
    c.customer_zip_code_prefix AS geolocation_zip_code_prefix,
    NULL::numeric(9,6)         AS geolocation_lat,
    NULL::numeric(9,6)         AS geolocation_lng,
    NULL::varchar              AS geolocation_city,
    NULL::char(2)              AS geolocation_state
FROM raw.customers c
LEFT JOIN raw.geolocation g2
    ON c.customer_zip_code_prefix = g2.geolocation_zip_code_prefix
WHERE g2.geolocation_zip_code_prefix IS NULL
  AND c.customer_zip_code_prefix IS NOT NULL

UNION

-- C) prefixes from sellers not present in raw.geolocation
SELECT DISTINCT
    s.seller_zip_code_prefix   AS geolocation_zip_code_prefix,
    NULL::numeric(9,6)         AS geolocation_lat,
    NULL::numeric(9,6)         AS geolocation_lng,
    NULL::varchar              AS geolocation_city,
    NULL::char(2)              AS geolocation_state
FROM raw.sellers s
LEFT JOIN raw.geolocation g3
    ON s.seller_zip_code_prefix = g3.geolocation_zip_code_prefix
WHERE g3.geolocation_zip_code_prefix IS NULL
  AND s.seller_zip_code_prefix IS NOT NULL;


-- 2.2 Product category translation (complete)
INSERT INTO core.product_category_translation (
    product_category_name,
    product_category_name_english
)
SELECT DISTINCT
    p.product_category_name,
    t.product_category_name_english
FROM raw.products p
LEFT JOIN raw.product_category_translation t
  ON p.product_category_name = t.product_category_name
WHERE p.product_category_name IS NOT NULL;

-- 2.3 Customers
INSERT INTO core.customers (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
)
SELECT DISTINCT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM raw.customers;

-- 2.4 Sellers
INSERT INTO core.sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
)
SELECT DISTINCT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM raw.sellers;

-- 2.5 Products
INSERT INTO core.products (
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
SELECT DISTINCT
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM raw.products;

-- 2.6 Marketing qualified leads
INSERT INTO core.marketing_qualified_leads (
    mql_id,
    first_contact_date,
    landing_page_id,
    origin
)
SELECT DISTINCT
    mql_id,
    first_contact_date::date,
    landing_page_id,
    origin
FROM raw.leads;

-- =========================================
-- 3. Fact tables
-- =========================================

-- 3.1 Orders
INSERT INTO core.orders (
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
)
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp,
    order_approved_at::timestamp,
    order_delivered_carrier_date::timestamp,
    order_delivered_customer_date::timestamp,
    order_estimated_delivery_date::timestamp
FROM raw.orders;

-- 3.2 Order items
INSERT INTO core.order_items (
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
)
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date::timestamp,
    price::numeric(10,2),
    freight_value::numeric(10,2)
FROM raw.order_items;

-- 3.3 Payments
INSERT INTO core.payments (
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
)
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value::numeric(10,2)
FROM raw.payments;

-- 3.4 Reviews (one canonical row per review_id to ensure it meets the one pk restraint)
INSERT INTO core.reviews (
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
)
SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date::timestamp AS review_creation_date,
        review_answer_timestamp::timestamp AS review_answer_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY review_id
            ORDER BY
                review_creation_date::timestamp DESC NULLS LAST,
                review_answer_timestamp::timestamp DESC NULLS LAST
        ) AS rn
    FROM raw.reviews
) t
WHERE rn = 1;


-- 3.5 Closed deals
INSERT INTO core.closed_deals (
    mql_id,
    seller_id,
    sdr_id,
    sr_id,
    won_date,
    business_segment,
    lead_type,
    lead_behaviour_profile,
    has_company,
    has_gtin,
    average_stock,
    business_type,
    declared_product_catalog_size,
    declared_monthly_revenue
)
SELECT
    cd.mql_id,
    cd.seller_id,
    cd.sdr_id,
    cd.sr_id,
    cd.won_date::date,
    cd.business_segment,
    cd.lead_type,
    cd.lead_behaviour_profile,

    CASE
        WHEN LOWER(TRIM(cd.has_company::text)) IN ('yes','y','true','t','1') THEN TRUE
        WHEN LOWER(TRIM(cd.has_company::text)) IN ('no','n','false','f','0') THEN FALSE
        ELSE NULL
    END AS has_company,

    CASE
        WHEN LOWER(TRIM(cd.has_gtin::text)) IN ('yes','y','true','t','1') THEN TRUE
        WHEN LOWER(TRIM(cd.has_gtin::text)) IN ('no','n','false','f','0') THEN FALSE
        ELSE NULL
    END AS has_gtin,

    -- average_stock: replace ranges like '20-50' with NULL
    CASE
        WHEN TRIM(cd.average_stock::text) ~ '^[0-9]+$'
            THEN TRIM(cd.average_stock::text)::int
        WHEN TRIM(cd.average_stock::text) ~ '^[0-9]+-[0-9]+$'
            THEN NULL
        ELSE NULL
    END AS average_stock,

    cd.business_type,

    -- declared_product_catalog_size: same rules
    CASE
        WHEN TRIM(cd.declared_product_catalog_size::text) ~ '^[0-9]+$'
            THEN TRIM(cd.declared_product_catalog_size::text)::int
        WHEN TRIM(cd.declared_product_catalog_size::text) ~ '^[0-9]+-[0-9]+$'
            THEN NULL
        ELSE NULL
    END AS declared_product_catalog_size,

    cd.declared_monthly_revenue
FROM raw.closed_deals cd
JOIN core.sellers s
  ON cd.seller_id = s.seller_id;


COMMIT;
