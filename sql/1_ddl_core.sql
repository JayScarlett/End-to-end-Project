-- =========================================
-- 1. Schemas
-- =========================================
CREATE SCHEMA IF NOT EXISTS core;

-- =========================================
-- 2. Reference and dimension tables
-- =========================================

-- 2.1 Geolocation
CREATE TABLE IF NOT EXISTS core.geolocation (
    geolocation_zip_code_prefix INT PRIMARY KEY,
    geolocation_lat NUMERIC(9,6),
    geolocation_lng NUMERIC(9,6),
    geolocation_city VARCHAR,
    geolocation_state CHAR(2),
    geolocation_source VARCHAR(20)  -- 'raw' or 'inferred'
);


-- 2.2 Product category translation
CREATE TABLE IF NOT EXISTS core.product_category_translation (
    product_category_name VARCHAR PRIMARY KEY,
    product_category_name_english VARCHAR
);

-- 2.3 Customers
CREATE TABLE IF NOT EXISTS core.customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_unique_id VARCHAR,
    customer_zip_code_prefix INT,
    customer_city VARCHAR,
    customer_state CHAR(2),
    CONSTRAINT fk_customers_geolocation
        FOREIGN KEY (customer_zip_code_prefix)
        REFERENCES core.geolocation (geolocation_zip_code_prefix)
);

-- 2.4 Sellers
CREATE TABLE IF NOT EXISTS core.sellers (
    seller_id VARCHAR PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR,
    seller_state CHAR(2),
    CONSTRAINT fk_sellers_geolocation
        FOREIGN KEY (seller_zip_code_prefix)
        REFERENCES core.geolocation (geolocation_zip_code_prefix)
);

-- 2.5 Products
CREATE TABLE IF NOT EXISTS core.products (
    product_id VARCHAR PRIMARY KEY,
    product_category_name VARCHAR,
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,
    CONSTRAINT fk_products_category
        FOREIGN KEY (product_category_name)
        REFERENCES core.product_category_translation (product_category_name)
);

-- =========================================
-- 3. Fact tables (orders, items, payments, reviews)
-- =========================================

-- 3.1 Orders
CREATE TABLE IF NOT EXISTS core.orders (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id)
        REFERENCES core.customers (customer_id)
);

-- 3.2 Order items
CREATE TABLE IF NOT EXISTS core.order_items (
    order_id VARCHAR NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR,
    seller_id VARCHAR,
    shipping_limit_date TIMESTAMP,
    price NUMERIC(10,2),
    freight_value NUMERIC(10,2),
    CONSTRAINT pk_order_items PRIMARY KEY (order_id, order_item_id),
    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id)
        REFERENCES core.orders (order_id),
    CONSTRAINT fk_order_items_product
        FOREIGN KEY (product_id)
        REFERENCES core.products (product_id),
    CONSTRAINT fk_order_items_seller
        FOREIGN KEY (seller_id)
        REFERENCES core.sellers (seller_id),
    CONSTRAINT chk_order_items_price_nonneg
        CHECK (price IS NULL OR price >= 0),
    CONSTRAINT chk_order_items_freight_nonneg
        CHECK (freight_value IS NULL OR freight_value >= 0)
);

-- 3.3 Payments
CREATE TABLE IF NOT EXISTS core.payments (
    order_id VARCHAR NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR,
    payment_installments INT,
    payment_value NUMERIC(10,2),
    CONSTRAINT pk_payments PRIMARY KEY (order_id, payment_sequential),
    CONSTRAINT fk_payments_order
        FOREIGN KEY (order_id)
        REFERENCES core.orders (order_id),
    CONSTRAINT chk_payments_installments_nonneg
        CHECK (payment_installments IS NULL OR payment_installments >= 0),
    CONSTRAINT chk_payments_value_nonneg
        CHECK (payment_value IS NULL OR payment_value >= 0)
);

-- 3.4 Reviews
CREATE TABLE IF NOT EXISTS core.reviews (
    review_id VARCHAR PRIMARY KEY,
    order_id VARCHAR,
    review_score INT,
    review_comment_title VARCHAR,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    CONSTRAINT fk_reviews_order
        FOREIGN KEY (order_id)
        REFERENCES core.orders (order_id),
    CONSTRAINT chk_reviews_score_range
        CHECK (review_score IS NULL OR (review_score BETWEEN 1 AND 5))
);

-- =========================================
-- 4. Marketing funnel tables
-- =========================================

-- 4.1 Marketing qualified leads
CREATE TABLE IF NOT EXISTS core.marketing_qualified_leads (
    mql_id VARCHAR PRIMARY KEY,
    first_contact_date DATE,
    landing_page_id VARCHAR,
    origin VARCHAR
);

-- 4.2 Closed deals
CREATE TABLE IF NOT EXISTS core.closed_deals (
    mql_id VARCHAR PRIMARY KEY,
    seller_id VARCHAR,
    sdr_id VARCHAR,
    sr_id VARCHAR,
    won_date DATE,
    business_segment VARCHAR,
    lead_type VARCHAR,
    lead_behaviour_profile VARCHAR,
    has_company BOOLEAN,
    has_gtin BOOLEAN,
    average_stock INT,
    business_type VARCHAR,
    declared_product_catalog_size INT,
    declared_monthly_revenue VARCHAR,
    CONSTRAINT fk_closed_deals_mql
        FOREIGN KEY (mql_id)
        REFERENCES core.marketing_qualified_leads (mql_id),
    CONSTRAINT fk_closed_deals_seller
        FOREIGN KEY (seller_id)
        REFERENCES core.sellers (seller_id)
);

-- =========================================
-- 5. Indexes for performance
-- =========================================

-- 5.1 Orders and customers
CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON core.orders (customer_id);

CREATE INDEX IF NOT EXISTS idx_customers_zip_prefix
    ON core.customers (customer_zip_code_prefix);

-- 5.2 Order items joins
CREATE INDEX IF NOT EXISTS idx_order_items_order_id
    ON core.order_items (order_id);

CREATE INDEX IF NOT EXISTS idx_order_items_product_id
    ON core.order_items (product_id);

CREATE INDEX IF NOT EXISTS idx_order_items_seller_id
    ON core.order_items (seller_id);

-- 5.3 Payments and reviews
CREATE INDEX IF NOT EXISTS idx_payments_order_id
    ON core.payments (order_id);

CREATE INDEX IF NOT EXISTS idx_reviews_order_id
    ON core.reviews (order_id);

CREATE INDEX IF NOT EXISTS idx_reviews_score
    ON core.reviews (review_score);

-- 5.4 Marketing funnel
CREATE INDEX IF NOT EXISTS idx_closed_deals_seller_id
    ON core.closed_deals (seller_id);

CREATE INDEX IF NOT EXISTS idx_closed_deals_won_date
    ON core.closed_deals (won_date);

CREATE INDEX IF NOT EXISTS idx_mql_first_contact_date
    ON core.marketing_qualified_leads (first_contact_date);

-- =========================================
-- 6. Optional helper views
-- =========================================

CREATE OR REPLACE VIEW core.v_order_summary AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    SUM(oi.price)                          AS items_value,
    SUM(oi.freight_value)                  AS freight_value,
    COALESCE(SUM(p.payment_value), 0)      AS total_payment_value,
    COUNT(DISTINCT oi.product_id)          AS distinct_products,
    COUNT(*)                               AS total_items
FROM core.orders o
LEFT JOIN core.order_items oi
    ON o.order_id = oi.order_id
LEFT JOIN core.payments p
    ON o.order_id = p.order_id
GROUP BY
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp;

CREATE OR REPLACE VIEW core.v_closed_deals_enriched AS
SELECT
    cd.*,
    mql.first_contact_date,
    mql.origin,
    mql.landing_page_id
FROM core.closed_deals cd
LEFT JOIN core.marketing_qualified_leads mql
    ON cd.mql_id = mql.mql_id;
