CREATE OR REPLACE VIEW core.v_customer_metrics AS
WITH order_agg AS (
    -- Aggregate orders at the order level with item and payment totals
    SELECT
        o.order_id,
        o.customer_id,
        o.order_purchase_timestamp::date AS order_date,

        -- Monetary metrics from items
        SUM(oi.price) AS items_value,
        SUM(oi.freight_value) AS freight_value,

        -- Total payment value across all payments for the order
        COALESCE(SUM(p.payment_value), 0) AS payment_value,

        -- Count of items in the order
        COUNT(*) AS items_count
    FROM core.orders o
    LEFT JOIN core.order_items oi
        ON oi.order_id = o.order_id
    LEFT JOIN core.payments p
        ON p.order_id = o.order_id
    GROUP BY
        o.order_id,
        o.customer_id,
        o.order_purchase_timestamp
)
SELECT
    -- Customer identity and location
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,

    -- Order frequency
    COUNT(DISTINCT oa.order_id) AS total_orders,
    MIN(oa.order_date) AS first_order_date,
    MAX(oa.order_date) AS most_recent_order_date,

    -- Volume metrics
    COALESCE(SUM(oa.items_count), 0) AS total_items,

    -- Monetary totals
    COALESCE(SUM(oa.items_value), 0) AS total_items_value,
    COALESCE(SUM(oa.freight_value), 0) AS total_freight_value,
    COALESCE(SUM(oa.payment_value), 0) AS total_payment_value,

    -- Derived averages
    CASE WHEN COUNT(DISTINCT oa.order_id) > 0
         THEN ROUND(SUM(oa.payment_value) / COUNT(DISTINCT oa.order_id), 2)
         ELSE NULL
    END AS avg_order_value,

    CASE WHEN COUNT(DISTINCT oa.order_id) > 0
         THEN ROUND(SUM(oa.items_count)::numeric / COUNT(DISTINCT oa.order_id), 2)
         ELSE NULL
    END AS avg_items_per_order
FROM core.customers c
LEFT JOIN order_agg oa
    ON c.customer_id = oa.customer_id
GROUP BY
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state;

CREATE OR REPLACE VIEW core.v_product_metrics AS
SELECT
    -- Product details
    p.product_id,
    p.product_category_name,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,

    -- Core sales metrics
    COUNT(oi.order_id) AS total_items_sold,
    COUNT(DISTINCT oi.order_id) AS total_orders,

    -- Monetary value metrics
    COALESCE(SUM(oi.price), 0) AS total_items_value,
    COALESCE(SUM(oi.freight_value), 0) AS total_freight_value,

    -- Avg selling price
    CASE WHEN COUNT(oi.order_id) > 0
         THEN ROUND(AVG(oi.price), 2)
         ELSE NULL
    END AS avg_item_price,

    -- Sale timeline
    MIN(o.order_purchase_timestamp::date) AS first_sale_date,
    MAX(o.order_purchase_timestamp::date) AS most_recent_sale_date
FROM core.products p
LEFT JOIN core.order_items oi
    ON oi.product_id = p.product_id
LEFT JOIN core.orders o
    ON o.order_id = oi.order_id
GROUP BY
    p.product_id,
    p.product_category_name,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm;

CREATE OR REPLACE VIEW core.v_seller_metrics AS
WITH seller_orders AS (
    -- Aggregate financials per seller per order
    SELECT
        oi.seller_id,
        oi.order_id,
        SUM(oi.price) AS items_value,
        SUM(oi.freight_value) AS freight_value
    FROM core.order_items oi
    GROUP BY
        oi.seller_id,
        oi.order_id
),
seller_reviews AS (
    -- Average review score per seller per order
    SELECT
        oi.seller_id,
        r.order_id,
        AVG(r.review_score) AS avg_review_score
    FROM core.order_items oi
    JOIN core.reviews r
        ON r.order_id = oi.order_id
    GROUP BY
        oi.seller_id,
        r.order_id
)
SELECT
    -- Seller identity and location
    s.seller_id,
    s.seller_city,
    s.seller_state,

    -- Orders fulfilled by seller
    COUNT(DISTINCT so.order_id) AS total_orders,

    -- Financial totals
    COALESCE(SUM(so.items_value), 0) AS total_items_value,
    COALESCE(SUM(so.freight_value), 0) AS total_freight_value,

    -- Avg revenue per order for the seller
    CASE WHEN COUNT(DISTINCT so.order_id) > 0
         THEN ROUND(SUM(so.items_value) / COUNT(DISTINCT so.order_id), 2)
         ELSE NULL
    END AS avg_order_item_value,

    -- Seller quality indicator
    AVG(sr.avg_review_score) AS avg_review_score
FROM core.sellers s
LEFT JOIN seller_orders so
    ON so.seller_id = s.seller_id
LEFT JOIN seller_reviews sr
    ON sr.seller_id = s.seller_id
GROUP BY
    s.seller_id,
    s.seller_city,
    s.seller_state;

CREATE OR REPLACE VIEW core.v_order_delivery_metrics AS
WITH review_agg AS (
    -- Aggregate review metrics per order
    SELECT
        r.order_id,
        AVG(r.review_score) AS avg_review_score,
        MIN(r.review_creation_date) AS first_review_date
    FROM core.reviews r
    GROUP BY
        r.order_id
)
SELECT
    -- Order details
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- Delivery duration
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
        THEN (o.order_delivered_customer_date::date
              - o.order_purchase_timestamp::date)
        ELSE NULL
    END AS delivery_days,

    -- Late delivery flag
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
             AND o.order_estimated_delivery_date IS NOT NULL
             AND o.order_delivered_customer_date::date
                 > o.order_estimated_delivery_date::date
        THEN TRUE
        ELSE FALSE
    END AS is_late_delivery,

    -- Review-related metrics
    ra.avg_review_score,
    ra.first_review_date
FROM core.orders o
LEFT JOIN review_agg ra
    ON ra.order_id = o.order_id;

CREATE OR REPLACE VIEW core.v_daily_sales AS
WITH order_totals AS (
    -- Aggregate monetary totals at the order level
    SELECT
        o.order_id,
        o.order_purchase_timestamp::date AS order_date,
        SUM(oi.price) AS items_value,
        SUM(oi.freight_value) AS freight_value,
        COALESCE(SUM(p.payment_value), 0) AS payment_value
    FROM core.orders o
    LEFT JOIN core.order_items oi
        ON oi.order_id = o.order_id
    LEFT JOIN core.payments p
        ON p.order_id = o.order_id
    WHERE o.order_status <> 'canceled'   -- exclude canceled orders
    GROUP BY
        o.order_id,
        o.order_purchase_timestamp::date
)
SELECT
    -- Daily metrics
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    COALESCE(SUM(items_value), 0) AS total_items_value,
    COALESCE(SUM(freight_value), 0) AS total_freight_value,
    COALESCE(SUM(payment_value), 0) AS total_payment_value,

    -- Derived AOV
    CASE WHEN COUNT(DISTINCT order_id) > 0
         THEN ROUND(SUM(payment_value) / COUNT(DISTINCT order_id), 2)
         ELSE NULL
    END AS avg_order_value
FROM order_totals
GROUP BY
    order_date
ORDER BY
    order_date;

