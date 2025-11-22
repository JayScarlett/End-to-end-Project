-- 1. Daily revenue and order volume
SELECT
    d.date_key,
    d.year,
    d.month,
    d.day,
    ds.total_orders,
    ds.total_payment_value,
    ds.avg_order_value
FROM analytics.fact_daily_sales ds
JOIN analytics.dim_date d
    ON ds.date_key = d.date_key
ORDER BY d.date_key;

-- 2. Monthly revenue by year
SELECT
    d.year,
    d.month,
    d.month_name,
    SUM(ds.total_payment_value) AS revenue,
    SUM(ds.total_orders) AS orders,
    ROUND(SUM(ds.total_payment_value) / NULLIF(SUM(ds.total_orders), 0), 2) AS avg_order_value
FROM analytics.fact_daily_sales ds
JOIN analytics.dim_date d
    ON ds.date_key = d.date_key
GROUP BY
    d.year,
    d.month,
    d.month_name
ORDER BY
    d.year,
    d.month;

-- 3. Top customer states by total revenue
SELECT
    c.customer_state,
    COUNT(DISTINCT fo.customer_id) AS customers,
    COUNT(DISTINCT fo.order_id) AS orders,
    SUM(fo.total_payment_value) AS revenue
FROM analytics.fact_orders fo
JOIN analytics.dim_customer c
    ON fo.customer_id = c.customer_id
GROUP BY
    c.customer_state
ORDER BY
    revenue DESC
LIMIT 10;

-- 4. Customer segmentation by order frequency
WITH customer_orders AS (
    SELECT
        fo.customer_id,
        COUNT(*) AS order_count,
        SUM(fo.total_payment_value) AS total_spent
    FROM analytics.fact_orders fo
    GROUP BY fo.customer_id
)
SELECT
    CASE
        WHEN order_count = 1 THEN 'one_time'
        WHEN order_count BETWEEN 2 AND 4 THEN 'repeat_2_to_4'
        ELSE 'repeat_5_plus'
    END AS segment,
    COUNT(*) AS customers,
    SUM(total_spent) AS revenue,
    ROUND(SUM(total_spent) / NULLIF(COUNT(*), 0), 2) AS avg_revenue_per_customer
FROM customer_orders
GROUP BY
    CASE
        WHEN order_count = 1 THEN 'one_time'
        WHEN order_count BETWEEN 2 AND 4 THEN 'repeat_2_to_4'
        ELSE 'repeat_5_plus'
    END
ORDER BY revenue DESC;

-- 5. Product categories ranked by revenue
SELECT
    p.product_category_name,
    p.product_category_name_english,
    COUNT(DISTINCT foi.order_id) AS orders,
    COUNT(*) AS items_sold,
    SUM(foi.price) AS items_revenue,
    SUM(foi.freight_value) AS freight_cost
FROM analytics.fact_order_items foi
JOIN analytics.dim_product p
    ON foi.product_id = p.product_id
GROUP BY
    p.product_category_name,
    p.product_category_name_english
ORDER BY
    items_revenue DESC
LIMIT 20;

-- 6. Top products for a given category (replace :category_name as needed)
SELECT
    foi.product_id,
    COUNT(*) AS items_sold,
    COUNT(DISTINCT foi.order_id) AS orders,
    SUM(foi.price) AS revenue
FROM analytics.fact_order_items foi
JOIN analytics.dim_product p
    ON foi.product_id = p.product_id
WHERE p.product_category_name = :category_name
GROUP BY foi.product_id
ORDER BY revenue DESC
LIMIT 20;

-- 7. Seller performance by seller state
SELECT
    s.seller_state,
    COUNT(DISTINCT fo.order_id) AS orders,
    COUNT(DISTINCT fo.customer_id) AS customers,
    SUM(fo.items_value) AS items_revenue,
    SUM(fo.freight_value) AS freight_revenue
FROM analytics.fact_orders fo
JOIN analytics.fact_order_items foi
    ON fo.order_id = foi.order_id
JOIN analytics.dim_seller s
    ON foi.seller_id = s.seller_id
GROUP BY s.seller_state
ORDER BY items_revenue DESC;

-- 8. Comparison of late vs on time deliveries
SELECT
    CASE
        WHEN fo.is_late_delivery THEN 'late'
        ELSE 'on_time_or_unknown'
    END AS delivery_status,
    COUNT(*) AS orders,
    SUM(fo.total_payment_value) AS revenue,
    ROUND(AVG(fo.delivery_days), 2) AS avg_delivery_days,
    ROUND(AVG(fo.avg_review_score), 2) AS avg_review_score
FROM analytics.fact_orders fo
GROUP BY
    CASE
        WHEN fo.is_late_delivery THEN 'late'
        ELSE 'on_time_or_unknown'
    END;

-- 9. Compare performance by weekday vs weekend
SELECT
    CASE WHEN d.is_weekend THEN 'weekend' ELSE 'weekday' END AS day_type,
    COUNT(DISTINCT fo.order_id) AS orders,
    SUM(fo.total_payment_value) AS revenue,
    ROUND(SUM(fo.total_payment_value) / NULLIF(COUNT(DISTINCT fo.order_id), 0), 2) AS avg_order_value
FROM analytics.fact_orders fo
JOIN analytics.dim_date d
    ON fo.date_key = d.date_key
GROUP BY
    CASE WHEN d.is_weekend THEN 'weekend' ELSE 'weekday' END;

-- 10. Year over year growth by category
WITH category_year AS (
    SELECT
        d.year,
        p.product_category_name,
        SUM(foi.price) AS revenue
    FROM analytics.fact_order_items foi
    JOIN analytics.dim_product p
        ON foi.product_id = p.product_id
    JOIN analytics.dim_date d
        ON foi.date_key = d.date_key
    GROUP BY
        d.year,
        p.product_category_name
)
SELECT
    cy.product_category_name,
    cy.year,
    cy.revenue,
    LAG(cy.revenue) OVER (
        PARTITION BY cy.product_category_name
        ORDER BY cy.year
    ) AS prev_year_revenue,
    CASE
        WHEN LAG(cy.revenue) OVER (
            PARTITION BY cy.product_category_name
            ORDER BY cy.year
        ) IS NULL THEN NULL
        ELSE ROUND(
            100.0 * (cy.revenue
                     - LAG(cy.revenue) OVER (
                         PARTITION BY cy.product_category_name
                         ORDER BY cy.year
                     )
                    )
            / NULLIF(LAG(cy.revenue) OVER (
                        PARTITION BY cy.product_category_name
                        ORDER BY cy.year
                     ), 0),
            2
        )
    END AS yoy_growth_percent
FROM category_year cy
ORDER BY
    cy.product_category_name,
    cy.year;
