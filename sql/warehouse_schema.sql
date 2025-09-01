CREATE SCHEMA IF NOT EXISTS olist_dw;

CREATE TABLE IF NOT EXISTS olist_dw.orders(
	order_ide TEXT PRIMARY KEY,
    customer_id TEXT,
    order_purchase_at TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivered_date TIMESTAMP,
    total_order_value NUMERIC
);

CREATE TABLE IF NOT EXISTS olist_dw.order_items(
	order_item_id INTEGER,
    order_id TEXT,
    product_id TEXT,
    seller_id TEXT,
    price NUMERIC,
    freight_value NUMERIC,
    PRIMARY KEY (order_item_id, order_id)
);

CREATE TABLE IF NOT EXISTS olist_dw.products(
	product_id TEXT PRIMARY KEY,
	product_category_name TEXT,
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

CREATE TABLE IF NOT EXISTS olist_dw.product_sales_summary (
    product_id TEXT PRIMARY KEY,
    units_sold BIGINT,
    total_revenue NUMERIC
);

CREATE TABLE IF NOT EXISTS olist_dw.daily_orders_summary (
    order_date DATE PRIMARY KEY,
    orders_count BIGINT,
    total_revenue NUMERIC
);