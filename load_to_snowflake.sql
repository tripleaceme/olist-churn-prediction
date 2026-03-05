-- =============================================================
-- Olist E-Commerce: Snowflake Data Loading Script
-- Run this in the Snowflake UI (Worksheets) or via SnowSQL
-- =============================================================

-- 1. Create database and schemas
CREATE DATABASE IF NOT EXISTS OLIST;

USE DATABASE OLIST;

CREATE SCHEMA IF NOT EXISTS RAW_DATA;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS MARTS;

USE SCHEMA RAW_DATA;

-- 2. Create file format for Olist CSVs
CREATE OR REPLACE FILE FORMAT olist_csv_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE;

-- 3. Create internal stage
CREATE OR REPLACE STAGE olist_stage
    FILE_FORMAT = olist_csv_format;

-- 4. Create raw tables
CREATE OR REPLACE TABLE customers (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(5)
);

CREATE OR REPLACE TABLE orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE OR REPLACE TABLE order_items (
    order_id VARCHAR(50),
    order_item_id INTEGER,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price FLOAT,
    freight_value FLOAT
);

CREATE OR REPLACE TABLE order_payments (
    order_id VARCHAR(50),
    payment_sequential INTEGER,
    payment_type VARCHAR(30),
    payment_installments INTEGER,
    payment_value FLOAT
);

CREATE OR REPLACE TABLE order_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INTEGER,
    review_comment_title VARCHAR(500),
    review_comment_message VARCHAR(5000),
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

CREATE OR REPLACE TABLE products (
    product_id VARCHAR(50),
    product_category_name VARCHAR(100),
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

CREATE OR REPLACE TABLE sellers (
    seller_id VARCHAR(50),
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(5)
);

CREATE OR REPLACE TABLE geolocation (
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(5)
);

-- =============================================================
-- 5. Upload files to stage
--    Run these from SnowSQL CLI (NOT from the Snowflake UI):
--
--    snowsql -a <your_account> -u <your_user>
--
--    USE DATABASE OLIST;
--    USE SCHEMA RAW_DATA;
--
--    PUT file:///path/to/data/customers_dataset.csv @olist_stage AUTO_COMPRESS=TRUE;
--    PUT file:///path/to/data/orders_dataset.csv @olist_stage AUTO_COMPRESS=TRUE;
--    PUT file:///path/to/data/order_items_dataset.csv @olist_stage AUTO_COMPRESS=TRUE;
--    PUT file:///path/to/data/order_payments_dataset.csv @olist_stage AUTO_COMPRESS=TRUE;
--    PUT file:///path/to/data/order_reviews_dataset.csv @olist_stage AUTO_COMPRESS=TRUE;
--    PUT file:///path/to/data/products_dataset.csv @olist_stage AUTO_COMPRESS=TRUE;
--    PUT file:///path/to/data/sellers_dataset.csv @olist_stage AUTO_COMPRESS=TRUE;
--    PUT file:///path/to/data/geolocation_dataset.csv @olist_stage AUTO_COMPRESS=TRUE;
-- =============================================================

-- 6. Copy data from stage into tables
COPY INTO customers FROM @olist_stage/customers_dataset.csv FILE_FORMAT = olist_csv_format ON_ERROR = 'CONTINUE';
COPY INTO orders FROM @olist_stage/orders_dataset.csv FILE_FORMAT = olist_csv_format ON_ERROR = 'CONTINUE';
COPY INTO order_items FROM @olist_stage/order_items_dataset.csv FILE_FORMAT = olist_csv_format ON_ERROR = 'CONTINUE';
COPY INTO order_payments FROM @olist_stage/order_payments_dataset.csv FILE_FORMAT = olist_csv_format ON_ERROR = 'CONTINUE';
COPY INTO order_reviews FROM @olist_stage/order_reviews_dataset.csv FILE_FORMAT = olist_csv_format ON_ERROR = 'CONTINUE';
COPY INTO products FROM @olist_stage/products_dataset.csv FILE_FORMAT = olist_csv_format ON_ERROR = 'CONTINUE';
COPY INTO sellers FROM @olist_stage/sellers_dataset.csv FILE_FORMAT = olist_csv_format ON_ERROR = 'CONTINUE';
COPY INTO geolocation FROM @olist_stage/geolocation_dataset.csv FILE_FORMAT = olist_csv_format ON_ERROR = 'CONTINUE';

-- 7. Verify row counts
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM customers
UNION ALL SELECT 'orders', COUNT(*) FROM orders
UNION ALL SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL SELECT 'order_payments', COUNT(*) FROM order_payments
UNION ALL SELECT 'order_reviews', COUNT(*) FROM order_reviews
UNION ALL SELECT 'products', COUNT(*) FROM products
UNION ALL SELECT 'sellers', COUNT(*) FROM sellers
UNION ALL SELECT 'geolocation', COUNT(*) FROM geolocation
ORDER BY table_name;
