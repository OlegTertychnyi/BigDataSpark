CREATE DATABASE IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.mart_product_sales;
CREATE TABLE analytics.mart_product_sales (
    product_name String,
    product_brand String,
    category String,
    pet_category String,
    total_quantity  UInt64,
    total_revenue Decimal(18, 2),
    avg_rating Decimal(4, 2),
    total_reviews UInt64,
    orders_count UInt64
) ENGINE = MergeTree()
ORDER BY (total_revenue, product_name);

DROP TABLE IF EXISTS analytics.mart_customer_sales;
CREATE TABLE analytics.mart_customer_sales (
    customer_email  String,
    first_name      String,
    last_name       String,
    country         String,
    orders_count    UInt64,
    total_spent     Decimal(18, 2),
    avg_order_value Decimal(14, 2)
) ENGINE = MergeTree()
ORDER BY (total_spent, customer_email);

DROP TABLE IF EXISTS analytics.mart_time_sales;
CREATE TABLE analytics.mart_time_sales (
    year UInt16,
    quarter UInt8,
    month UInt8,
    month_name String,
    orders_count UInt64,
    total_revenue Decimal(18, 2),
    total_quantity UInt64,
    avg_order_value Decimal(14, 2)
) ENGINE = MergeTree()
ORDER BY (year, month);

DROP TABLE IF EXISTS analytics.mart_store_sales;
CREATE TABLE analytics.mart_store_sales (
    store_name String,
    city String,
    country String,
    orders_count UInt64,
    total_revenue Decimal(18, 2),
    avg_order_value Decimal(14, 2)
) ENGINE = MergeTree()
ORDER BY (total_revenue, store_name);

DROP TABLE IF EXISTS analytics.mart_supplier_sales;
CREATE TABLE analytics.mart_supplier_sales (
    supplier_name String,
    supplier_country String,
    orders_count UInt64,
    total_revenue Decimal(18, 2),
    avg_product_price Decimal(14, 2),
    products_sold UInt64
) ENGINE = MergeTree()
ORDER BY (total_revenue, supplier_name);

DROP TABLE IF EXISTS analytics.mart_product_quality;
CREATE TABLE analytics.mart_product_quality (
    product_name String,
    product_brand String,
    category String,
    rating Decimal(4, 2),
    reviews_count UInt64,
    units_sold UInt64,
    total_revenue Decimal(18, 2)
) ENGINE = MergeTree()
ORDER BY (rating, product_name);

SELECT 'ClickHouse datamarts created' AS status;