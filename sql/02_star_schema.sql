CREATE SCHEMA IF NOT EXISTS star;

DROP TABLE IF EXISTS star.fact_sales;
DROP TABLE IF EXISTS star.dim_customers;
DROP TABLE IF EXISTS star.dim_sellers;
DROP TABLE IF EXISTS star.dim_products;
DROP TABLE IF EXISTS star.dim_stores;
DROP TABLE IF EXISTS star.dim_suppliers;
DROP TABLE IF EXISTS star.dim_dates;

CREATE TABLE star.dim_customers (
    customer_sk BIGSERIAL PRIMARY KEY,
    customer_email VARCHAR(100) NOT NULL UNIQUE,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age SMALLINT,
    country VARCHAR(60),
    postal_code VARCHAR(20),
    pet_type VARCHAR(20),
    pet_name VARCHAR(50),
    pet_breed VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);
COMMENT ON TABLE star.dim_customers IS 'Измерение: покупатели. BK = email';

CREATE TABLE star.dim_sellers (
    seller_sk BIGSERIAL PRIMARY KEY,
    seller_email VARCHAR(100) NOT NULL UNIQUE,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    country VARCHAR(60),
    postal_code VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);
COMMENT ON TABLE star.dim_sellers IS 'Измерение: продавцы. BK = email';

CREATE TABLE star.dim_products (
    product_sk BIGSERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    product_brand VARCHAR(50) NOT NULL,
    category VARCHAR(50),
    pet_category VARCHAR(30),
    price NUMERIC(12, 2),
    weight NUMERIC(10, 2),
    color VARCHAR(30),
    size VARCHAR(20),
    material VARCHAR(30),
    description TEXT,
    rating NUMERIC(3, 1),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (product_name, product_brand)
);
COMMENT ON TABLE star.dim_products IS 'Измерение: товары. BK = (name, brand)';

CREATE TABLE star.dim_stores (
    store_sk BIGSERIAL PRIMARY KEY,
    store_name VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    location VARCHAR(100),
    state VARCHAR(20),
    country VARCHAR(60),
    phone VARCHAR(20),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (store_name, city)
);
COMMENT ON TABLE star.dim_stores IS 'Измерение: магазины. BK = (name, city)';

CREATE TABLE star.dim_suppliers (
    supplier_sk BIGSERIAL PRIMARY KEY,
    supplier_email VARCHAR(100) NOT NULL UNIQUE,
    supplier_name VARCHAR(100),
    contact VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(60),
    created_at TIMESTAMP DEFAULT NOW()
);
COMMENT ON TABLE star.dim_suppliers IS 'Измерение: поставщики. BK = email';

CREATE TABLE star.dim_dates (
    date_sk BIGSERIAL  PRIMARY KEY,
    full_date DATE       NOT NULL UNIQUE,
    year SMALLINT,
    quarter SMALLINT,
    month  SMALLINT,
    month_name VARCHAR(20),
    day SMALLINT,
    day_of_week SMALLINT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);
COMMENT ON TABLE star.dim_dates IS 'Календарное измерение: даты продаж';

CREATE TABLE star.fact_sales (
    sale_sk BIGSERIAL PRIMARY KEY,

    customer_sk BIGINT NOT NULL REFERENCES star.dim_customers(customer_sk),
    seller_sk BIGINT NOT NULL REFERENCES star.dim_sellers(seller_sk),
    product_sk BIGINT NOT NULL REFERENCES star.dim_products(product_sk),
    store_sk BIGINT NOT NULL REFERENCES star.dim_stores(store_sk),
    supplier_sk BIGINT NOT NULL REFERENCES star.dim_suppliers(supplier_sk),
    date_sk BIGINT NOT NULL REFERENCES star.dim_dates(date_sk),

    sale_date DATE NOT NULL,

    sale_quantity SMALLINT NOT NULL,
    sale_total_price NUMERIC(14, 2) NOT NULL,
    unit_price NUMERIC(12, 2),

    source_row_id BIGINT,
    created_at TIMESTAMP DEFAULT NOW()
);
COMMENT ON TABLE star.fact_sales IS 'Факт: продажи. 10000 строк, по одной на каждую запись raw';

CREATE INDEX idx_fact_customer ON star.fact_sales(customer_sk);
CREATE INDEX idx_fact_seller ON star.fact_sales(seller_sk);
CREATE INDEX idx_fact_product ON star.fact_sales(product_sk);
CREATE INDEX idx_fact_store ON star.fact_sales(store_sk);
CREATE INDEX idx_fact_supplier ON star.fact_sales(supplier_sk);
CREATE INDEX idx_fact_date ON star.fact_sales(date_sk);
CREATE INDEX idx_fact_sale_date ON star.fact_sales(sale_date);

SELECT 'Star schema создана (пустая, ждём заполнения Spark-ом)' AS status;