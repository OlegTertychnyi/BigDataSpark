CREATE SCHEMA IF NOT EXISTS raw;
DROP TABLE IF EXISTS raw.mock_data;

CREATE TABLE raw.mock_data (
    row_id               BIGSERIAL PRIMARY KEY,
    id                   INTEGER NOT NULL,

    customer_first_name VARCHAR(50),
    customer_last_name VARCHAR(50),
    customer_age SMALLINT,
    customer_email VARCHAR(100),
    customer_country VARCHAR(60),
    customer_postal_code VARCHAR(50),
    customer_pet_type VARCHAR(50),
    customer_pet_name VARCHAR(50),
    customer_pet_breed VARCHAR(50),

    seller_first_name VARCHAR(50),
    seller_last_name VARCHAR(50),
    seller_email VARCHAR(100),
    seller_country VARCHAR(100),
    seller_postal_code VARCHAR(50),

    product_name VARCHAR(100),
    product_category VARCHAR(50),
    product_price NUMERIC(12, 2),
    product_quantity INTEGER,

    sale_date DATE,
    sale_customer_id INTEGER,
    sale_seller_id INTEGER,
    sale_product_id INTEGER,
    sale_quantity SMALLINT,
    sale_total_price NUMERIC(14, 2),
    store_name VARCHAR(100),
    store_location VARCHAR(100),
    store_city VARCHAR(100),
    store_state VARCHAR(50),
    store_country VARCHAR(100),
    store_phone VARCHAR(50),
    store_email VARCHAR(100),

    pet_category VARCHAR(50),
    product_weight NUMERIC(10, 2),
    product_color VARCHAR(50),
    product_size VARCHAR(50),
    product_brand VARCHAR(50),
    product_material VARCHAR(30),
    product_description TEXT,
    product_rating NUMERIC(3, 1),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE,

    supplier_name VARCHAR(100),
    supplier_contact VARCHAR(100),
    supplier_email VARCHAR(100),
    supplier_phone VARCHAR(20),
    supplier_address VARCHAR(100),
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(60),

    loaded_from_file VARCHAR(100),
    loaded_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_mock_data_sale_date ON raw.mock_data(sale_date);
CREATE INDEX idx_mock_data_customer ON raw.mock_data(customer_email);
CREATE INDEX idx_mock_data_product ON raw.mock_data(product_name, product_brand);
CREATE INDEX idx_mock_data_supplier ON raw.mock_data(supplier_email);
CREATE INDEX idx_mock_data_store ON raw.mock_data(store_name, store_city);

COMMENT ON TABLE  raw.mock_data IS 'Сырые данные из 10 CSV. Landing zone для ETL в star schema.';
COMMENT ON COLUMN raw.mock_data.row_id IS 'Суррогатный PK (исходный id не уникален между файлами)';
COMMENT ON COLUMN raw.mock_data.id IS 'Исходный id из CSV (1..1000, повторяется 10 раз)';
COMMENT ON COLUMN raw.mock_data.loaded_from_file IS 'Имя CSV-файла, откуда загружена строка';

SELECT 'raw.mock_data создана успешно' AS status;