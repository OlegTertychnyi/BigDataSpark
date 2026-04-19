-- Базовая проверка: должно быть 10000 строк
SELECT COUNT(*) AS total_rows FROM raw.mock_data;
-- Ожидаем: 10000

-- Уникальность исходного id (должно быть 1000 уникальных)
SELECT COUNT(DISTINCT id) AS unique_source_ids FROM raw.mock_data;
-- Ожидаем: 1000

-- Разбивка по файлам: 10 файлов по 1000 строк
SELECT loaded_from_file, COUNT(*) AS rows
FROM raw.mock_data
GROUP BY loaded_from_file
ORDER BY loaded_from_file;

-- Первые 5 строк — визуальная проверка
SELECT row_id, id, customer_first_name, product_name, sale_date, sale_total_price
FROM raw.mock_data
ORDER BY row_id
LIMIT 5;

-- NULL-статистика по ключевым полям (где ожидаем NULL — там они есть)
SELECT
    COUNT(*) FILTER (WHERE customer_postal_code IS NULL) AS customer_pc_nulls,
    COUNT(*) FILTER (WHERE seller_postal_code   IS NULL) AS seller_pc_nulls,
    COUNT(*) FILTER (WHERE store_state           IS NULL) AS store_state_nulls,
    COUNT(*) FILTER (WHERE product_description   IS NULL) AS descr_nulls
FROM raw.mock_data;
-- Ожидаем: ~1700 customer_pc_nulls, ~1700 seller_pc_nulls, ~2700 store_state_nulls, 0 descr_nulls

-- Проверка распарсенных дат
SELECT
    MIN(sale_date)            AS earliest_sale,
    MAX(sale_date)            AS latest_sale,
    MIN(product_release_date) AS earliest_release,
    MAX(product_expiry_date)  AS latest_expiry
FROM raw.mock_data;

-- Проверка низкокардинальных полей (кандидаты в справочники)
SELECT 'product_name'      AS col, array_agg(DISTINCT product_name)      AS values FROM raw.mock_data
UNION ALL
SELECT 'product_category',       array_agg(DISTINCT product_category)         FROM raw.mock_data
UNION ALL
SELECT 'product_size',           array_agg(DISTINCT product_size)             FROM raw.mock_data
UNION ALL
SELECT 'pet_category',           array_agg(DISTINCT pet_category)             FROM raw.mock_data
UNION ALL
SELECT 'customer_pet_type',      array_agg(DISTINCT customer_pet_type)        FROM raw.mock_data;

-- Проверка что Spark-джоба сможет джойнить: совпадают ли sale_*_id с реальными id?
-- (ожидаем, что sale_customer_id === id той же строки, это плоские данные)
SELECT
    COUNT(*) FILTER (WHERE id = sale_customer_id) AS match_customer,
    COUNT(*) FILTER (WHERE id = sale_seller_id)   AS match_seller,
    COUNT(*) FILTER (WHERE id = sale_product_id)  AS match_product
FROM raw.mock_data;
