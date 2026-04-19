-- ВИТРИНА 1: продажи по продуктам

-- Топ-10 самых продаваемых продуктов (по количеству)
SELECT product_name, product_brand, total_quantity, total_revenue
FROM analytics.mart_product_sales
ORDER BY total_quantity DESC
LIMIT 10;

-- Общая выручка по категориям
SELECT category, SUM(total_revenue) AS revenue, SUM(total_quantity) AS units
FROM analytics.mart_product_sales
GROUP BY category
ORDER BY revenue DESC;

-- Средний рейтинг и отзывы по продуктам (топ-5)
SELECT product_name, product_brand, avg_rating, total_reviews
FROM analytics.mart_product_sales
ORDER BY avg_rating DESC, total_reviews DESC
LIMIT 5;


-- ВИТРИНА 2: продажи по клиентам

-- Топ-10 клиентов по сумме покупок
SELECT first_name, last_name, country, total_spent, orders_count
FROM analytics.mart_customer_sales
ORDER BY total_spent DESC
LIMIT 10;

-- Распределение клиентов по странам
SELECT country, COUNT(*) AS customers, SUM(total_spent) AS revenue
FROM analytics.mart_customer_sales
GROUP BY country
ORDER BY customers DESC
LIMIT 10;

-- Средний чек
SELECT first_name, last_name, avg_order_value
FROM analytics.mart_customer_sales
ORDER BY avg_order_value DESC
LIMIT 10;


-- ВИТРИНА 3: продажи по времени

-- Месячные тренды
SELECT year, month, month_name, orders_count, total_revenue, avg_order_value
FROM analytics.mart_time_sales
ORDER BY year, month;

-- Годовые итоги
SELECT year, SUM(orders_count) AS orders, SUM(total_revenue) AS revenue
FROM analytics.mart_time_sales
GROUP BY year
ORDER BY year;


-- ВИТРИНА 4: продажи по магазинам

-- Топ-5 магазинов по выручке
SELECT store_name, city, country, total_revenue, orders_count
FROM analytics.mart_store_sales
ORDER BY total_revenue DESC
LIMIT 5;

-- Распределение по странам
SELECT country, COUNT(*) AS stores, SUM(total_revenue) AS revenue
FROM analytics.mart_store_sales
GROUP BY country
ORDER BY revenue DESC
LIMIT 10;


-- ВИТРИНА 5: продажи по поставщикам

-- Топ-5 поставщиков по выручке
SELECT supplier_name, supplier_country, total_revenue, products_sold, avg_product_price
FROM analytics.mart_supplier_sales
ORDER BY total_revenue DESC
LIMIT 5;

-- Распределение по странам поставщиков
SELECT supplier_country, COUNT(*) AS suppliers, SUM(total_revenue) AS revenue
FROM analytics.mart_supplier_sales
GROUP BY supplier_country
ORDER BY revenue DESC
LIMIT 10;


-- ВИТРИНА 6: качество продукции

-- Продукты с наивысшим рейтингом
SELECT product_name, product_brand, rating, reviews_count, units_sold
FROM analytics.mart_product_quality
ORDER BY rating DESC, reviews_count DESC
LIMIT 10;

-- Продукты с наименьшим рейтингом
SELECT product_name, product_brand, rating, reviews_count, units_sold
FROM analytics.mart_product_quality
ORDER BY rating ASC, reviews_count DESC
LIMIT 10;

-- Топ по количеству отзывов
SELECT product_name, product_brand, reviews_count, rating, total_revenue
FROM analytics.mart_product_quality
ORDER BY reviews_count DESC
LIMIT 10;

-- Корреляция рейтинг ↔ продажи (средние продажи по группам рейтинга)
SELECT 
    FLOOR(rating) AS rating_group,
    COUNT(*) AS products,
    AVG(units_sold) AS avg_units_sold,
    AVG(total_revenue) AS avg_revenue
FROM analytics.mart_product_quality
GROUP BY rating_group
ORDER BY rating_group DESC;