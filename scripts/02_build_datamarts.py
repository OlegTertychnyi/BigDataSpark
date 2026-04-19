"""
ETL: star schema (Postgres) → 6 витрин в ClickHouse.
Читает dim'ы и fact из star, агрегирует, пишет в analytics.mart_*.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Postgres (источник)
PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB   = os.environ.get("POSTGRES_DB",   "bigdataspark")
PG_USER = os.environ.get("POSTGRES_USER", "spark_user")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "spark_pass")

PG_URL   = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPS = {"user": PG_USER, "password": PG_PASS, "driver": "org.postgresql.Driver"}

# ClickHouse (целевой)
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = os.environ.get("CLICKHOUSE_PORT", "8123")
CH_DB   = os.environ.get("CLICKHOUSE_DB",   "analytics")
CH_USER = os.environ.get("CLICKHOUSE_USER", "ch_user")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "ch_pass")

CH_URL   = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}"
CH_PROPS = {"user": CH_USER, "password": CH_PASS, "driver": "com.clickhouse.jdbc.ClickHouseDriver"}


def read_pg(spark, table):
    return spark.read.jdbc(PG_URL, table, properties=PG_PROPS)


def write_ch(df, table):
    """Пишет DataFrame в ClickHouse. Таблицы созданы заранее через DDL."""
    # truncate=True — очищает таблицу перед записью (идемпотентность)
    (df.write
       .mode("append")  # append т.к. заранее создали DDL; TRUNCATE делаем отдельно ниже
       .jdbc(CH_URL, f"{CH_DB}.{table}", properties=CH_PROPS))
    cnt = df.count()
    print(f"  {table}: {cnt} rows")


def truncate_ch(spark, table):
    """TRUNCATE перед вставкой, чтобы повторный запуск не дублировал данные."""
    jvm = spark._jvm
    jvm.Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    props = jvm.java.util.Properties()
    props.setProperty("user", CH_USER)
    props.setProperty("password", CH_PASS)
    conn = jvm.java.sql.DriverManager.getConnection(CH_URL, props)
    try:
        conn.createStatement().execute(f"TRUNCATE TABLE {CH_DB}.{table}")
    finally:
        conn.close()


def main():
    spark = (SparkSession.builder
             .appName("build_datamarts")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    try:
        # === Читаем star schema ===
        fact      = read_pg(spark, "star.fact_sales").cache()
        customers = read_pg(spark, "star.dim_customers")
        products  = read_pg(spark, "star.dim_products")
        stores    = read_pg(spark, "star.dim_stores")
        suppliers = read_pg(spark, "star.dim_suppliers")
        dates     = read_pg(spark, "star.dim_dates")

        print(f"Star schema loaded: fact={fact.count()}")

        # === Витрина 1: продажи по продуктам ===
        truncate_ch(spark, "mart_product_sales")
        mart1 = (fact
                 .join(products, "product_sk")
                 .groupBy("product_name", "product_brand", "category", "pet_category")
                 .agg(
                    F.sum("sale_quantity").cast("long").alias("total_quantity"),
                    F.sum("sale_total_price").cast("decimal(18,2)").alias("total_revenue"),
                    F.avg("rating").cast("decimal(4,2)").alias("avg_rating"),
                    F.sum("reviews").cast("long").alias("total_reviews"),
                    F.count("*").cast("long").alias("orders_count"),
                 ))
        write_ch(mart1, "mart_product_sales")

        # === Витрина 2: продажи по клиентам ===
        truncate_ch(spark, "mart_customer_sales")
        mart2 = (fact
                 .join(customers, "customer_sk")
                 .groupBy("customer_email", "first_name", "last_name", "country")
                 .agg(
                    F.count("*").cast("long").alias("orders_count"),
                    F.sum("sale_total_price").cast("decimal(18,2)").alias("total_spent"),
                    F.avg("sale_total_price").cast("decimal(14,2)").alias("avg_order_value"),
                 ))
        write_ch(mart2, "mart_customer_sales")

        # === Витрина 3: продажи по времени ===
        truncate_ch(spark, "mart_time_sales")
        mart3 = (fact
                 .join(dates, "date_sk")
                 .groupBy("year", "quarter", "month", "month_name")
                 .agg(
                    F.count("*").cast("long").alias("orders_count"),
                    F.sum("sale_total_price").cast("decimal(18,2)").alias("total_revenue"),
                    F.sum("sale_quantity").cast("long").alias("total_quantity"),
                    F.avg("sale_total_price").cast("decimal(14,2)").alias("avg_order_value"),
                 )
                 .withColumn("year", F.col("year").cast("int"))
                 .withColumn("quarter", F.col("quarter").cast("int"))
                 .withColumn("month", F.col("month").cast("int")))
        write_ch(mart3, "mart_time_sales")

        # === Витрина 4: продажи по магазинам ===
        truncate_ch(spark, "mart_store_sales")
        mart4 = (fact
                 .join(stores, "store_sk")
                 .groupBy("store_name", "city", "country")
                 .agg(
                    F.count("*").cast("long").alias("orders_count"),
                    F.sum("sale_total_price").cast("decimal(18,2)").alias("total_revenue"),
                    F.avg("sale_total_price").cast("decimal(14,2)").alias("avg_order_value"),
                 ))
        write_ch(mart4, "mart_store_sales")

        # === Витрина 5: продажи по поставщикам ===
        truncate_ch(spark, "mart_supplier_sales")
        mart5 = (fact
                 .join(suppliers, "supplier_sk")
                 .join(products,  "product_sk")
                 .groupBy(
                     suppliers["supplier_name"].alias("supplier_name"),
                     suppliers["country"].alias("supplier_country"),
                 )
                 .agg(
                    F.count("*").cast("long").alias("orders_count"),
                    F.sum("sale_total_price").cast("decimal(18,2)").alias("total_revenue"),
                    F.avg("unit_price").cast("decimal(14,2)").alias("avg_product_price"),
                    F.sum("sale_quantity").cast("long").alias("products_sold"),
                 ))
        write_ch(mart5, "mart_supplier_sales")

        # === Витрина 6: качество продукции ===
        truncate_ch(spark, "mart_product_quality")
        mart6 = (fact
                 .join(products, "product_sk")
                 .groupBy("product_name", "product_brand", "category", "rating", "reviews")
                 .agg(
                    F.sum("sale_quantity").cast("long").alias("units_sold"),
                    F.sum("sale_total_price").cast("decimal(18,2)").alias("total_revenue"),
                 )
                 .select(
                    "product_name", "product_brand", "category",
                    F.col("rating").cast("decimal(4,2)").alias("rating"),
                    F.col("reviews").cast("long").alias("reviews_count"),
                    "units_sold", "total_revenue"
                 ))
        write_ch(mart6, "mart_product_quality")

        print("All datamarts built")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()