"""
ETL: raw.mock_data → star schema.
Пересоздаёт звезду через DDL, наполняет dim-ы и fact_sales.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Конфиг из env (задан в docker-compose)
PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB   = os.environ.get("POSTGRES_DB",   "bigdataspark")
PG_USER = os.environ.get("POSTGRES_USER", "spark_user")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "spark_pass")

JDBC_URL   = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASS, "driver": "org.postgresql.Driver"}


def recreate_star_schema(spark):
    """Пересоздаёт пустую star schema через DDL-скрипт."""
    with open("/opt/sql/02_star_schema.sql", encoding="utf-8") as f:
        ddl = f.read()

    jvm = spark._jvm
    # Явная регистрация драйвера — DriverManager сам его не найдёт через py4j
    jvm.Class.forName("org.postgresql.Driver")

    props = jvm.java.util.Properties()
    props.setProperty("user", PG_USER)
    props.setProperty("password", PG_PASS)
    conn = jvm.java.sql.DriverManager.getConnection(JDBC_URL, props)
    try:
        conn.createStatement().execute(ddl)
        print("Star schema recreated")
    finally:
        conn.close()


def dedupe_by_key(df, business_keys):
    """Оставляет первое появление каждой сущности по business_keys (детерминировано по row_id)."""
    w = Window.partitionBy(*business_keys).orderBy("row_id")
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def write_dim(df, table):
    """Записывает dim в Postgres и возвращает count."""
    df = df.drop("row_id")  # row_id не нужен в dim'ах
    df.write.jdbc(JDBC_URL, f"star.{table}", mode="append", properties=JDBC_PROPS)
    cnt = df.count()
    print(f"  {table}: {cnt} rows")


def main():
    spark = SparkSession.builder.appName("transform_to_star").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        recreate_star_schema(spark)

        raw = spark.read.jdbc(JDBC_URL, "raw.mock_data", properties=JDBC_PROPS).cache()
        print(f"Raw: {raw.count()} rows")

        customers = dedupe_by_key(
            raw.select("row_id", "customer_email",
                       F.col("customer_first_name").alias("first_name"),
                       F.col("customer_last_name").alias("last_name"),
                       F.col("customer_age").cast("smallint").alias("age"),
                       F.col("customer_country").alias("country"),
                       F.col("customer_postal_code").alias("postal_code"),
                       F.col("customer_pet_type").alias("pet_type"),
                       F.col("customer_pet_name").alias("pet_name"),
                       F.col("customer_pet_breed").alias("pet_breed"))
               .filter(F.col("customer_email").isNotNull()),
            ["customer_email"])
        write_dim(customers, "dim_customers")

        sellers = dedupe_by_key(
            raw.select("row_id", "seller_email",
                       F.col("seller_first_name").alias("first_name"),
                       F.col("seller_last_name").alias("last_name"),
                       F.col("seller_country").alias("country"),
                       F.col("seller_postal_code").alias("postal_code"))
               .filter(F.col("seller_email").isNotNull()),
            ["seller_email"])
        write_dim(sellers, "dim_sellers")

        products = dedupe_by_key(
            raw.select("row_id", "product_name", "product_brand",
                       F.col("product_category").alias("category"),
                       "pet_category",
                       F.col("product_price").alias("price"),
                       F.col("product_weight").alias("weight"),
                       F.col("product_color").alias("color"),
                       F.col("product_size").alias("size"),
                       F.col("product_material").alias("material"),
                       F.col("product_description").alias("description"),
                       F.col("product_rating").alias("rating"),
                       F.col("product_reviews").alias("reviews"),
                       "product_release_date", "product_expiry_date")
               .withColumnRenamed("product_release_date", "release_date")
               .withColumnRenamed("product_expiry_date",  "expiry_date")
               .filter(F.col("product_name").isNotNull() & F.col("product_brand").isNotNull()),
            ["product_name", "product_brand"])
        write_dim(products, "dim_products")

        stores = dedupe_by_key(
            raw.select("row_id", "store_name",
                       F.col("store_city").alias("city"),
                       F.col("store_location").alias("location"),
                       F.col("store_state").alias("state"),
                       F.col("store_country").alias("country"),
                       F.col("store_phone").alias("phone"),
                       F.col("store_email").alias("email"))
               .filter(F.col("store_name").isNotNull() & F.col("store_city").isNotNull()),
            ["store_name", "city"])
        write_dim(stores, "dim_stores")

        suppliers = dedupe_by_key(
            raw.select("row_id", "supplier_email", "supplier_name",
                       F.col("supplier_contact").alias("contact"),
                       F.col("supplier_phone").alias("phone"),
                       F.col("supplier_address").alias("address"),
                       F.col("supplier_city").alias("city"),
                       F.col("supplier_country").alias("country"))
               .filter(F.col("supplier_email").isNotNull()),
            ["supplier_email"])
        write_dim(suppliers, "dim_suppliers")

        dates = (raw.select("sale_date").distinct()
                    .filter(F.col("sale_date").isNotNull())
                    .withColumnRenamed("sale_date", "full_date")
                    .withColumn("year",        F.year("full_date").cast("smallint"))
                    .withColumn("quarter",     F.quarter("full_date").cast("smallint"))
                    .withColumn("month",       F.month("full_date").cast("smallint"))
                    .withColumn("month_name",  F.date_format("full_date", "MMMM"))
                    .withColumn("day",         F.dayofmonth("full_date").cast("smallint"))
                    .withColumn("day_of_week",
                        F.when(F.dayofweek("full_date") == 1, 7)
                         .otherwise(F.dayofweek("full_date") - 1).cast("smallint"))
                    .withColumn("day_name",    F.date_format("full_date", "EEEE"))
                    .withColumn("is_weekend",  F.dayofweek("full_date").isin(1, 7)))
        dates.write.jdbc(JDBC_URL, "star.dim_dates", mode="append", properties=JDBC_PROPS)
        print(f"  dim_dates: {dates.count()} rows")

        def read_dim(name, cols):
            return spark.read.jdbc(JDBC_URL, f"star.{name}", properties=JDBC_PROPS).select(*cols)

        dc = read_dim("dim_customers", ["customer_sk", "customer_email"])
        ds = read_dim("dim_sellers",   ["seller_sk", "seller_email"])
        dp = read_dim("dim_products",  ["product_sk", "product_name", "product_brand"])
        dst = read_dim("dim_stores",   ["store_sk", "store_name", "city"])
        dsp = read_dim("dim_suppliers", ["supplier_sk", "supplier_email"])
        dd = read_dim("dim_dates",     ["date_sk", "full_date"])

        fact = (raw.alias("r")
            .join(dc, "customer_email")
            .join(ds, "seller_email")
            .join(dp, ["product_name", "product_brand"])
            .join(dst, (F.col("r.store_name") == dst.store_name) & (F.col("r.store_city") == dst.city))
            .join(dsp, "supplier_email")
            .join(dd, F.col("r.sale_date") == dd.full_date)
            .select("customer_sk", "seller_sk", "product_sk", "store_sk", "supplier_sk", "date_sk",
                    F.col("r.sale_date").alias("sale_date"),
                    F.col("r.sale_quantity").cast("smallint").alias("sale_quantity"),
                    F.col("r.sale_total_price").alias("sale_total_price"),
                    F.col("r.product_price").alias("unit_price"),
                    F.col("r.row_id").alias("source_row_id")))
        fact.write.jdbc(JDBC_URL, "star.fact_sales", mode="append", properties=JDBC_PROPS)
        print(f"  fact_sales: {fact.count()} rows")

        print("Done")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()