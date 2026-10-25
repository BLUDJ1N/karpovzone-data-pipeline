"""Обогащаю карточки товаров и сохраняю результат в S3."""

import uuid

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

DATA_PATH = "s3a://YOUR_SOURCE_BUCKET/YOUR_SOURCE_PREFIX/"
TARGET_PATH = "s3a://YOUR_S3_BUCKET/YOUR_S3_PREFIX/seller_items/"

S3_ENDPOINT_URL = "https://YOUR_S3_ENDPOINT"
S3_REGION = "YOUR_S3_REGION"
S3_ACCESS_KEY = "YOUR_AWS_ACCESS_KEY_ID"
S3_SECRET_KEY = "YOUR_AWS_SECRET_ACCESS_KEY"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.2"

# Описываю входные поля; для чтения оставляю схему самих Parquet-файлов.
schema = StructType(
    [
        StructField("sku_id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("seller", StringType(), True),
        StructField("group_type", StringType(), True),
        StructField("country", StringType(), True),
        StructField("availability_items_count", LongType(), True),
        StructField("ordered_items_count", LongType(), True),
        StructField("warehouses_count", LongType(), True),
        StructField("item_price", LongType(), True),
        StructField("goods_sold_count", LongType(), True),
        StructField("item_rate", DoubleType(), True),
        StructField("days_on_sell", LongType(), True),
        StructField("avg_percent_to_sold", LongType(), True),
    ]
)


def _spark_session():
    return (
        SparkSession.builder
        .appName(f"karpovzone-seller-items-{uuid.uuid4().hex}")
        .config("spark.jars.packages", HADOOP_AWS_PACKAGE)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT_URL)
        .config("spark.hadoop.fs.s3a.region", S3_REGION)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .getOrCreate()
    )


def main():
    spark = _spark_session()

    try:
        df = (
            spark.read
            .option("mergeSchema", "true")
            .option("inferSchema", "true")
            .parquet(DATA_PATH)
        )

        result = (
            df
            # Оцениваю возвраты по доле выкупленных товаров.
            .withColumn(
                "returned_items_count",
                (
                    F.col("ordered_items_count")
                    * (1.0 - F.col("avg_percent_to_sold") / 100.0)
                ).cast("integer"),
            )
            .withColumn(
                "potential_revenue",
                (
                    F.col("availability_items_count")
                    + F.col("ordered_items_count")
                )
                * F.col("item_price"),
            )
            # Вычитаю возвраты, чтобы учесть их в выручке.
            .withColumn(
                "total_revenue",
                (
                    F.col("goods_sold_count")
                    - F.col("returned_items_count")
                )
                * F.col("item_price"),
            )
            # Считаю продажи в день, избегая деления на ноль.
            .withColumn(
                "avg_daily_sales",
                F.when(
                    F.col("days_on_sell") > 0,
                    F.col("goods_sold_count") / F.col("days_on_sell"),
                ).otherwise(0),
            )
            # Если продаж нет, оставляю срок распродажи пустым.
            .withColumn(
                "days_to_sold",
                F.when(
                    F.col("avg_daily_sales") > 0,
                    F.col("availability_items_count")
                    / F.col("avg_daily_sales"),
                ),
            )
            # Сравниваю рейтинг товара с остальными карточками.
            .withColumn(
                "item_rate_percent",
                F.percent_rank().over(Window.orderBy("item_rate")),
            )
        )

        result.show(truncate=False)
        result.printSchema()

        # Перезаписываю результат, чтобы при повторном запуске не было дублей.
        result.write.mode("overwrite").parquet(TARGET_PATH)
    finally:
        # Освобождаю ресурсы Spark, даже если обработка завершилась с ошибкой.
        spark.stop()


if __name__ == "__main__":
    main()
