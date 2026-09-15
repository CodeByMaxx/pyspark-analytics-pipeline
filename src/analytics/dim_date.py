from pyspark.sql import functions as F

from src.utils.spark_session import create_spark_session


ORDERS_PATH = "data/silver/orders"
GOLD_PATH = "data/gold/dim_date"


def main():
    spark = create_spark_session()

    orders = spark.read.parquet(ORDERS_PATH)

    date_range = orders.select(
        F.min("order_purchase_date").alias("min_date"),
        F.max("order_purchase_date").alias("max_date"),
    ).collect()[0]

    min_date = date_range["min_date"]
    max_date = date_range["max_date"]

    print(f"Date range: {min_date} -> {max_date}")

    dim_date = (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(
                    F.lit(min_date),
                    F.lit(max_date),
                    F.expr("INTERVAL 1 DAY"),
                )
            ).alias("date")
        )
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("integer"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("week", F.weekofyear("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
        .select(
            "date_key",
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "week",
            "day",
            "day_of_week",
            "day_name",
            "is_weekend",
        )
    )

    print("Date Dimension Schema:")
    dim_date.printSchema()

    print("Date Rows:", dim_date.count())

    dim_date.show(10, truncate=False)

    (dim_date.write.mode("overwrite").parquet(GOLD_PATH))

    print(f"Gold data written to: {GOLD_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
