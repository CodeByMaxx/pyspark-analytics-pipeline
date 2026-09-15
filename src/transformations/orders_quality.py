from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def check_orders_quality(df: DataFrame) -> None:
    print("=== Orders Data Quality ===")

    print(f"Total rows: {df.count()}")

    print("\nNull values:")
    df.select(
        [
            F.count(F.when(F.col(column).isNull(), column)).alias(column)
            for column in df.columns
        ]
    ).show()

    print("\nOrder status distribution:")
    (df.groupBy("order_status").count().orderBy(F.desc("count")).show())

    print("\nDuplicate order IDs:")
    (df.groupBy("order_id").count().filter(F.col("count") > 1).show())

    print("\nDelivery days statistics:")
    df.select(
        F.min("delivery_days").alias("min_delivery_days"),
        F.max("delivery_days").alias("max_delivery_days"),
        F.avg("delivery_days").alias("avg_delivery_days"),
    ).show()
