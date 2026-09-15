from pyspark.sql import functions as F

from src.utils.spark_session import create_spark_session
from src.utils.paths import GOLD_FACT_ORDERS


def test_fact_orders_quality():
    spark = create_spark_session()

    fact_orders = spark.read.parquet(str(GOLD_FACT_ORDERS))

    # 1. Fact table must not be empty.
    assert fact_orders.count() > 0

    # 2. One row per order.
    duplicate_count = (
        fact_orders.groupBy("order_id").count().filter(F.col("count") > 1).count()
    )

    assert duplicate_count == 0

    # 3. Dimension keys must be populated.
    missing_keys = fact_orders.select(
        F.count(F.when(F.col("customer_key").isNull(), 1)).alias("customer"),
        F.count(F.when(F.col("date_key").isNull(), 1)).alias("date"),
    ).first()

    assert missing_keys["customer"] == 0
    assert missing_keys["date"] == 0

    # 4. Payment values must not be negative.
    negative_payments = fact_orders.filter(F.col("payment_value") < 0).count()

    assert negative_payments == 0

    # 5. Payment count must not be negative.
    negative_payment_counts = fact_orders.filter(F.col("payment_count") < 0).count()

    assert negative_payment_counts == 0

    # 6. Payment installments must not be negative.
    negative_installments = fact_orders.filter(
        F.col("payment_installments") < 0
    ).count()

    assert negative_installments == 0

    # 7. Delivery days must not be negative.
    invalid_delivery_days = fact_orders.filter(F.col("delivery_days") < 0).count()

    assert invalid_delivery_days == 0

    spark.stop()
