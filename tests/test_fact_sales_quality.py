from pyspark.sql import functions as F

from src.utils.spark_session import create_spark_session
from src.utils.paths import GOLD_FACT_SALES


def test_fact_sales_quality():
    spark = create_spark_session()

    fact_sales = spark.read.parquet(str(GOLD_FACT_SALES))

    # 1. Fact table must not be empty.
    assert fact_sales.count() > 0

    # 2. One row per order item.
    duplicate_count = (
        fact_sales.groupBy("order_id", "order_item_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert duplicate_count == 0

    # 3. Dimension keys must be populated.
    missing_keys = fact_sales.select(
        F.count(F.when(F.col("customer_key").isNull(), 1)).alias("customer"),
        F.count(F.when(F.col("product_key").isNull(), 1)).alias("product"),
        F.count(F.when(F.col("seller_key").isNull(), 1)).alias("seller"),
        F.count(F.when(F.col("date_key").isNull(), 1)).alias("date"),
    ).first()

    assert missing_keys["customer"] == 0
    assert missing_keys["product"] == 0
    assert missing_keys["seller"] == 0
    assert missing_keys["date"] == 0

    # 4. Monetary values must not be negative.
    negative_values = fact_sales.filter(
        (F.col("price") < 0) | (F.col("freight_value") < 0) | (F.col("item_total") < 0)
    ).count()

    assert negative_values == 0

    # 5. item_total must equal price + freight_value.
    invalid_totals = fact_sales.filter(
        F.abs(F.col("item_total") - (F.col("price") + F.col("freight_value")))
        > 0.000001
    ).count()

    assert invalid_totals == 0

    spark.stop()
