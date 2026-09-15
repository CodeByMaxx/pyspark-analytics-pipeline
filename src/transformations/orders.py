from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_orders(df: DataFrame) -> DataFrame:
    """
    Clean and standardize the Olist orders dataset.
    """

    return (
        df
        # Remove rows without essential identifiers
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        # Standardize status
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
        # Add useful date columns
        .withColumn("order_purchase_date", F.to_date("order_purchase_timestamp"))
        .withColumn("order_purchase_year", F.year("order_purchase_timestamp"))
        .withColumn("order_purchase_month", F.month("order_purchase_timestamp"))
        # Delivery duration in days
        .withColumn(
            "delivery_days",
            F.datediff(
                F.to_date("order_delivered_customer_date"),
                F.to_date("order_purchase_timestamp"),
            ),
        )
    )
