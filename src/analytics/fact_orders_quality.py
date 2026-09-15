from pyspark.sql import functions as F

from src.utils.spark_session import create_spark_session
from src.utils.paths import GOLD_FACT_ORDERS


def main():
    spark = create_spark_session()

    fact_orders = spark.read.parquet(str(GOLD_FACT_ORDERS))

    print("\n=== FACT ORDERS QUALITY CHECK ===")

    # --------------------------------------------------
    # Basic information
    # --------------------------------------------------

    total_rows = fact_orders.count()

    print(f"\nTotal rows: {total_rows}")

    print("\nSchema:")
    fact_orders.printSchema()

    # --------------------------------------------------
    # Duplicate order IDs
    # --------------------------------------------------

    print("\nDuplicate order IDs:")

    duplicates = fact_orders.groupBy("order_id").count().filter(F.col("count") > 1)

    duplicates.show()

    duplicate_count = duplicates.count()

    print(f"Duplicate order count: {duplicate_count}")

    # --------------------------------------------------
    # NULL dimension keys
    # --------------------------------------------------

    print("\nNULL dimension keys:")

    fact_orders.select(
        F.count(F.when(F.col("customer_key").isNull(), 1)).alias(
            "missing_customer_key"
        ),
        F.count(F.when(F.col("date_key").isNull(), 1)).alias("missing_date_key"),
    ).show()

    # --------------------------------------------------
    # Order status distribution
    # --------------------------------------------------

    print("\nOrder status distribution:")

    (fact_orders.groupBy("order_status").count().orderBy(F.desc("count")).show())

    # --------------------------------------------------
    # Payment metrics
    # --------------------------------------------------

    print("\nPayment metrics:")

    fact_orders.select(
        F.round(F.sum("payment_value"), 2).alias("total_payment_value"),
        F.round(F.avg("payment_value"), 2).alias("avg_payment_value"),
        F.round(F.avg("payment_installments"), 2).alias("avg_payment_installments"),
        F.sum("payment_count").alias("total_payment_records"),
    ).show()

    # --------------------------------------------------
    # Missing payments
    # --------------------------------------------------

    print("\nOrders without payment information:")

    fact_orders.select(
        F.count(F.when(F.col("payment_value").isNull(), 1)).alias(
            "orders_without_payment"
        )
    ).show()

    # --------------------------------------------------
    # Delivery statistics
    # --------------------------------------------------

    print("\nDelivery statistics:")

    fact_orders.select(
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
        F.min("delivery_days").alias("min_delivery_days"),
        F.max("delivery_days").alias("max_delivery_days"),
    ).show()

    # --------------------------------------------------
    # Invalid delivery days
    # --------------------------------------------------

    print("\nInvalid delivery days:")

    invalid_delivery = fact_orders.filter(F.col("delivery_days") < 0).select(
        "order_id",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "delivery_days",
    )

    invalid_delivery.show()

    print(
        "Invalid delivery count:",
        invalid_delivery.count(),
    )

    # --------------------------------------------------
    # Sample
    # --------------------------------------------------

    print("\nSample:")

    fact_orders.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
