from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.spark_session import create_spark_session

from src.utils.paths import (
    SILVER_ORDERS,
    SILVER_CUSTOMERS,
    SILVER_ORDER_PAYMENTS,
    GOLD_DIM_CUSTOMER,
    GOLD_DIM_DATE,
    GOLD_FACT_ORDERS,
)


def build_fact_orders(
    orders: DataFrame,
    customers: DataFrame,
    payments: DataFrame,
    dim_customer: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:

    payment_summary = payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("payment_value"),
        F.count("*").alias("payment_count"),
        F.max("payment_installments").alias("payment_installments"),
        F.first("payment_type").alias("payment_type"),
    )

    return (
        orders.alias("o")
        .join(
            customers.alias("c"),
            F.col("o.customer_id") == F.col("c.customer_id"),
            "left",
        )
        .join(
            dim_customer.alias("dc"),
            F.col("c.customer_unique_id") == F.col("dc.customer_unique_id"),
            "left",
        )
        .join(
            dim_date.alias("dd"),
            F.col("o.order_purchase_date") == F.col("dd.date"),
            "left",
        )
        .join(
            payment_summary.alias("p"),
            F.col("o.order_id") == F.col("p.order_id"),
            "left",
        )
        .select(
            F.col("o.order_id"),
            F.col("dc.customer_key"),
            F.col("dd.date_key"),
            F.col("o.order_status"),
            F.col("o.order_purchase_timestamp"),
            F.col("o.order_approved_at"),
            F.col("o.order_delivered_carrier_date"),
            F.col("o.order_delivered_customer_date"),
            F.col("o.order_estimated_delivery_date"),
            F.col("o.delivery_days"),
            F.col("p.payment_value"),
            F.col("p.payment_count"),
            F.col("p.payment_installments"),
            F.col("p.payment_type"),
        )
    )


def main():
    spark = create_spark_session()

    print("Loading Silver data...")

    orders = spark.read.parquet(str(SILVER_ORDERS))
    customers = spark.read.parquet(str(SILVER_CUSTOMERS))
    payments = spark.read.parquet(str(SILVER_ORDER_PAYMENTS))

    print("Loading Gold dimensions...")

    dim_customer = spark.read.parquet(str(GOLD_DIM_CUSTOMER))
    dim_date = spark.read.parquet(str(GOLD_DIM_DATE))

    print("Aggregating payment data...")

    payment_summary = payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("payment_value"),
        F.count("*").alias("payment_count"),
        F.max("payment_installments").alias("payment_installments"),
        F.first("payment_type").alias("payment_type"),
    )

    print("Building fact_orders...")

    fact_orders = (
        orders.alias("o")
        .join(
            customers.alias("c"),
            F.col("o.customer_id") == F.col("c.customer_id"),
            "left",
        )
        .join(
            dim_customer.alias("dc"),
            F.col("c.customer_unique_id") == F.col("dc.customer_unique_id"),
            "left",
        )
        .join(
            dim_date.alias("dd"),
            F.col("o.order_purchase_date") == F.col("dd.date"),
            "left",
        )
        .join(
            payment_summary.alias("p"),
            F.col("o.order_id") == F.col("p.order_id"),
            "left",
        )
        .select(
            F.col("o.order_id"),
            F.col("dc.customer_key"),
            F.col("dd.date_key"),
            F.col("o.order_status"),
            F.col("o.order_purchase_timestamp"),
            F.col("o.order_approved_at"),
            F.col("o.order_delivered_carrier_date"),
            F.col("o.order_delivered_customer_date"),
            F.col("o.order_estimated_delivery_date"),
            F.col("o.delivery_days"),
            F.col("p.payment_value"),
            F.col("p.payment_count"),
            F.col("p.payment_installments"),
            F.col("p.payment_type"),
        )
    )

    print("\n=== FACT ORDERS ===")

    print("\nSchema:")
    fact_orders.printSchema()

    row_count = fact_orders.count()
    print(f"\nRows: {row_count}")

    print("\nSample:")
    fact_orders.show(10, truncate=False)

    print(f"\nWriting Gold data to: {GOLD_FACT_ORDERS}")

    fact_orders.write.mode("overwrite").parquet(str(GOLD_FACT_ORDERS))

    print("\nFact orders successfully written.")

    spark.stop()


if __name__ == "__main__":
    main()
