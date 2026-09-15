from datetime import date, datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.analytics.fact_orders import build_fact_orders
from src.utils.spark_session import create_spark_session


def test_build_fact_orders():
    spark = create_spark_session()

    orders_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_purchase_timestamp", TimestampType(), True),
            StructField("order_approved_at", TimestampType(), True),
            StructField("order_delivered_carrier_date", TimestampType(), True),
            StructField("order_delivered_customer_date", TimestampType(), True),
            StructField("order_estimated_delivery_date", TimestampType(), True),
            StructField("delivery_days", IntegerType(), True),
        ]
    )

    orders = spark.createDataFrame(
        [
            (
                "order-1",
                "customer-1",
                "delivered",
                datetime(2018, 1, 10, 12, 30, 0),
                datetime(2018, 1, 10, 13, 0, 0),
                None,
                datetime(2018, 1, 15, 10, 0, 0),
                datetime(2018, 1, 20, 0, 0, 0),
                5,
            )
        ],
        orders_schema,
    ).withColumn(
        "order_purchase_date",
        F.to_date("order_purchase_timestamp"),
    )

    customers = spark.createDataFrame(
        [
            ("customer-1", "unique-customer-1"),
        ],
        ["customer_id", "customer_unique_id"],
    )

    payments = spark.createDataFrame(
        [
            ("order-1", 1, "credit_card", 2, 100.0),
            ("order-1", 2, "voucher", 1, 25.0),
        ],
        [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ],
    )

    dim_customer = spark.createDataFrame(
        [
            (101, "unique-customer-1"),
        ],
        ["customer_key", "customer_unique_id"],
    )

    dim_date_schema = StructType(
        [
            StructField("date_key", IntegerType(), True),
            StructField("date", DateType(), True),
        ]
    )

    dim_date = spark.createDataFrame(
        [
            (20180110, date(2018, 1, 10)),
        ],
        dim_date_schema,
    )

    result = build_fact_orders(
        orders,
        customers,
        payments,
        dim_customer,
        dim_date,
    )

    rows = result.collect()

    assert len(rows) == 1

    row = rows[0]

    assert row["order_id"] == "order-1"
    assert row["customer_key"] == 101
    assert row["date_key"] == 20180110
    assert row["order_status"] == "delivered"

    # Two payment records must be aggregated into one order.
    assert row["payment_value"] == 125.0
    assert row["payment_count"] == 2
    assert row["payment_installments"] == 2

    assert row["payment_type"] in {"credit_card", "voucher"}

    spark.stop()
