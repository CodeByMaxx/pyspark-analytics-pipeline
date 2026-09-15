from datetime import datetime

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.transformations.orders import transform_orders
from src.utils.spark_session import create_spark_session


def test_transform_orders():
    spark = create_spark_session()

    schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_purchase_timestamp", TimestampType(), True),
            StructField("order_approved_at", TimestampType(), True),
            StructField("order_delivered_carrier_date", TimestampType(), True),
            StructField("order_delivered_customer_date", TimestampType(), True),
            StructField("order_estimated_delivery_date", TimestampType(), True),
        ]
    )

    data = [
        (
            "order-1",
            "customer-1",
            "delivered",
            datetime(2018, 1, 10, 12, 30, 0),
            datetime(2018, 1, 15, 10, 0, 0),
            None,
            None,
            None,
        ),
    ]

    df = spark.createDataFrame(data, schema)

    result = transform_orders(df)

    row = result.first()

    assert row["order_id"] == "order-1"
    assert row["customer_id"] == "customer-1"
    assert row["order_status"] == "delivered"
    assert str(row["order_purchase_date"]) == "2018-01-10"
    assert row["order_purchase_year"] == 2018
    assert row["order_purchase_month"] == 1

    spark.stop()
