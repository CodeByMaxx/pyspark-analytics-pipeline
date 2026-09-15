from datetime import datetime

from src.analytics.fact_sales import build_fact_sales
from src.utils.spark_session import create_spark_session


def test_build_fact_sales():
    spark = create_spark_session()

    orders = spark.createDataFrame(
        [
            (
                "order-1",
                "customer-1",
                "delivered",
                datetime(2018, 1, 10, 12, 30, 0),
            ),
        ],
        [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
        ],
    ).withColumn(
        "order_purchase_date",
        __import__("pyspark.sql.functions", fromlist=["to_date"]).to_date(
            "order_purchase_timestamp"
        ),
    )

    order_items = spark.createDataFrame(
        [
            (
                "order-1",
                1,
                "product-1",
                "seller-1",
                100.0,
                15.0,
                115.0,
            ),
        ],
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "price",
            "freight_value",
            "item_total",
        ],
    )

    customers = spark.createDataFrame(
        [
            (
                "customer-1",
                "unique-customer-1",
            ),
        ],
        [
            "customer_id",
            "customer_unique_id",
        ],
    )

    dim_customer = spark.createDataFrame(
        [
            (
                101,
                "unique-customer-1",
            ),
        ],
        [
            "customer_key",
            "customer_unique_id",
        ],
    )

    dim_product = spark.createDataFrame(
        [
            (
                201,
                "product-1",
            ),
        ],
        [
            "product_key",
            "product_id",
        ],
    )

    dim_seller = spark.createDataFrame(
        [
            (
                301,
                "seller-1",
            ),
        ],
        [
            "seller_key",
            "seller_id",
        ],
    )

    dim_date = spark.createDataFrame(
        [
            (
                20180110,
                datetime(2018, 1, 10, 0, 0, 0).date(),
            ),
        ],
        [
            "date_key",
            "date",
        ],
    )

    result = build_fact_sales(
        orders,
        order_items,
        customers,
        dim_customer,
        dim_product,
        dim_seller,
        dim_date,
    )

    rows = result.collect()

    assert len(rows) == 1

    row = rows[0]

    assert row["order_id"] == "order-1"
    assert row["order_item_id"] == 1

    assert row["customer_key"] == 101
    assert row["product_key"] == 201
    assert row["seller_key"] == 301
    assert row["date_key"] == 20180110

    assert row["order_status"] == "delivered"

    assert row["price"] == 100.0
    assert row["freight_value"] == 15.0
    assert row["item_total"] == 115.0

    spark.stop()
