from src.transformations.order_items import transform_order_items
from src.utils.spark_session import create_spark_session


def test_transform_order_items():
    spark = create_spark_session()

    data = [
        (
            "order-1",
            1,
            "product-1",
            "seller-1",
            100.00,
            15.50,
        ),
    ]

    columns = [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "price",
        "freight_value",
    ]

    df = spark.createDataFrame(data, columns)

    result = transform_order_items(df)

    row = result.first()

    assert row["order_id"] == "order-1"
    assert row["product_id"] == "product-1"
    assert row["seller_id"] == "seller-1"

    assert row["price"] == 100.00
    assert row["freight_value"] == 15.50
    assert row["item_total"] == 115.50

    spark.stop()
