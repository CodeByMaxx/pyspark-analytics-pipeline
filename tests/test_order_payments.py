from src.transformations.order_payments import transform_order_payments
from src.utils.spark_session import create_spark_session


def test_transform_order_payments():
    spark = create_spark_session()

    data = [
        (
            "order-1",
            1,
            " Credit_Card ",
            "2",
            "150.75",
        ),
    ]

    columns = [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    ]

    df = spark.createDataFrame(data, columns)

    result = transform_order_payments(df)

    row = result.first()

    assert row["order_id"] == "order-1"
    assert row["payment_type"] == "credit_card"
    assert row["payment_installments"] == 2
    assert row["payment_value"] == 150.75

    spark.stop()
