from src.transformations.customers import transform_customers
from src.utils.spark_session import create_spark_session


def test_transform_customers():
    spark = create_spark_session()

    data = [
        (
            "customer-1",
            "unique-1",
            "12345",
            " sao paulo ",
            " sp ",
        ),
    ]

    columns = [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ]

    df = spark.createDataFrame(data, columns)

    result = transform_customers(df)

    row = result.first()

    assert row["customer_id"] == "customer-1"
    assert row["customer_unique_id"] == "unique-1"

    assert row["customer_zip_code_prefix"] == 12345
    assert row["customer_city"] == "Sao Paulo"
    assert row["customer_state"] == "SP"

    spark.stop()
