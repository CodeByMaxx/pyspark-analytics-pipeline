from src.transformations.sellers import transform_sellers
from src.utils.spark_session import create_spark_session


def test_transform_sellers():
    spark = create_spark_session()

    data = [
        (
            "seller-1",
            "12345",
            " rio de janeiro ",
            " rj ",
        ),
    ]

    columns = [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ]

    df = spark.createDataFrame(data, columns)

    result = transform_sellers(df)

    row = result.first()

    assert row["seller_id"] == "seller-1"
    assert row["seller_zip_code_prefix"] == 12345
    assert row["seller_city"] == "Rio De Janeiro"
    assert row["seller_state"] == "RJ"

    spark.stop()
