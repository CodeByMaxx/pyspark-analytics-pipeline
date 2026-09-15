from src.transformations.products import transform_products
from src.utils.spark_session import create_spark_session


def test_transform_products():
    spark = create_spark_session()

    data = [
        (
            "product-1",
            " ELETRONICS ",
            50,
            100,
            5,
            1000,
            20,
            30,
            40,
        ),
    ]

    columns = [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]

    df = spark.createDataFrame(data, columns)

    result = transform_products(df)

    row = result.first()

    assert row["product_id"] == "product-1"
    assert row["product_category_name"] == "eletronics"

    assert row["product_weight_g"] == 1000.0
    assert row["product_length_cm"] == 20.0
    assert row["product_height_cm"] == 30.0
    assert row["product_width_cm"] == 40.0

    assert row["product_volume_cm3"] == 24000.0

    spark.stop()
