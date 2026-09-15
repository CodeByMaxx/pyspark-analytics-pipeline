from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_products(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("product_id").isNotNull())
        .withColumn(
            "product_category_name", F.lower(F.trim(F.col("product_category_name")))
        )
        .withColumn("product_weight_g", F.col("product_weight_g").cast("double"))
        .withColumn("product_length_cm", F.col("product_length_cm").cast("double"))
        .withColumn("product_height_cm", F.col("product_height_cm").cast("double"))
        .withColumn("product_width_cm", F.col("product_width_cm").cast("double"))
        .withColumn(
            "product_volume_cm3",
            F.col("product_length_cm")
            * F.col("product_height_cm")
            * F.col("product_width_cm"),
        )
    )
