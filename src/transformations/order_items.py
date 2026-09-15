from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_order_items(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("order_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("seller_id").isNotNull())
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
        .withColumn("item_total", F.col("price") + F.col("freight_value"))
    )
