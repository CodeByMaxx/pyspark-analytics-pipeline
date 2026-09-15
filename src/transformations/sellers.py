from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_sellers(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("seller_id").isNotNull())
        .withColumn(
            "seller_zip_code_prefix", F.col("seller_zip_code_prefix").cast("integer")
        )
        .withColumn("seller_city", F.initcap(F.trim(F.col("seller_city"))))
        .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
    )
