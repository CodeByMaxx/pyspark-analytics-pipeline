from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_customers(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("customer_id").isNotNull())
        .filter(F.col("customer_unique_id").isNotNull())
        .withColumn(
            "customer_zip_code_prefix",
            F.col("customer_zip_code_prefix").cast("integer"),
        )
        .withColumn("customer_city", F.initcap(F.trim(F.col("customer_city"))))
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
    )
