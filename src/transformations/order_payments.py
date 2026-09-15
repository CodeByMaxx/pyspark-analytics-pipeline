from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_order_payments(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("order_id").isNotNull())
        .filter(F.col("payment_value").isNotNull())
        .withColumn("payment_value", F.col("payment_value").cast("double"))
        .withColumn(
            "payment_installments", F.col("payment_installments").cast("integer")
        )
        .withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
    )
