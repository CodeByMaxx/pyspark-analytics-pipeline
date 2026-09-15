from pyspark.sql import DataFrame, SparkSession


def read_csv(
    spark: SparkSession,
    path: str,
    header: bool = True,
    infer_schema: bool = True,
) -> DataFrame:
    """
    Read a CSV file into a Spark DataFrame.
    """

    return (
        spark.read.option("header", header)
        .option("inferSchema", infer_schema)
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )
