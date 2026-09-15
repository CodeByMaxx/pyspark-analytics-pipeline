from src.utils.spark_session import create_spark_session


def main():
    spark = create_spark_session()

    orders_df = spark.read.parquet("data/bronze/orders")

    print("Bronze schema:")
    orders_df.printSchema()

    print("Bronze rows:", orders_df.count())

    orders_df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
