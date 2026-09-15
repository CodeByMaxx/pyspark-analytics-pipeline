from src.utils.spark_session import create_spark_session


def main():
    spark = create_spark_session()

    print("Spark version:", spark.version)

    data = [
        (1, "Alice", 100.0),
        (2, "Bob", 200.0),
        (3, "Charlie", 150.0),
    ]

    df = spark.createDataFrame(data, ["id", "name", "amount"])

    df.show()

    spark.stop()


if __name__ == "__main__":
    main()
