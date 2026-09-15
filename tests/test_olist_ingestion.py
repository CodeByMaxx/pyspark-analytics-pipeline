from src.ingestion.csv_reader import read_csv
from src.utils.spark_session import create_spark_session


def main():
    spark = create_spark_session()

    path = "data/raw/olist_orders_dataset.csv"

    orders_df = read_csv(spark, path)

    print("Schema:")
    orders_df.printSchema()

    print("Rows:", orders_df.count())

    print("Sample:")
    orders_df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
