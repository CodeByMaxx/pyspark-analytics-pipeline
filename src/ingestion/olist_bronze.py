from src.ingestion.csv_reader import read_csv
from src.utils.spark_session import create_spark_session


RAW_PATH = "data/raw/olist_orders_dataset.csv"
BRONZE_PATH = "data/bronze/orders"


def main():
    spark = create_spark_session()

    orders_df = read_csv(spark, RAW_PATH)

    print("Input schema:")
    orders_df.printSchema()

    print("Input rows:", orders_df.count())

    (orders_df.write.mode("overwrite").parquet(BRONZE_PATH))

    print(f"Bronze data written to: {BRONZE_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
