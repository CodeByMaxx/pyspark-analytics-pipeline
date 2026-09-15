from src.utils.spark_session import create_spark_session
from src.transformations.order_items import transform_order_items


BRONZE_PATH = "data/bronze/order_items"
SILVER_PATH = "data/silver/order_items"


def main():
    spark = create_spark_session()

    df = spark.read.parquet(BRONZE_PATH)

    print("Bronze rows:", df.count())

    silver_df = transform_order_items(df)

    print("Silver schema:")
    silver_df.printSchema()

    print("Silver rows:", silver_df.count())

    silver_df.show(10, truncate=False)

    (silver_df.write.mode("overwrite").parquet(SILVER_PATH))

    print(f"Silver data written to: {SILVER_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
