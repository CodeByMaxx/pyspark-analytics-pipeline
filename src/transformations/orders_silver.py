from src.utils.spark_session import create_spark_session
from src.transformations.orders import transform_orders


BRONZE_PATH = "data/bronze/orders"
SILVER_PATH = "data/silver/orders"


def main():
    spark = create_spark_session()

    orders_df = spark.read.parquet(BRONZE_PATH)

    print("Bronze rows:", orders_df.count())

    silver_df = transform_orders(orders_df)

    print("Silver schema:")
    silver_df.printSchema()

    print("Silver rows:", silver_df.count())

    silver_df.show(10, truncate=False)

    (silver_df.write.mode("overwrite").parquet(SILVER_PATH))

    print(f"Silver data written to: {SILVER_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
