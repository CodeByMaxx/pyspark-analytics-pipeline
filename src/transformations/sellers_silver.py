from src.transformations.sellers import transform_sellers
from src.utils.spark_session import create_spark_session


BRONZE_PATH = "data/bronze/sellers"
SILVER_PATH = "data/silver/sellers"


def main():
    spark = create_spark_session()

    sellers = spark.read.parquet(BRONZE_PATH)

    silver_sellers = transform_sellers(sellers)

    print("Seller Silver Schema:")
    silver_sellers.printSchema()

    print("Seller Rows:", silver_sellers.count())

    silver_sellers.show(10, truncate=False)

    (silver_sellers.write.mode("overwrite").parquet(SILVER_PATH))

    print(f"Silver data written to: {SILVER_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
