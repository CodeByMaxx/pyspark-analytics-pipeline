from src.utils.spark_session import create_spark_session
from src.transformations.customers import transform_customers


BRONZE_PATH = "data/bronze/customers"
SILVER_PATH = "data/silver/customers"


def main():
    spark = create_spark_session()

    df = spark.read.parquet(BRONZE_PATH)

    silver_df = transform_customers(df)

    print("Customers:")
    print("Rows:", silver_df.count())

    silver_df.printSchema()
    silver_df.show(5, truncate=False)

    (silver_df.write.mode("overwrite").parquet(SILVER_PATH))

    print(f"Silver data written to: {SILVER_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
