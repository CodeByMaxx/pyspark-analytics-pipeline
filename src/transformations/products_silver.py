from src.utils.spark_session import create_spark_session
from src.utils.paths import SILVER_PRODUCTS, BRONZE_PRODUCTS
from src.transformations.products import transform_products


def main():
    spark = create_spark_session()

    print("Loading Bronze products...")

    products = spark.read.parquet(str(BRONZE_PRODUCTS))

    print(f"Bronze rows: {products.count()}")

    print("Transforming products...")

    products_silver = transform_products(products)

    print("\nSilver schema:")
    products_silver.printSchema()

    print(f"\nSilver rows: {products_silver.count()}")

    print("\nSample:")
    products_silver.show(10, truncate=False)

    print(f"\nWriting Silver data to: {SILVER_PRODUCTS}")

    products_silver.write.mode("overwrite").parquet(str(SILVER_PRODUCTS))

    print("\nProducts successfully written.")

    spark.stop()


if __name__ == "__main__":
    main()
