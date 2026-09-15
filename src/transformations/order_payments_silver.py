from src.utils.spark_session import create_spark_session
from src.transformations.order_payments import transform_order_payments


BRONZE_PATH = "data/bronze/order_payments"
SILVER_PATH = "data/silver/order_payments"


def main():
    spark = create_spark_session()

    print("Loading Bronze order payments...")

    payments = spark.read.parquet(BRONZE_PATH)

    print(f"Bronze rows: {payments.count()}")

    print("Transforming order payments...")

    payments_silver = transform_order_payments(payments)

    print("\nSilver schema:")
    payments_silver.printSchema()

    print(f"\nSilver rows: {payments_silver.count()}")

    print("\nSample:")
    payments_silver.show(10, truncate=False)

    print(f"\nWriting Silver data to: {SILVER_PATH}")

    (payments_silver.write.mode("overwrite").parquet(SILVER_PATH))

    print("\nOrder payments successfully written.")

    spark.stop()


if __name__ == "__main__":
    main()
