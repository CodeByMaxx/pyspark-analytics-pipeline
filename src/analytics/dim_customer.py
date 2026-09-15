from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark_session import create_spark_session
from src.utils.paths import SILVER_CUSTOMERS, GOLD_DIM_CUSTOMER


def main():
    spark = create_spark_session()

    print("Loading Silver customers...")

    customers = spark.read.parquet(str(SILVER_CUSTOMERS))

    print("Building customer dimension...")

    dim_customer = (
        customers.select(
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        )
        .dropDuplicates(["customer_unique_id"])
        .withColumn(
            "customer_key",
            F.row_number().over(Window.orderBy("customer_unique_id")),
        )
        .select(
            "customer_key",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        )
    )

    print("\n=== DIM CUSTOMER ===")

    print("\nSchema:")
    dim_customer.printSchema()

    row_count = dim_customer.count()

    print(f"\nRows: {row_count}")

    print("\nSample:")
    dim_customer.show(10, truncate=False)

    print(f"\nWriting Gold data to: {GOLD_DIM_CUSTOMER}")

    dim_customer.write.mode("overwrite").parquet(str(GOLD_DIM_CUSTOMER))

    print("\nCustomer dimension successfully written.")

    spark.stop()


if __name__ == "__main__":
    main()
