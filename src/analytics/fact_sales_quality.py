from pyspark.sql import functions as F

from src.utils.spark_session import create_spark_session


FACT_PATH = "data/gold/fact_sales"


def main():
    spark = create_spark_session()

    fact_sales = spark.read.parquet(FACT_PATH)

    print("=== FACT SALES QUALITY CHECK ===")

    # 1. Anzahl Zeilen
    print(f"\nTotal rows: {fact_sales.count()}")

    # 2. Schema
    print("\nSchema:")
    fact_sales.printSchema()

    # 3. NULL Dimension Keys
    print("\nNULL dimension keys:")

    fact_sales.select(
        F.count(F.when(F.col("customer_key").isNull(), 1)).alias(
            "missing_customer_key"
        ),
        F.count(F.when(F.col("product_key").isNull(), 1)).alias("missing_product_key"),
        F.count(F.when(F.col("seller_key").isNull(), 1)).alias("missing_seller_key"),
        F.count(F.when(F.col("date_key").isNull(), 1)).alias("missing_date_key"),
    ).show()

    # 4. Doppelte Order Items
    print("\nDuplicate order items:")

    (
        fact_sales.groupBy(
            "order_id",
            "order_item_id",
        )
        .count()
        .filter(F.col("count") > 1)
        .show()
    )

    # 5. Umsatz
    print("\nSales metrics:")

    fact_sales.select(
        F.round(F.sum("price"), 2).alias("revenue"),
        F.round(F.sum("freight_value"), 2).alias("freight"),
        F.round(F.sum("item_total"), 2).alias("total"),
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("items"),
    ).show()

    # 6. Sample
    print("\nSample:")
    fact_sales.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
