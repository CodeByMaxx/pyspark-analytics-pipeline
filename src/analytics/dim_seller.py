from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark_session import create_spark_session
from src.utils.paths import SILVER_SELLERS, GOLD_DIM_SELLER


def main():
    spark = create_spark_session()

    print("Loading Silver sellers...")

    sellers = spark.read.parquet(str(SILVER_SELLERS))

    print("Building seller dimension...")

    dim_seller = (
        sellers.select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        )
        .dropDuplicates(["seller_id"])
        .withColumn(
            "seller_key",
            F.row_number().over(Window.orderBy("seller_id")),
        )
        .select(
            "seller_key",
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        )
    )

    print("\n=== DIM SELLER ===")

    print("\nSchema:")
    dim_seller.printSchema()

    row_count = dim_seller.count()

    print(f"\nRows: {row_count}")

    print("\nSample:")
    dim_seller.show(10, truncate=False)

    print(f"\nWriting Gold data to: {GOLD_DIM_SELLER}")

    dim_seller.write.mode("overwrite").parquet(str(GOLD_DIM_SELLER))

    print("\nSeller dimension successfully written.")

    spark.stop()


if __name__ == "__main__":
    main()
