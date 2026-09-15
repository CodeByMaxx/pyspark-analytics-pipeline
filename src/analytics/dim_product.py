from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark_session import create_spark_session
from src.utils.paths import SILVER_PRODUCTS, GOLD_DIM_PRODUCT


def main():
    spark = create_spark_session()

    print("Loading Silver products...")

    products = spark.read.parquet(str(SILVER_PRODUCTS))

    print("Building product dimension...")

    dim_product = (
        products.select(
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "product_volume_cm3",
        )
        .dropDuplicates(["product_id"])
        .withColumn(
            "product_key",
            F.row_number().over(Window.orderBy("product_id")),
        )
        .select(
            "product_key",
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "product_volume_cm3",
        )
    )

    print("\n=== DIM PRODUCT ===")

    print("\nSchema:")
    dim_product.printSchema()

    row_count = dim_product.count()

    print(f"\nRows: {row_count}")

    print("\nSample:")
    dim_product.show(10, truncate=False)

    print(f"\nWriting Gold data to: {GOLD_DIM_PRODUCT}")

    dim_product.write.mode("overwrite").parquet(str(GOLD_DIM_PRODUCT))

    print("\nProduct dimension successfully written.")

    spark.stop()


if __name__ == "__main__":
    main()
