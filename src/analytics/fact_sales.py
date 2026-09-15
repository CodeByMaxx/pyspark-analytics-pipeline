from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.spark_session import create_spark_session
from src.utils.paths import (
    SILVER_ORDERS,
    SILVER_ORDER_ITEMS,
    SILVER_CUSTOMERS,
    GOLD_DIM_CUSTOMER,
    GOLD_DIM_PRODUCT,
    GOLD_DIM_SELLER,
    GOLD_DIM_DATE,
    GOLD_FACT_SALES,
)


def build_fact_sales(
    orders: DataFrame,
    order_items: DataFrame,
    customers: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
    dim_seller: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:

    return (
        order_items.alias("oi")
        .join(
            orders.alias("o"),
            F.col("oi.order_id") == F.col("o.order_id"),
            "inner",
        )
        .join(
            customers.alias("c"),
            F.col("o.customer_id") == F.col("c.customer_id"),
            "left",
        )
        .join(
            dim_customer.alias("dc"),
            F.col("c.customer_unique_id") == F.col("dc.customer_unique_id"),
            "left",
        )
        .join(
            dim_product.alias("dp"),
            F.col("oi.product_id") == F.col("dp.product_id"),
            "left",
        )
        .join(
            dim_seller.alias("ds"),
            F.col("oi.seller_id") == F.col("ds.seller_id"),
            "left",
        )
        .join(
            dim_date.alias("dd"),
            F.col("o.order_purchase_date") == F.col("dd.date"),
            "left",
        )
        .select(
            F.col("oi.order_id"),
            F.col("oi.order_item_id"),
            F.col("dc.customer_key"),
            F.col("dp.product_key"),
            F.col("ds.seller_key"),
            F.col("dd.date_key"),
            F.col("o.order_status"),
            F.col("o.order_purchase_timestamp"),
            F.col("oi.price"),
            F.col("oi.freight_value"),
            F.col("oi.item_total"),
        )
    )


def main():
    spark = create_spark_session()

    print("Loading Silver data...")

    orders = spark.read.parquet(str(SILVER_ORDERS))
    order_items = spark.read.parquet(str(SILVER_ORDER_ITEMS))
    customers = spark.read.parquet(str(SILVER_CUSTOMERS))

    print("Loading Gold dimensions...")

    dim_customer = spark.read.parquet(str(GOLD_DIM_CUSTOMER))
    dim_product = spark.read.parquet(str(GOLD_DIM_PRODUCT))
    dim_seller = spark.read.parquet(str(GOLD_DIM_SELLER))
    dim_date = spark.read.parquet(str(GOLD_DIM_DATE))

    print("Building fact_sales...")

    fact_sales = build_fact_sales(
        orders,
        order_items,
        customers,
        dim_customer,
        dim_product,
        dim_seller,
        dim_date,
    )

    print("\n=== FACT SALES ===")

    print("\nSchema:")
    fact_sales.printSchema()

    row_count = fact_sales.count()
    print(f"\nRows: {row_count}")

    print("\nSample:")
    fact_sales.show(10, truncate=False)

    print(f"\nWriting Gold data to: {GOLD_FACT_SALES}")

    fact_sales.write.mode("overwrite").parquet(str(GOLD_FACT_SALES))

    print("\nFact sales successfully written.")

    spark.stop()


if __name__ == "__main__":
    main()
