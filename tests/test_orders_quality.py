from src.utils.spark_session import create_spark_session
from src.transformations.orders_quality import check_orders_quality


def main():
    spark = create_spark_session()

    orders_df = spark.read.parquet("data/silver/orders")

    check_orders_quality(orders_df)

    spark.stop()


if __name__ == "__main__":
    main()
