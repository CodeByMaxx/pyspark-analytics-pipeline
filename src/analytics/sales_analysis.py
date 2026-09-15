from src.utils.spark_session import create_spark_session


GOLD_PATH = "data/gold/fact_sales"


def main():
    spark = create_spark_session()

    fact_sales = spark.read.parquet(GOLD_PATH)

    fact_sales.createOrReplaceTempView("fact_sales")

    result = spark.sql("""
        SELECT
            order_purchase_year AS year,
            order_purchase_month AS month,
            ROUND(SUM(price), 2) AS revenue,
            ROUND(SUM(freight_value), 2) AS freight,
            COUNT(DISTINCT order_id) AS orders,
            COUNT(*) AS items
        FROM fact_sales
        GROUP BY
            order_purchase_year,
            order_purchase_month
        ORDER BY
            year,
            month
    """)

    result.show(100, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
