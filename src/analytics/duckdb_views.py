from src.utils.duckdb import create_duckdb_connection
from src.utils.paths import (
    GOLD_FACT_SALES,
    GOLD_FACT_ORDERS,
    GOLD_DIM_DATE,
    GOLD_DIM_PRODUCT,
)


def create_views(con):
    print("\n=== DuckDB Views ===")

    views = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_type = 'VIEW'
        ORDER BY table_name
        """
    ).fetchall()

    for view in views:
        print(view[0])

    fact_sales = f"{GOLD_FACT_SALES}/*.parquet"
    fact_orders = f"{GOLD_FACT_ORDERS}/*.parquet"
    dim_date = f"{GOLD_DIM_DATE}/*.parquet"
    dim_product = f"{GOLD_DIM_PRODUCT}/*.parquet"

    con.execute(
        f"""
        CREATE OR REPLACE VIEW analytics_sales AS
        SELECT
            f.order_id,
            f.order_item_id,
            f.customer_key,
            f.product_key,
            f.seller_key,
            f.date_key,
            f.order_status,
            f.order_purchase_timestamp,
            f.price,
            f.freight_value,
            f.item_total
        FROM read_parquet('{fact_sales}') f
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW analytics_orders AS
        SELECT
            order_id,
            customer_key,
            date_key,
            order_status,
            order_purchase_timestamp,
            delivery_days,
            payment_value,
            payment_count,
            payment_installments,
            payment_type
        FROM read_parquet('{fact_orders}')
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW analytics_monthly_revenue AS
        SELECT
            d.year,
            d.month,
            ROUND(SUM(f.item_total), 2) AS revenue,
            COUNT(DISTINCT f.order_id) AS orders,
            COUNT(*) AS items
        FROM read_parquet('{fact_sales}') f
        JOIN read_parquet('{dim_date}') d
            ON f.date_key = d.date_key
        GROUP BY
            d.year,
            d.month
        ORDER BY
            d.year,
            d.month
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW analytics_product_sales AS
        SELECT
            p.product_category_name,
            ROUND(SUM(f.price), 2) AS product_revenue,
            ROUND(SUM(f.freight_value), 2) AS freight_revenue,
            ROUND(SUM(f.item_total), 2) AS total_revenue,
            COUNT(*) AS items,
            COUNT(DISTINCT f.order_id) AS orders
        FROM read_parquet('{fact_sales}') f
        JOIN read_parquet('{dim_product}') p
            ON f.product_key = p.product_key
        GROUP BY
            p.product_category_name
        """
    )


def main():
    con = create_duckdb_connection()

    print("Creating DuckDB analytics views...")

    create_views(con)

    print("\n=== analytics_monthly_revenue ===")

    rows = con.execute(
        """
        SELECT *
        FROM analytics_monthly_revenue
        """
    ).fetchall()

    for row in rows:
        print(row)

    print("\n=== analytics_product_sales ===")

    rows = con.execute(
        """
        SELECT *
        FROM analytics_product_sales
        ORDER BY total_revenue DESC
        LIMIT 10
        """
    ).fetchall()

    for row in rows:
        print(row)

    con.close()


if __name__ == "__main__":
    main()
