import duckdb

from src.utils.paths import (
    GOLD_FACT_SALES,
    GOLD_DIM_DATE,
    GOLD_DIM_PRODUCT,
)


def main():
    con = duckdb.connect()

    fact_sales_path = f"{GOLD_FACT_SALES}/*.parquet"
    dim_date_path = f"{GOLD_DIM_DATE}/*.parquet"
    dim_product_path = f"{GOLD_DIM_PRODUCT}/*.parquet"

    print("\n=== DuckDB: Gold Data Check ===")

    print("\nFact Sales:")
    result = con.execute(
        f"""
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT order_id) AS orders,
            ROUND(SUM(price), 2) AS revenue,
            ROUND(SUM(freight_value), 2) AS freight,
            ROUND(SUM(item_total), 2) AS total
        FROM read_parquet('{fact_sales_path}')
        """
    ).fetchone()

    print(result)

    print("\nRevenue by year/month:")

    rows = con.execute(
        f"""
        SELECT
            d.year,
            d.month,
            ROUND(SUM(f.item_total), 2) AS revenue
        FROM read_parquet('{fact_sales_path}') f
        JOIN read_parquet('{dim_date_path}') d
            ON f.date_key = d.date_key
        GROUP BY
            d.year,
            d.month
        ORDER BY
            d.year,
            d.month
        """
    ).fetchall()

    for row in rows:
        print(row)

    print("\nTop 10 products:")

    rows = con.execute(
        f"""
        SELECT
            p.product_category_name,
            ROUND(SUM(f.item_total), 2) AS revenue,
            COUNT(*) AS items
        FROM read_parquet('{fact_sales_path}') f
        JOIN read_parquet('{dim_product_path}') p
            ON f.product_key = p.product_key
        GROUP BY
            p.product_category_name
        ORDER BY
            revenue DESC
        LIMIT 10
        """
    ).fetchall()

    for row in rows:
        print(row)

    con.close()


if __name__ == "__main__":
    main()
