from pathlib import Path

from src.ingestion.csv_reader import read_csv
from src.utils.spark_session import create_spark_session


RAW_DIR = Path("data/raw")
BRONZE_DIR = Path("data/bronze")


def load_csv_to_bronze(spark, csv_path: Path):
    table_name = csv_path.stem.replace("olist_", "").replace("_dataset", "")

    output_path = BRONZE_DIR / table_name

    print(f"Processing: {csv_path}")
    print(f"Output:    {output_path}")

    df = read_csv(spark, str(csv_path))

    print(f"Rows: {df.count()}")

    (df.write.mode("overwrite").parquet(str(output_path)))

    print(f"Finished: {table_name}")
    print("-" * 60)


def main():
    spark = create_spark_session()

    csv_files = sorted(RAW_DIR.glob("*.csv"))

    print(f"Found {len(csv_files)} CSV files")

    for csv_path in csv_files:
        load_csv_to_bronze(spark, csv_path)

    spark.stop()


if __name__ == "__main__":
    main()
