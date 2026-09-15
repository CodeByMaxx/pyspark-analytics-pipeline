import duckdb

from src.utils.paths import ANALYTICS_DIR, DUCKDB_PATH


def create_duckdb_connection():
    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)

    return duckdb.connect(str(DUCKDB_PATH))
