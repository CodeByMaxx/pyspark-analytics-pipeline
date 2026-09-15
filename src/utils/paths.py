from pathlib import Path


# Project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]


# Data directories
DATA_DIR = PROJECT_ROOT / "data"

RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

ANALYTICS_DIR = DATA_DIR / "analytics"
DUCKDB_PATH = ANALYTICS_DIR / "analytics.duckdb"

# Source-specific directories
BRONZE_ORDERS = BRONZE_DIR / "orders"
BRONZE_ORDER_ITEMS = BRONZE_DIR / "order_items"
BRONZE_ORDER_PAYMENTS = BRONZE_DIR / "order_payments"
BRONZE_CUSTOMERS = BRONZE_DIR / "customers"
BRONZE_PRODUCTS = BRONZE_DIR / "products"
BRONZE_SELLERS = BRONZE_DIR / "sellers"


# Silver tables
SILVER_ORDERS = SILVER_DIR / "orders"
SILVER_ORDER_ITEMS = SILVER_DIR / "order_items"
SILVER_ORDER_PAYMENTS = SILVER_DIR / "order_payments"
SILVER_CUSTOMERS = SILVER_DIR / "customers"
SILVER_PRODUCTS = SILVER_DIR / "products"
SILVER_SELLERS = SILVER_DIR / "sellers"


# Gold tables
GOLD_FACT_SALES = GOLD_DIR / "fact_sales"
GOLD_FACT_ORDERS = GOLD_DIR / "fact_orders"

GOLD_DIM_CUSTOMER = GOLD_DIR / "dim_customer"
GOLD_DIM_PRODUCT = GOLD_DIR / "dim_product"
GOLD_DIM_SELLER = GOLD_DIR / "dim_seller"
GOLD_DIM_DATE = GOLD_DIR / "dim_date"
