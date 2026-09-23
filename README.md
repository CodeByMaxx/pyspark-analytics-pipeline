# PySpark Analytics Pipeline

A modular and extensible data analytics and ETL pipeline built with **Apache Spark (PySpark)**, **Parquet**, **DuckDB**, and **Apache Superset**.

The project implements a complete **Bronze → Silver → Gold** data pipeline using the [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) as its initial data source.

The architecture separates ingestion, transformation, dimensional modeling, data-quality validation, analytical SQL, and visualization into independent components.

## Architecture

```text
Raw CSV Data
     │
     ▼
  Bronze
     │
     ▼
  Silver
     │
     ▼
   Gold
     │
     ▼
  DuckDB
     │
     ├──────────────► Apache Superset
     │
     └──────────────► Power BI
```

### Technology Stack

| Component        | Technology             |
| ---------------- | ---------------------- |
| ETL / Processing | Apache Spark / PySpark |
| Storage          | Parquet                |
| Analytical SQL   | DuckDB                 |
| Visualization    | Apache Superset        |
| Windows BI       | Power BI               |
| Testing          | pytest                 |
| Containerization | Docker Compose         |
| Language         | Python                 |

## Project Structure

```text
pyspark-analytics-pipeline/
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── analytics/
├── src/
│   ├── ingestion/
│   ├── transformations/
│   ├── analytics/
│   └── utils/
├── superset/
│   ├── docker-compose.yml
│   ├── docker/
│   ├── dashboard/
│   └── README.md
├── tests/
├── requirements.txt
└── README.md
```

## Data Source

The initial implementation uses the **Olist Brazilian E-Commerce Public Dataset**.

The dataset contains approximately 100,000 orders from the Brazilian e-commerce marketplace between 2016 and 2018.

The source data contains information about:

* Orders
* Customers
* Products
* Sellers
* Order items
* Payments
* Reviews
* Geolocation
* Product categories

The raw CSV files are stored under:

```text
data/raw/
```

The Olist dataset is the first reference implementation. The pipeline itself is designed to support additional data sources and ingestion mechanisms.

## Data Layers

### Raw

The Raw layer contains the original source files without modification.

Its purpose is to:

* Preserve the original source data
* Provide reproducible pipeline input
* Keep source data separate from processing logic

### Bronze

The Bronze layer converts raw CSV files into Parquet.

Current datasets include:

* `customers`
* `geolocation`
* `order_items`
* `order_payments`
* `order_reviews`
* `orders`
* `products`
* `sellers`
* `product_category_name_translation`

The Bronze ingestion logic is designed to be reusable for additional CSV-based sources.

### Silver

The Silver layer contains cleaned and standardized datasets.

Transformations include:

* Null filtering
* Type casting
* String normalization
* Date conversion
* Timestamp handling
* Derived metrics
* Numeric normalization
* Business-rule preparation

Examples include:

* Normalized order status
* Standardized customer city and state
* Cleaned product categories
* Delivery duration
* Item totals
* Normalized payment fields

### Gold

The Gold layer contains analytics-ready datasets based on a dimensional model.

Current Gold datasets:

* `fact_sales`
* `fact_orders`
* `dim_customer`
* `dim_product`
* `dim_seller`
* `dim_date`

## Star Schema

The analytical model follows a **star-schema** design.

```text
                 dim_customer
                      │
                      │
                      ▼
dim_product ─────► fact_sales ◄───── dim_seller
                      ▲
                      │
                      │
                  dim_date
```

`fact_orders` also uses the customer and date dimensions.

### `fact_sales`

**Grain:** one row per order item / product line.

Contains:

* `order_id`
* `order_item_id`
* `customer_key`
* `product_key`
* `seller_key`
* `date_key`
* `order_status`
* `order_purchase_timestamp`
* `price`
* `freight_value`
* `item_total`

### `fact_orders`

**Grain:** one row per order.

Contains:

* `order_id`
* `customer_key`
* `date_key`
* `order_status`
* Purchase timestamps
* Delivery information
* Payment information
* Payment count
* Payment installments
* Payment type

Payment records are aggregated to the order level before being joined to `fact_orders`. This prevents one-to-many payment relationships from multiplying order rows.

### Dimensions

**`dim_customer`**

Customer master data with a generated surrogate `customer_key`.

**`dim_product`**

Product master data with a generated surrogate `product_key` and derived product volume.

**`dim_seller`**

Seller master data with a generated surrogate `seller_key`.

**`dim_date`**

Calendar dimension containing date keys and attributes such as:

* Year
* Month
* Quarter
* Day
* Weekday

## PySpark Pipeline

The complete pipeline consists of **15 processing and validation steps**:

1. Bronze ingestion
2. Orders Silver transformation
3. Order Items Silver transformation
4. Customers Silver transformation
5. Products Silver transformation
6. Sellers Silver transformation
7. Order Payments Silver transformation
8. Customer dimension
9. Product dimension
10. Seller dimension
11. Date dimension
12. Sales fact
13. Orders fact
14. Sales fact quality validation
15. Orders fact quality validation

Run the complete pipeline with:

```bash
python -m src.pipeline
```

The pipeline provides structured logging including:

* Step name
* Success/failure status
* Execution duration
* Total pipeline duration
* Final pipeline status

A successful reference run completes all 15 steps.

## Data Quality

Data-quality validation is implemented directly as part of the pipeline.

### `fact_sales`

Checks include:

* Row count
* Duplicate order-item detection
* Missing customer keys
* Missing product keys
* Missing seller keys
* Missing date keys
* Negative prices
* Negative freight values
* Item-total consistency

The current reference dataset produces approximately **112,650 `fact_sales` rows**.

### `fact_orders`

Checks include:

* Row count
* Duplicate order IDs
* Missing customer keys
* Missing date keys
* Payment consistency
* Payment record counts
* Payment installments
* Delivery duration
* Invalid delivery values

The current reference dataset produces approximately **99,441 `fact_orders` rows**.

## Testing

The project uses **pytest**.

Run the complete test suite with:

```bash
pytest
```

The test suite currently covers:

* Customer transformations
* Order transformations
* Order-item transformations
* Payment transformations
* Product transformations
* Seller transformations
* Sales fact construction
* Orders fact construction
* Sales fact quality
* Orders fact quality

Current reference result:

```text
10 passed
```

## DuckDB

DuckDB provides the analytical SQL layer on top of the Gold Parquet datasets.

The local database is stored at:

```text
data/analytics/analytics.duckdb
```

The database file is intentionally excluded from Git because it can be recreated from the Gold layer.

Current analytical views include:

* `analytics_sales`
* `analytics_orders`
* `analytics_monthly_revenue`
* `analytics_product_sales`

DuckDB reads the Gold Parquet datasets directly.

```text
Gold Parquet
     │
     ▼
  DuckDB
     │
     ▼
Analytical Views
     │
     ▼
  SQL / BI
```

## Apache Superset

Apache Superset provides the Linux visualization and dashboard layer.

The repository contains the project-specific Superset deployment configuration rather than the complete Superset source repository.

```text
superset/
├── docker-compose.yml
├── docker/
│   ├── .env.example
│   ├── requirements-local.txt
│   └── superset_config.py
├── dashboard/
│   └── superset-overview.png
└── README.md
```

The current deployment uses **Apache Superset 6.0.0**.

### Superset Architecture

```text
PySpark
   │
   ▼
Gold Parquet
   │
   ▼
DuckDB
   │
   ▼
Apache Superset
   │
   ▼
Dashboard
```

The Superset container mounts:

```text
data/analytics → /app/analytics
data/gold      → /app/gold
```

The DuckDB configuration uses:

```text
DUCKDB_GOLD_ROOT=/app/gold
```

The local Superset `.env` file is intentionally excluded from Git.

Use the provided template:

```text
superset/docker/.env.example
```

### Start Superset

From the project root:

```bash
cd superset
docker compose up -d
```

Check the containers:

```bash
docker compose ps
```

Superset is available at:

```text
http://localhost:8088
```

### Superset Dashboard

The dashboard contains visualizations for:

* Monthly revenue
* Monthly order volume
* Top product categories
* Revenue by order status

Dashboard screenshot:

```text
superset/dashboard/superset-overview.png
```

## Power BI

Power BI is planned as the Windows visualization layer.

The intended architecture is:

```text
PySpark
   │
   ▼
Gold Parquet
   │
   ▼
DuckDB
   │
   ▼
Power BI
```

The Linux/Superset environment is the primary implementation and test environment.

The Windows / Power BI integration remains separate so that the core PySpark pipeline stay

