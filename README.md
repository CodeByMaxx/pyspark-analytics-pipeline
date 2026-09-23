# PySpark Analytics Pipeline

A modular and extensible data analytics and ETL pipeline built with **Apache Spark (PySpark), Parquet, DuckDB, and Apache Superset**.

The current implementation uses the **Brazilian E-Commerce Public Dataset by Olist** as its reference data source. The architecture is designed to support additional data sources and ingestion mechanisms in the future.

## Architecture

```text
Raw Data
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
   ├──► Apache Superset
   │
   └──► Power BI (planned)
```

**PySpark** handles ingestion, transformation, dimensional modeling, and data-quality validation.

**DuckDB** provides an analytical SQL layer directly on top of the Gold Parquet datasets.

**Apache Superset** provides the current Linux-based dashboard environment, while Power BI integration is planned for Windows.

## Features

* PySpark-based ETL pipeline
* Bronze / Silver / Gold data architecture
* Parquet-based data storage
* Star-schema dimensional model
* Data-quality validation
* Automated pytest test suite
* DuckDB analytical layer
* Apache Superset dashboard
* Docker-based Superset environment
* Structured pipeline logging
* Extensible ingestion architecture

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

Raw CSV files are stored under:

```text
data/raw/
```

The pipeline itself is not limited to the Olist dataset.

## Data Architecture

### Raw

The Raw layer contains the original source files without modification.

Goals:

* Preserve the original source data
* Provide reproducible pipeline input
* Separate source data from processing logic

### Bronze

The Bronze layer converts raw CSV data into Parquet.

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

The Bronze ingestion logic is designed to be reusable for additional CSV sources.

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

The Gold layer follows a star-schema design.

```text
                    dim_customer
                         │
                         │
                         ▼
dim_product ───────► fact_sales ◄────── dim_seller
                         ▲
                         │
                         │
                      dim_date


dim_customer ───────► fact_orders ◄────── dim_date
```

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

Calendar dimension containing date keys and calendar attributes such as year, month, quarter, day, and weekday.

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

Data-quality validation is an explicit part of the pipeline.

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

Run all tests with:

```bash
pytest
```

The test suite covers:

* Customer transformations
* Order transformations
* Order-item transformations
* Payment transformations
* Product transformations
* Seller transformations
* `fact_sales` construction
* `fact_orders` construction
* `fact_sales` quality
* `fact_orders` quality

The documented reference result is:

```text
10 passed
```

## DuckDB

DuckDB provides the analytical SQL layer on top of the Gold Parquet datasets.

The database is stored at:

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

Apache Superset is the current Linux visualization and dashboard layer.

The repository contains the project-specific Superset configuration rather than the complete Superset source repository.

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

The local Superset `.env` file is intentionally excluded from Git. Use:

```text
superset/docker/.env.example
```

as the configuration template.

### Dashboard

The completed dashboard contains visualizations for:

* Monthly revenue
* Monthly order volume
* Top product categories
* Revenue by order status

![Apache Superset Dashboard](superset/dashboard/superset-overview.png)

The dashboard screenshot is included in the repository.

## Power BI

Power BI is planned as the **Windows visualization layer**.

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

The Linux/Superset environment is currently the primary implementation and test environment. The planned Power BI integration remains separate from the core PySpark pipeline so that the data-processing layer stays platform-independent.

## Project Structure

```text
pyspark-analytics-pipeline/
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── analytics/
│
├── src/
│   ├── ingestion/
│   ├── transformations/
│   ├── analytics/
│   └── utils/
│
├── docs/
│   └── images/
│
├── superset/
│   ├── docker-compose.yml
│   ├── docker/
│   ├── dashboard/
│   └── README.md
│
├── tests/
├── requirements.txt
├── README.md
└── .gitignore
```

The current GitHub repository contains `data/`, `docs/images/`, `src/`, `superset/`, `tests/`, `requirements.txt`, and the project documentation.

## Running Individual Components

### Bronze ingestion

```bash
python -m src.ingestion.bronze_loader
```

### Dimensions

```bash
python -m src.analytics.dim_customer
python -m src.analytics.dim_product
python -m src.analytics.dim_seller
python -m src.analytics.dim_date
```

### Facts

```bash
python -m src.analytics.fact_sales
python -m src.analytics.fact_orders
```

### Quality checks

```bash
python -m src.analytics.fact_sales_quality
python -m src.analytics.fact_orders_quality
```

### DuckDB views

```bash
python -m src.analytics.duckdb_views
```

These commands correspond to the current README's documented pipeline modules.

## Local Development

Create a Python virtual environment:

```bash
python -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

Install the dependencies:

```bash
pip install -r requirements.txt
```

The documented reference environment uses:

* Python 3.12
* PySpark 4.2.0
* DuckDB 1.5.5
* pytest

## Design Principles

### Separation of Concerns

Ingestion, transformation, analytics, validation, and visualization are separated into dedicated modules.

### Layered Data Architecture

Bronze, Silver, and Gold provide a clear separation between source data, cleaned data, and analytics-ready data.

### Reusable Transformations

Transformation functions operate on Spark DataFrames and are designed to be reusable.

### Dimensional Modeling

The Gold layer follows a star schema suitable for BI and analytical workloads.

### Surrogate Keys

Dimensions use surrogate keys to decouple the analytical model from source-system identifiers.

### Data Quality

Validation is implemented as part of the pipeline rather than as a separate manual process.

### Separation of Compute and BI

```text
PySpark
   │
   ├── ETL
   └── Data preparation
          │
          ▼
       DuckDB
          │
          ▼
   ┌──────┴──────┐
   ▼             ▼
Superset      Power BI
(current)     (planned)
```

### Extensibility

The architecture is designed to support additional data sources and ingestion mechanisms.

## Future Extensions

Potential future extensions include:

* Additional CSV sources
* JSON ingestion
* Parquet ingestion
* SQL database connectors
* REST API ingestion
* Kafka / streaming ingestion
* Incremental processing
* Partitioning strategies
* Schema validation
* Data contracts
* Slowly Changing Dimensions
* Advanced Spark SQL analytics
* Window-function-based KPIs
* Additional BI dashboards
* Power BI integration
* CI/CD
* Cloud object storage such as S3
* Workflow orchestration

## Git Hygiene

The following local or generated files are intentionally excluded from Git:

* Superset `.env` files
* Superset local state
* DuckDB database files
* Generated analytics data
* Other runtime artifacts

The repository should contain project configuration and source code, not local credentials or runtime state.

## Status

### Current

* PySpark ETL pipeline
* Bronze layer
* Silver layer
* Gold star schema
* `fact_sales`
* `fact_orders`
* Data-quality validation
* pytest test suite
* DuckDB analytics layer
* Apache Superset integration
* Superset dashboard

### Planned

* Power BI / Windows integration final validation

The Linux-based PySpark, DuckDB and Superset stack is currently the primary reference implementation.

## About

A scalable PySpark-based data analytics pipeline demonstrating:

* Modern ETL architecture
* Layered data processing
* Dimensional modeling
* Data-quality engineering
* Analytical SQL with DuckDB
* BI dashboards with Apache Superset
* Extensible data ingestion

