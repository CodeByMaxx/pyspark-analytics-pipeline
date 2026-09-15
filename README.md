xrk Analytics Pipeline

A modular and extensible analytics and ETL pipeline built with Apache Spark (PySpark), Parquet, DuckDB, and Apache Superset.

The current implementation uses the **Brazilian E-Commerce Public Dataset by Olist** as the initial reference data source. The architecture is designed to support additional data sources and ingestion mechanisms in the future.

## Architecture

Raw Data  
↓  
Bronze  
↓  
Silver  
↓  
Gold  
↓  
DuckDB  
↓  
Apache Superset / Power BI

PySpark is responsible for ingestion, transformation, dimensional modeling, and data-quality validation.

DuckDB provides a lightweight analytical SQL layer directly on top of the Gold Parquet datasets.

Apache Superset provides interactive dashboards on Linux, while Power BI can be used as the Windows BI layer.

## Project Structure

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
├── notebooks/
├── powerbi/
├── superset/
│   ├── docker-compose.yml
│   ├── docker/
│   │   ├── .env.example
│   │   ├── requirements-local.txt
│   │   └── superset_config.py
│   ├── dashboard/
│   │   └── superset-overview.png
│   └── README.md
├── tests/
├── requirements.txt
├── README.md
└── .gitignore

## Data Source

The initial implementation uses the Olist Brazilian E-Commerce Public Dataset.

The dataset contains approximately 100,000 orders from the Brazilian e-commerce marketplace Olist between 2016 and 2018.

The source data contains information about:

- Orders
- Customers
- Products
- Sellers
- Order items
- Payments
- Reviews
- Geolocation
- Product categories

The raw CSV files are stored under:

data/raw/

The Olist dataset is used as the first reference implementation. The pipeline itself is not limited to Olist.

## Data Layers

### Raw

The Raw layer contains the original source files without modification.

Purpose:

- Preserve source data
- Provide reproducible pipeline input
- Keep source data separate from processing logic

### Bronze

The Bronze layer converts raw CSV data into Parquet.

Example datasets:

- customers
- geolocation
- order_items
- order_payments
- order_reviews
- orders
- products
- sellers
- product_category_name_translation

The Bronze ingestion logic is designed to be reusable for additional CSV sources.

### Silver

The Silver layer contains cleaned and standardized datasets.

Transformations include:

- Null filtering
- Type casting
- String normalization
- Date conversion
- Timestamp handling
- Derived metrics
- Numeric normalization
- Basic business-rule preparation

Examples include:

- normalized order status
- standardized customer city and state
- cleaned product categories
- delivery duration
- item totals
- normalized payment fields

### Gold

The Gold layer contains analytics-ready datasets based on a dimensional model.

Current Gold datasets:

- fact_sales
- fact_orders
- dim_customer
- dim_product
- dim_seller
- dim_date

## Star Schema

The analytical model follows a star-schema design.

                       dim_customer
                                                   │
                                                                               │ 1 : *
                                                                                                           ▼
                                                                                                           dim_product ─────────► fact_sales ◄──────── dim_seller
                                                                                                                1 : *                                      1 : *
                                                                                                                                            ▲
                                                                                                                                                                        │ 1 : *
                                                                                                                                                                                                    │
                                                                                                                                                                                                                            dim_date

                                                                                                                                                                                                                            fact_orders also uses the customer and date dimensions.

                                                                                                                                                                                                                            ### fact_sales

                                                                                                                                                                                                                            Grain:

                                                                                                                                                                                                                            One row per order item / product line.

                                                                                                                                                                                                                            Contains:

                                                                                                                                                                                                                            - order_id
                                                                                                                                                                                                                            - order_item_id
                                                                                                                                                                                                                            - customer_key
                                                                                                                                                                                                                            - product_key
                                                                                                                                                                                                                            - seller_key
                                                                                                                                                                                                                            - date_key
                                                                                                                                                                                                                            - order_status
                                                                                                                                                                                                                            - order_purchase_timestamp
                                                                                                                                                                                                                            - price
                                                                                                                                                                                                                            - freight_value
                                                                                                                                                                                                                            - item_total

                                                                                                                                                                                                                            ### fact_orders

                                                                                                                                                                                                                            Grain:

                                                                                                                                                                                                                            One row per order.

                                                                                                                                                                                                                            Contains:

                                                                                                                                                                                                                            - order_id
                                                                                                                                                                                                                            - customer_key
                                                                                                                                                                                                                            - date_key
                                                                                                                                                                                                                            - order_status
                                                                                                                                                                                                                            - purchase timestamps
                                                                                                                                                                                                                            - delivery information
                                                                                                                                                                                                                            - payment information
                                                                                                                                                                                                                            - payment count
                                                                                                                                                                                                                            - payment installments
                                                                                                                                                                                                                            - payment type

                                                                                                                                                                                                                            Payment records are aggregated to the order level before being joined to fact_orders. This prevents one-to-many payment relationships from multiplying order rows.

                                                                                                                                                                                                                            ### Dimensions

                                                                                                                                                                                                                            dim_customer

                                                                                                                                                                                                                            Customer master data with a generated surrogate customer_key.

                                                                                                                                                                                                                            dim_product

                                                                                                                                                                                                                            Product master data with a generated surrogate product_key and derived product volume.

                                                                                                                                                                                                                            dim_seller

                                                                                                                                                                                                                            Seller master data with a generated surrogate seller_key.

                                                                                                                                                                                                                            dim_date

                                                                                                                                                                                                                            Calendar dimension containing date keys and calendar attributes such as year, month, quarter, day, and weekday.

                                                                                                                                                                                                                            ## PySpark Pipeline

                                                                                                                                                                                                                            The complete pipeline consists of 15 processing and validation steps:

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

                                                                                                                                                                                                                            python -m src.pipeline

                                                                                                                                                                                                                            The pipeline provides structured logging including:

                                                                                                                                                                                                                            - Step name
                                                                                                                                                                                                                            - Success/failure status
                                                                                                                                                                                                                            - Execution duration
                                                                                                                                                                                                                            - Pipeline duration
                                                                                                                                                                                                                            - Final pipeline status

                                                                                                                                                                                                                            A successful reference run completes all 15 steps.

                                                                                                                                                                                                                            ## Data Quality

                                                                                                                                                                                                                            Data-quality validation is implemented as an explicit part of the pipeline.

                                                                                                                                                                                                                            ### fact_sales

                                                                                                                                                                                                                            Checks include:

                                                                                                                                                                                                                            - Row count
                                                                                                                                                                                                                            - Duplicate order-item detection
                                                                                                                                                                                                                            - Missing customer keys
                                                                                                                                                                                                                            - Missing product keys
                                                                                                                                                                                                                            - Missing seller keys
                                                                                                                                                                                                                            - Missing date keys
                                                                                                                                                                                                                            - Negative prices
                                                                                                                                                                                                                            - Negative freight values
                                                                                                                                                                                                                            - Item-total consistency

                                                                                                                                                                                                                            The current reference dataset produces approximately 112,650 fact_sales rows.

                                                                                                                                                                                                                            ### fact_orders

                                                                                                                                                                                                                            Checks include:

                                                                                                                                                                                                                            - Row count
                                                                                                                                                                                                                            - Duplicate order IDs
                                                                                                                                                                                                                            - Missing customer keys
                                                                                                                                                                                                                            - Missing date keys
                                                                                                                                                                                                                            - Payment consistency
                                                                                                                                                                                                                            - Payment record counts
                                                                                                                                                                                                                            - Payment installments
                                                                                                                                                                                                                            - Delivery duration
                                                                                                                                                                                                                            - Invalid delivery values

                                                                                                                                                                                                                            The current reference dataset produces approximately 99,441 fact_orders rows.

                                                                                                                                                                                                                            ## Testing

                                                                                                                                                                                                                            The project uses pytest.

                                                                                                                                                                                                                            Run all tests with:

                                                                                                                                                                                                                            pytest

                                                                                                                                                                                                                            The current test suite covers:

                                                                                                                                                                                                                            - Customers transformation
                                                                                                                                                                                                                            - Orders transformation
                                                                                                                                                                                                                            - Order items transformation
                                                                                                                                                                                                                            - Payments transformation
                                                                                                                                                                                                                            - Products transformation
                                                                                                                                                                                                                            - Sellers transformation
                                                                                                                                                                                                                            - Fact sales construction
                                                                                                                                                                                                                            - Fact orders construction
                                                                                                                                                                                                                            - Fact sales quality
                                                                                                                                                                                                                            - Fact orders quality

                                                                                                                                                                                                                            Current test result:

                                                                                                                                                                                                                            10 passed

                                                                                                                                                                                                                            ## DuckDB

                                                                                                                                                                                                                            DuckDB provides the analytical SQL layer on top of the Gold Parquet datasets.

                                                                                                                                                                                                                            The DuckDB database is stored at:

                                                                                                                                                                                                                            data/analytics/analytics.duckdb

                                                                                                                                                                                                                            The database file is intentionally excluded from Git because it can be recreated from the Gold layer.

                                                                                                                                                                                                                            Current analytical views:

                                                                                                                                                                                                                            - analytics_sales
                                                                                                                                                                                                                            - analytics_orders
                                                                                                                                                                                                                            - analytics_monthly_revenue
                                                                                                                                                                                                                            - analytics_product_sales

                                                                                                                                                                                                                            DuckDB reads the Gold Parquet datasets directly.

                                                                                                                                                                                                                            Architecture:

                                                                                                                                                                                                                            Gold Parquet
                                                                                                                                                                                                                                 ↓
                                                                                                                                                                                                                                 DuckDB
                                                                                                                                                                                                                                      ↓
                                                                                                                                                                                                                                      Analytical Views
                                                                                                                                                                                                                                           ↓
                                                                                                                                                                                                                                           SQL / BI

                                                                                                                                                                                                                                           ## Apache Superset

                                                                                                                                                                                                                                           Apache Superset is used as the Linux visualization and dashboard layer.

                                                                                                                                                                                                                                           The repository contains only the project-specific Superset deployment configuration. The complete Apache Superset source repository is not included.

                                                                                                                                                                                                                                           Superset configuration:

                                                                                                                                                                                                                                           superset/
                                                                                                                                                                                                                                           ├── docker-compose.yml
                                                                                                                                                                                                                                           ├── docker/
                                                                                                                                                                                                                                           │   ├── .env.example
                                                                                                                                                                                                                                           │   ├── requirements-local.txt
                                                                                                                                                                                                                                           │   └── superset_config.py
                                                                                                                                                                                                                                           ├── dashboard/
                                                                                                                                                                                                                                           │   └── superset-overview.png
                                                                                                                                                                                                                                           └── README.md

                                                                                                                                                                                                                                           The current deployment uses Apache Superset 6.0.0.

                                                                                                                                                                                                                                           Architecture:

                                                                                                                                                                                                                                           PySpark
                                                                                                                                                                                                                                              ↓
                                                                                                                                                                                                                                              Gold Parquet
                                                                                                                                                                                                                                                 ↓
                                                                                                                                                                                                                                                 DuckDB
                                                                                                                                                                                                                                                    ↓
                                                                                                                                                                                                                                                    Apache Superset
                                                                                                                                                                                                                                                       ↓
                                                                                                                                                                                                                                                       Dashboard

                                                                                                                                                                                                                                                       The Superset container mounts:

                                                                                                                                                                                                                                                       data/analytics → /app/analytics
                                                                                                                                                                                                                                                       data/gold      → /app/gold

                                                                                                                                                                                                                                                       DuckDB view configuration uses:

                                                                                                                                                                                                                                                       DUCKDB_GOLD_ROOT=/app/gold

                                                                                                                                                                                                                                                       The local Superset .env file is intentionally excluded from Git.

                                                                                                                                                                                                                                                       Use superset/docker/.env.example as the configuration template.

                                                                                                                                                                                                                                                       ### Start Superset

                                                                                                                                                                                                                                                       From the project root:

                                                                                                                                                                                                                                                       cd superset
                                                                                                                                                                                                                                                       docker compose up -d

                                                                                                                                                                                                                                                       Check the containers:

                                                                                                                                                                                                                                                       docker compose ps

                                                                                                                                                                                                                                                       Superset is available at:

                                                                                                                                                                                                                                                       http://localhost:8088

                                                                                                                                                                                                                                                       ### Superset Dashboard

                                                                                                                                                                                                                                                       The completed dashboard contains visualizations for:

                                                                                                                                                                                                                                                       - Monthly revenue
                                                                                                                                                                                                                                                       - Monthly order volume
                                                                                                                                                                                                                                                       - Top product categories
                                                                                                                                                                                                                                                       - Revenue by order status

                                                                                                                                                                                                                                                       Dashboard screenshot:

                                                                                                                                                                                                                                                       superset/dashboard/superset-overview.png

                                                                                                                                                                                                                                                       ## Power BI

                                                                                                                                                                                                                                                       Power BI is planned as the Windows visualization layer.

                                                                                                                                                                                                                                                       The intended architecture is:

                                                                                                                                                                                                                                                       PySpark
                                                                                                                                                                                                                                                          ↓
                                                                                                                                                                                                                                                          Gold Parquet
                                                                                                                                                                                                                                                             ↓
                                                                                                                                                                                                                                                             DuckDB
                                                                                                                                                                                                                                                                ↓
                                                                                                                                                                                                                                                                Power BI

                                                                                                                                                                                                                                                                The Linux/Superset environment is the primary implementation and test environment.

                                                                                                                                                                                                                                                                The Windows / Power BI integration is kept separate so that the core PySpark pipeline remains platform-independent.

                                                                                                                                                                                                                                                                ## Running Individual Components

                                                                                                                                                                                                                                                                Bronze ingestion:

                                                                                                                                                                                                                                                                python -m src.ingestion.bronze_loader

                                                                                                                                                                                                                                                                Dimensions:

                                                                                                                                                                                                                                                                python -m src.analytics.dim_customer
                                                                                                                                                                                                                                                                python -m src.analytics.dim_product
                                                                                                                                                                                                                                                                python -m src.analytics.dim_seller
                                                                                                                                                                                                                                                                python -m src.analytics.dim_date

                                                                                                                                                                                                                                                                Facts:

                                                                                                                                                                                                                                                                python -m src.analytics.fact_sales
                                                                                                                                                                                                                                                                python -m src.analytics.fact_orders

                                                                                                                                                                                                                                                                Quality checks:

                                                                                                                                                                                                                                                                python -m src.analytics.fact_sales_quality
                                                                                                                                                                                                                                                                python -m src.analytics.fact_orders_quality

                                                                                                                                                                                                                                                                DuckDB views:

                                                                                                                                                                                                                                                                python -m src.analytics.duckdb_views

                                                                                                                                                                                                                                                                ## Local Development

                                                                                                                                                                                                                                                                Create a Python virtual environment:

                                                                                                                                                                                                                                                                python -m venv .venv

                                                                                                                                                                                                                                                                Activate it:

                                                                                                                                                                                                                                                                source .venv/bin/activate

                                                                                                                                                                                                                                                                Install dependencies:

                                                                                                                                                                                                                                                                pip install -r requirements.txt

                                                                                                                                                                                                                                                                The reference environment uses:

                                                                                                                                                                                                                                                                - Python 3.12
                                                                                                                                                                                                                                                                - PySpark 4.2.0
                                                                                                                                                                                                                                                                - DuckDB 1.5.5
                                                                                                                                                                                                                                                                - pytest

                                                                                                                                                                                                                                                                ## Design Principles

                                                                                                                                                                                                                                                                ### Separation of concerns

                                                                                                                                                                                                                                                                Ingestion, transformation, analytics, validation, and visualization are separated into dedicated modules.

                                                                                                                                                                                                                                                                ### Layered data architecture

                                                                                                                                                                                                                                                                Bronze, Silver, and Gold layers provide clear separation between source data, cleaned data, and analytics-ready data.

                                                                                                                                                                                                                                                                ### Reusable transformations

                                                                                                                                                                                                                                                                Transformation functions operate on Spark DataFrames and are designed to be reusable.

                                                                                                                                                                                                                                                                ### Dimensional modeling

                                                                                                                                                                                                                                                                The Gold layer follows a star schema suitable for BI and analytical workloads.

                                                                                                                                                                                                                                                                ### Surrogate keys

                                                                                                                                                                                                                                                                Dimensions use surrogate keys to decouple the analytical model from source-system identifiers.

                                                                                                                                                                                                                                                                ### Data quality

                                                                                                                                                                                                                                                                Data-quality validation is implemented as part of the pipeline rather than as a separate manual process.

                                                                                                                                                                                                                                                                ### Separation of compute and BI

                                                                                                                                                                                                                                                                PySpark handles ETL and data preparation.

                                                                                                                                                                                                                                                                DuckDB provides analytical SQL access.

                                                                                                                                                                                                                                                                Superset and Power BI provide visualization.

                                                                                                                                                                                                                                                                ### Extensibility

                                                                                                                                                                                                                                                                The architecture is designed to support additional data sources and ingestion mechanisms.

                                                                                                                                                                                                                                                                ## Future Extensions

                                                                                                                                                                                                                                                                Potential future extensions include:

                                                                                                                                                                                                                                                                - Additional CSV sources
                                                                                                                                                                                                                                                                - JSON ingestion
                                                                                                                                                                                                                                                                - Parquet ingestion
                                                                                                                                                                                                                                                                - SQL database connectors
                                                                                                                                                                                                                                                                - REST API ingestion
                                                                                                                                                                                                                                                                - Kafka / streaming ingestion
                                                                                                                                                                                                                                                                - Incremental processing
                                                                                                                                                                                                                                                                - Partitioning strategies
                                                                                                                                                                                                                                                                - Schema validation
                                                                                                                                                                                                                                                                - Data contracts
                                                                                                                                                                                                                                                                - Slowly Changing Dimensions
                                                                                                                                                                                                                                                                - Advanced Spark SQL analytics
                                                                                                                                                                                                                                                                - Window-function based KPIs
                                                                                                                                                                                                                                                                - Additional BI dashboards
                                                                                                                                                                                                                                                                - Power BI integration
                                                                                                                                                                                                                                                                - CI/CD
                                                                                                                                                                                                                                                                - Cloud object storage such as S3
                                                                                                                                                                                                                                                                - Workflow orchestration

                                                                                                                                                                                                                                                                ## Git Hygiene

                                                                                                                                                                                                                                                                The following local/generated files are intentionally excluded from Git:

                                                                                                                                                                                                                                                                - Superset .env files
                                                                                                                                                                                                                                                                - Superset local state
                                                                                                                                                                                                                                                                - DuckDB database files
                                                                                                                                                                                                                                                                - Generated analytics data
                                                                                                                                                                                                                                                                - Other runtime artifacts

                                                                                                                                                                                                                                                                The repository should contain project configuration and source code, not local credentials or runtime state.

                                                                                                                                                                                                                                                                ## Status

                                                                                                                                                                                                                                                                Current implementation:

                                                                                                                                                                                                                                                                - PySpark ETL pipeline: complete
                                                                                                                                                                                                                                                                - Bronze layer: complete
                                                                                                                                                                                                                                                                - Silver layer: complete
                                                                                                                                                                                                                                                                - Gold star schema: complete
                                                                                                                                                                                                                                                                - fact_sales: complete
                                                                                                                                                                                                                                                                - fact_orders: complete
                                                                                                                                                                                                                                                                - Data-quality validation: complete
                                                                                                                                                                                                                                                                - pytest test suite: complete
                                                                                                                                                                                                                                                                - DuckDB analytics layer: complete
                                                                                                                                                                                                                                                                - Apache Superset integration: complete
                                                                                                                                                                                                                                                                - Superset dashboard: complete
                                                                                                                                                                                                                                                                - Power BI / Windows integration: planned final validation

                                                                                                                                                                                                                                                                The Linux-based analytics stack is currently the primary reference implementation.-superset-volumes:
  &superset-volumes
  - ./docker:/app/docker
  - superset_home:/app/superset_home
  - ../data/analytics:/app/analytics
  - ../data/gold:/app/gold:ro


