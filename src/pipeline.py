import logging
import subprocess
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

PIPELINE_STEPS = [
    # --------------------------------------------------
    # Bronze
    # --------------------------------------------------
    ("Bronze ingestion", "src.ingestion.bronze_loader"),
    # --------------------------------------------------
    # Silver transformations
    # --------------------------------------------------
    ("Orders Silver", "src.transformations.orders_silver"),
    ("Order Items Silver", "src.transformations.order_items_silver"),
    ("Order Payments Silver", "src.transformations.order_payments_silver"),
    ("Customers Silver", "src.transformations.customers_silver"),
    ("Products Silver", "src.transformations.products_silver"),
    ("Sellers Silver", "src.transformations.sellers_silver"),
    # --------------------------------------------------
    # Gold dimensions
    # --------------------------------------------------
    ("Customer Dimension", "src.analytics.dim_customer"),
    ("Product Dimension", "src.analytics.dim_product"),
    ("Seller Dimension", "src.analytics.dim_seller"),
    ("Date Dimension", "src.analytics.dim_date"),
    # --------------------------------------------------
    # Gold facts
    # --------------------------------------------------
    ("Fact Sales", "src.analytics.fact_sales"),
    ("Fact Orders", "src.analytics.fact_orders"),
    # --------------------------------------------------
    # Quality checks
    # --------------------------------------------------
    ("Fact Sales Quality", "src.analytics.fact_sales_quality"),
    ("Fact Orders Quality", "src.analytics.fact_orders_quality"),
]


def run_step(name: str, module: str) -> float:
    logger.info("Starting: %s", name)
    logger.info("Module: %s", module)

    start_time = time.perf_counter()

    result = subprocess.run(
        [sys.executable, "-m", module],
        check=False,
    )

    duration = time.perf_counter() - start_time

    if result.returncode != 0:
        logger.error(
            "FAILED: %s | duration: %.2f seconds",
            name,
            duration,
        )
        raise RuntimeError(
            f"Pipeline step failed: {name} (exit code {result.returncode})"
        )

    logger.info(
        "SUCCESS: %s | duration: %.2f seconds",
        name,
        duration,
    )

    return duration


def main():
    pipeline_start = time.perf_counter()

    logger.info("=" * 70)
    logger.info("PySpark Analytics Pipeline")
    logger.info("=" * 70)

    results = []

    try:
        for name, module in PIPELINE_STEPS:
            duration = run_step(name, module)

            results.append(
                {
                    "name": name,
                    "status": "SUCCESS",
                    "duration": duration,
                }
            )

    except RuntimeError as exc:
        logger.error(str(exc))

        logger.info("=" * 70)
        logger.info("PIPELINE FAILED")
        logger.info("=" * 70)

        for result in results:
            logger.info(
                "✓ %-30s %s",
                result["name"],
                result["status"],
            )

        raise

    total_duration = time.perf_counter() - pipeline_start

    logger.info("")
    logger.info("=" * 70)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 70)

    for result in results:
        logger.info(
            "✓ %-30s %s | %.2f sec",
            result["name"],
            result["status"],
            result["duration"],
        )

    logger.info("-" * 70)
    logger.info(
        "Steps:    %d/%d",
        len(results),
        len(PIPELINE_STEPS),
    )
    logger.info(
        "Duration: %.2f minutes",
        total_duration / 60,
    )
    logger.info("Status:   SUCCESS")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
