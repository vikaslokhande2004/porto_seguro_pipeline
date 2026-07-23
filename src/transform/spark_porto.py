"""
Porto Seguro Insurance Claims
Bronze -> Silver PySpark Transformation

Pipeline:
    1. Create Spark Session
    2. Load Bronze CSV
    3. Check Schema Drift
    4. Replace Sentinel Values (-1) with NULL
    5. Calculate NULL Density
    6. Calculate Data Completeness
    7. Create Derived Risk Bucket
    8. Cache / Persist DataFrame
    9. Run Aggregation Demo
    10. Write Partitioned Silver Parquet

Output:
    silver/porto_claims_silver/

Partition:
    risk_bucket
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.sql import functions as F

import os
import time
import builtins

from dotenv import load_dotenv

from src.utils.config import load_config
from src.utils.logger import get_logger


# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────

cfg = load_config("dev")

logger = get_logger("spark_porto", cfg["env"])


# ─────────────────────────────────────────
# PATH CONFIGURATION
# ─────────────────────────────────────────

BRONZE_PATH = "bronze/porto_claims.csv"

SILVER_PATH = "silver/porto_claims_silver"


# ─────────────────────────────────────────
# EXPECTED PORTO SEGURO COLUMNS
# ─────────────────────────────────────────
#
# These columns are used for schema drift
# detection.
#
# If your dataset contains additional columns,
# they will be reported as NEW columns.
#
# If any of these expected columns are missing,
# they will be reported as MISSING columns.
#
# The pipeline will NOT fail because of drift.
# ─────────────────────────────────────────

EXPECTED_COLUMNS = {
    "id",
    "target",
    # ─────────────────────────────────────
    # ps_ind columns
    # ─────────────────────────────────────
    "ps_ind_01",
    "ps_ind_02_cat",
    "ps_ind_03",
    "ps_ind_04_cat",
    "ps_ind_05_cat",
    "ps_ind_06_bin",
    "ps_ind_07_bin",
    "ps_ind_08_bin",
    "ps_ind_09_bin",
    "ps_ind_10_bin",
    "ps_ind_11_bin",
    "ps_ind_12_bin",
    "ps_ind_13_bin",
    # ─────────────────────────────────────
    # ps_car categorical columns
    # ─────────────────────────────────────
    "ps_car_01_cat",
    "ps_car_02_cat",
    "ps_car_03_cat",
    "ps_car_04_cat",
    "ps_car_05_cat",
    "ps_car_06_cat",
    "ps_car_07_cat",
    "ps_car_08_cat",
    "ps_car_09_cat",
    "ps_car_10_cat",
    "ps_car_11_cat",
    # ─────────────────────────────────────
    # ps_car numeric columns
    # ─────────────────────────────────────
    "ps_car_01",
    "ps_car_02",
    "ps_car_03",
    "ps_car_04",
    "ps_car_05",
    "ps_car_06",
    "ps_car_07",
    "ps_car_08",
    "ps_car_09",
    "ps_car_10",
    "ps_car_11",
    "ps_car_12",
    "ps_car_13",
    "ps_car_14",
    "ps_car_15",
    # ─────────────────────────────────────
    # ps_calc columns
    # ─────────────────────────────────────
    "ps_calc_01",
    "ps_calc_02",
    "ps_calc_03",
    "ps_calc_04",
    "ps_calc_05",
    "ps_calc_06",
    "ps_calc_07",
    "ps_calc_08",
    "ps_calc_09",
    "ps_calc_10",
    "ps_calc_11",
    "ps_calc_12",
    "ps_calc_13",
    "ps_calc_14",
}


# ─────────────────────────────────────────
# STEP 4.1 — CREATE SPARK SESSION
# ─────────────────────────────────────────


def create_spark(cfg: dict) -> SparkSession:

    spark = (
        SparkSession.builder.appName(cfg["spark_app"])
        .master(cfg["spark_master"])
        .config("spark.sql.shuffle.partitions", cfg["spark_parts"])
        .config("spark.driver.memory", cfg["spark_driver"])
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Spark started: {cfg['spark_master']}")

    logger.info("Spark UI: http://localhost:4040")

    return spark


# ─────────────────────────────────────────
# STEP 4.2 — LOAD WITH SCHEMA DRIFT CHECK
# ─────────────────────────────────────────


def load_with_drift_check(spark, path, expected_cols: set):

    logger.info(f"Loading Bronze data from: {path}")

    df = spark.read.option("header", True).option("inferSchema", True).csv(path)

    # Actual columns received from source
    actual_cols = set(df.columns)

    # Expected columns missing from source
    missing = expected_cols - actual_cols

    # New unexpected columns
    new_cols = actual_cols - expected_cols

    # ─────────────────────────────────────
    # LOG MISSING COLUMNS
    # ─────────────────────────────────────

    if missing:

        logger.warning(f"SCHEMA DRIFT: expected columns " f"missing: {sorted(missing)}")

    # ─────────────────────────────────────
    # LOG NEW COLUMNS
    # ─────────────────────────────────────

    if new_cols:

        logger.warning(
            f"SCHEMA DRIFT: unexpected new "
            f"columns found: {sorted(new_cols)}. "
            f"These will be carried through "
            f"but not used downstream until "
            f"reviewed."
        )

    # ─────────────────────────────────────
    # NO DRIFT
    # ─────────────────────────────────────

    if not missing and not new_cols:

        logger.info("Schema drift check passed. " "No schema changes detected.")

    logger.info(f"Actual column count: {len(actual_cols)}")

    return df


# ─────────────────────────────────────────
# STEP 4.3 — REPLACE SENTINEL WITH NULL
# ─────────────────────────────────────────


def replace_sentinel_with_null(df):
    """
    Replace numeric sentinel value -1 with NULL.

    Excludes:
        id
        target
    """

    numeric_cols = [
        field.name
        for field in df.schema.fields
        if field.dataType in (IntegerType(), DoubleType(), LongType())
        and field.name not in ("id", "target")
    ]

    logger.info(f"Numeric columns for sentinel " f"replacement: {len(numeric_cols)}")

    for c in numeric_cols:

        df = df.withColumn(c, when(col(c) == -1, None).otherwise(col(c)))

    logger.info("Sentinel value replacement completed.")

    return df


# ─────────────────────────────────────────
# STEP 4.4 — ADD NULL DENSITY COLUMN
# ─────────────────────────────────────────


def add_null_density_column(df, cols):

    """
    Add data quality metrics.

    null_field_count:
        Number of NULL values across selected columns.

    data_completeness_pct:
        Percentage of non-NULL values across selected columns.
    """

    if not cols:
        logger.warning(
            "No columns available for "
            "null density calculation."
        )

        return (
            df
            .withColumn(
                "null_field_count",
                lit(0)
            )
            .withColumn(
                "data_completeness_pct",
                lit(100.0)
            )
        )

    null_check_expr = builtins.sum(
        [
            when(
                col(c).isNull(),
                1
            ).otherwise(0)

            for c in cols
        ]
    )

    return (
        df
        .withColumn(
            "null_field_count",
            null_check_expr
        )
        .withColumn(
            "data_completeness_pct",
            round(
                (
                    lit(len(cols))
                    - col("null_field_count")
                )
                * 100.0
                / lit(len(cols)),
                1
            )
        )
    )


# ─────────────────────────────────────────
# STEP 4.5 — ADD DERIVED RISK BUCKET
# ─────────────────────────────────────────


def add_risk_bucket(df):
    """
    Create risk bucket based on
    data completeness percentage.

    LOW_RISK:
        >= 90%

    MEDIUM_RISK:
        >= 70% and < 90%

    HIGH_RISK:
        < 70%
    """

    df = df.withColumn(
        "risk_bucket",
        when(col("data_completeness_pct") >= 90, "LOW_RISK")
        .when(col("data_completeness_pct") >= 70, "MEDIUM_RISK")
        .otherwise("HIGH_RISK"),
    )

    return df


# ─────────────────────────────────────────
# STEP 4.6 — CACHE AND PERSIST DEMO
# ─────────────────────────────────────────


def demonstrate_cache_value(df):

    logger.info("=== PORTO CACHE PERFORMANCE TEST ===")

    # ─────────────────────────────────────
    # WITHOUT CACHE
    # ─────────────────────────────────────

    logger.info("Running aggregation without cache...")

    t1 = time.time()

    uncached_count = df.groupBy("risk_bucket").count().count()

    t2 = time.time()

    logger.info(
        f"Risk bucket count (UNCACHED): " f"{uncached_count} | " f"Time: {t2 - t1:.2f}s"
    )

    # ─────────────────────────────────────
    # CACHE DATAFRAME
    # ─────────────────────────────────────

    logger.info("Persisting DataFrame " "with MEMORY_AND_DISK...")

    df.persist(StorageLevel.MEMORY_AND_DISK)

    # Trigger materialisation
    df.count()

    logger.info("DataFrame cached successfully.")

    # ─────────────────────────────────────
    # WITH CACHE
    # ─────────────────────────────────────

    t1 = time.time()

    cached_count = df.groupBy("risk_bucket").count().count()

    t2 = time.time()

    logger.info(
        f"Risk bucket count (CACHED): " f"{cached_count} | " f"Time: {t2 - t1:.2f}s"
    )

    logger.info("Cache performance comparison completed.")

    return df


# ─────────────────────────────────────────
# STEP 4.7 — MAIN SILVER PIPELINE
# ─────────────────────────────────────────


def main():

    logger.info("==========================================")

    logger.info("Starting Porto Seguro " "Bronze → Silver Pipeline")

    logger.info("==========================================")

    # ─────────────────────────────────────
    # CREATE SPARK SESSION
    # ─────────────────────────────────────

    spark = create_spark(cfg)

    try:

        # ─────────────────────────────────
        # STEP 1 — LOAD BRONZE
        # ─────────────────────────────────

        df = load_with_drift_check(spark, BRONZE_PATH, EXPECTED_COLUMNS)

        logger.info(f"Bronze columns loaded: " f"{len(df.columns)}")

        # ─────────────────────────────────
        # STEP 2 — COUNT INPUT ROWS
        # ─────────────────────────────────

        input_count = df.count()

        logger.info(f"Bronze input rows: " f"{input_count:,}")

        # ─────────────────────────────────
        # STEP 3 — SENTINEL TO NULL
        # ─────────────────────────────────

        df = replace_sentinel_with_null(df)

        # ─────────────────────────────────
        # STEP 4 — COMPLETENESS COLUMNS
        # ─────────────────────────────────

        completeness_cols = [c for c in df.columns if c not in ("id", "target")]

        logger.info(
            f"Calculating data completeness "
            f"across {len(completeness_cols)} "
            f"columns."
        )

        df = add_null_density_column(df, completeness_cols)

        # ─────────────────────────────────
        # STEP 5 — RISK BUCKET
        # ─────────────────────────────────

        df = add_risk_bucket(df)

        logger.info("Derived risk_bucket column created.")

        # ─────────────────────────────────
        # STEP 6 — CACHE DEMONSTRATION
        # ─────────────────────────────────

        df = demonstrate_cache_value(df)

        # ─────────────────────────────────
        # STEP 7 — SHOW RISK SUMMARY
        # ─────────────────────────────────

        logger.info("Risk bucket distribution:")

        (df.groupBy("risk_bucket").count().orderBy("risk_bucket").show(truncate=False))

        # ─────────────────────────────────
        # STEP 8 — WRITE SILVER PARQUET
        # ─────────────────────────────────

        logger.info(f"Writing Silver Parquet to: " f"{SILVER_PATH}")

        (df.write.mode("overwrite").partitionBy("risk_bucket").parquet(SILVER_PATH))

        logger.info("Silver Parquet successfully written.")

        logger.info("Output partition column: " "risk_bucket")

        logger.info(f"Output path: {SILVER_PATH}")

        # ─────────────────────────────────
        # STEP 9 — RELEASE CACHE
        # ─────────────────────────────────

        df.unpersist()

        logger.info("DataFrame cache released.")

        logger.info("==========================================")

        logger.info("Porto Seguro Bronze → Silver " "Pipeline Completed Successfully")

        logger.info("==========================================")

    except Exception as e:

        logger.exception("Porto Seguro Silver pipeline failed.")

        raise e

    finally:

        # ─────────────────────────────────
        # STOP SPARK
        # ─────────────────────────────────

        spark.stop()

        logger.info("Spark session stopped.")


# ─────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────

if __name__ == "__main__":

    main()



'''
Bronze CSV
    │
    ▼
load_with_drift_check()
    │
    ├── Missing columns → WARNING
    └── New columns     → WARNING
    │
    ▼
replace_sentinel_with_null()
    │
    └── -1 → NULL
    │
    ▼
add_null_density_column()
    │
    ├── null_field_count
    └── data_completeness_pct
    │
    ▼
risk_bucket
    │
    ├── >= 90% → LOW_RISK
    ├── >= 70% → MEDIUM_RISK
    └── < 70%  → HIGH_RISK
    │
    ▼
CACHE / PERSIST
    │
    ▼
Aggregation Demo
    │
    ▼
Silver Parquet
    │
    ├── risk_bucket=LOW_RISK/
    ├── risk_bucket=MEDIUM_RISK/
    └── risk_bucket=HIGH_RISK/
'''