import os
from dotenv import load_dotenv
from pathlib import Path


def load_config(env: str = None):

    # Get environment name
    if env is None:
        env = os.getenv("APP_ENV", "dev").strip()

    # Build config file path
    config_file = Path(
        f"config/.env.{env}"
    )

    # Check config exists
    if not config_file.exists():
        raise FileNotFoundError(
            f"Config not found: {config_file}"
        )

    # Load environment variables
    load_dotenv(
        config_file,
        override=True
    )

    cfg = {
        # ─────────────────────────────
        # Project
        # ─────────────────────────────

        "project": os.getenv(
            "PROJECT_NAME"
        ),

        "env": os.getenv(
            "ENV",
            env
        ),

        # ─────────────────────────────
        # Medallion Architecture
        # ─────────────────────────────

        "bronze_path": os.getenv(
            "BRONZE_PATH",
            "./bronze"
        ),

        "silver_path": os.getenv(
            "SILVER_PATH",
            "./silver"
        ),

        "gold_path": os.getenv(
            "GOLD_PATH",
            "./gold"
        ),

        # ─────────────────────────────
        # Logging
        # ─────────────────────────────

        "logs_path": os.getenv(
            "LOGS_PATH",
            "./logs"
        ),

        # ─────────────────────────────
        # Spark
        # ─────────────────────────────

        "spark_app": os.getenv(
            "SPARK_APP_NAME"
        ),

        "spark_master": os.getenv(
            "SPARK_MASTER"
        ),

        "spark_parts": int(
            os.getenv(
                "SPARK_SHUFFLE_PARTITIONS",
                8
            )
        ),

        "spark_driver": os.getenv(
            "SPARK_DRIVER_MEMORY",
            "4g"
        ),

        # ─────────────────────────────
        # Porto Seguro datasets
        # ─────────────────────────────

        "claims_file": os.getenv(
            "CLAIMS_FILE",
            "porto_claims.csv"
        ),

        "dim_source_file": os.getenv(
            "DIM_SOURCE_FILE",
            "dim_source.csv"
        ),

        # ─────────────────────────────
        # Backward compatibility
        # Optional: keep these if other
        # reused Capstone 2 scripts use them
        # ─────────────────────────────

        "raw_path": os.getenv(
            "RAW_PATH",
            os.getenv(
                "BRONZE_PATH",
                "./bronze"
            )
        ),

        "processed": os.getenv(
            "PROCESSED_PATH",
            os.getenv(
                "SILVER_PATH",
                "./silver"
            )
        ),

        "reports": os.getenv(
            "REPORTS_PATH",
            os.getenv(
                "GOLD_PATH",
                "./gold"
            )
        ),

        # ─────────────────────────────
        # Legacy NFC fields
        # ─────────────────────────────

        "loan_file": os.getenv(
            "LOAN_FILE"
        ),

        "fraud_file": os.getenv(
            "FRAUD_FILE"
        ),

        "complaints_file": os.getenv(
            "COMPLAINTS_FILE"
        ),

        "fdic_file": os.getenv(
            "FDIC_FILE"
        ),

        "sample_size": int(
            os.getenv(
                "LOAN_SAMPLE_SIZE",
                500000
            )
        ),

        "full_run": os.getenv(
            "RUN_FULL_DATASET",
            "false"
        ).lower() == "true",
    }

    return cfg