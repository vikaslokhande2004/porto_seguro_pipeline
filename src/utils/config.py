import os
from dotenv import load_dotenv
from pathlib import Path


def load_config(env: str = None):
    if env is None:
        env = os.getenv("APP_ENV", "dev ")
    
    config_file = Path(f"config/.env.{env}")

    if not config_file.exists():
        raise FileNotFoundError(f"Config not found: {config_file}")
    
    load_dotenv(config_file, override=True)

    cfg = {
        "project": os.getenv("PROJECT_NAME"),
        "env": os.getenv("ENV"),
        "raw_path": os.getenv("RAW_PATH"),
        "processed": os.getenv("PROCESSED_PATH"),
        "reports": os.getenv("REPORTS_PATH"),
        "logs": os.getenv("LOGS_PATH"),
        "spark_app": os.getenv("SPARK_APP_NAME"),
        "spark_master": os.getenv("SPARK_MASTER"),
        "spark_parts": int(os.getenv("SPARK_SHUFFLE_PARTITIONS", 8)),
        "spark_driver": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
        "loan_file": os.getenv("LOAN_FILE"),
        "fraud_file": os.getenv("FRAUD_FILE"),
        "complaints_file": os.getenv("COMPLAINTS_FILE"),
        "fdic_file": os.getenv("FDIC_FILE"),
        "sample_size": int(os.getenv("LOAN_SAMPLE_SIZE", 500000)),
        "full_run": os.getenv("RUN_FULL_DATASET", "false").lower() == "true",
    }

    return cfg