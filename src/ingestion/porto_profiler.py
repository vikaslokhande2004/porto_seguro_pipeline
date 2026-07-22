import pandas as pd
import numpy as np
import re
from src.utils.config import load_config
from src.utils.logger import get_logger

cfg = load_config()
logger = get_logger("porto_profiler",cfg["env"])

# ─────────────────────────────────────────
# PROBLEM 1: -1 is not a real value.
# It must become an actual NULL before any
# aggregate function (AVG, SUM) is trusted.
# ─────────────────────────────────────────

def convert_sentinel_nulls(df: pd.DataFrame, sentinel=-1) -> pd.DataFrame:
    numeric_cols = df.select_dtypes(include=["int64","float64"])

    for col in numeric_cols:
        if col in ("id","target"):
            continue
        df[col] = df[col].replace(sentinel,np.nan)

    return df

# ─────────────────────────────────────────
# PROBLEM 2: You need to classify 38 unnamed
# columns into feature groups programmatically,
# not by memorising the list.
# ─────────────────────────────────────────

def classify_columns(df: pd.DataFrame) -> dict:
    groups = {
        "ind": [],
        "reg": [],
        "car": [],
        "calc": [],
        "other": []
    }

    for col in df.columns:
        matched = False

        for prefix in ["ind", "reg", "car", "calc"]:
            if (
                f"_{prefix}_" in col
                or col.startswith(f"ps_{prefix}_")
            ):
                groups[prefix].append(col)
                matched = True
                break

        # This must be outside the prefix loop
        if not matched and col not in ("id", "target"):
            groups["other"].append(col)

    return groups

# ─────────────────────────────────────────
# PROBLEM 3: Class imbalance must be reported
# BEFORE any modeling or aggregate reporting,
# or every downstream percentage is misleading.
# ─────────────────────────────────────────

def imbalance_report(df: pd.DataFrame, target_col="target") -> dict:
    counts = df[target_col].value_counts()
    total = len(df)
    return {
        "total_rows":     int(total),
        "positive_count": int(counts.get(1,0)),
        "negative_count": int(counts.get(0,0)),
        "positive_rate_pct": round(
            counts.get(1,0)/total*100, 3),
        "imbalance_ratio": round(
            counts.get(0,0) /
            max(counts.get(1,0),1), 1)
    }

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

def main():
    filepath = f"{cfg['bronze_path'] if 'bronze_path' in cfg else cfg['raw_path']}/porto_claims.csv"
    logger.info(f"Reading: {filepath}")

    df = pd.read_csv(filepath)
    logger.info(f"Loaded {len(df):,} rows "
               f"x {df.shape[1]} columns")

    groups = classify_columns(df)
    for g, cols in groups.items():
        logger.info(
            f"Column group '{g}': "
            f"{len(cols)} columns")

    df = convert_sentinel_nulls(df)

    null_pct = (df.isnull().sum() /
               len(df) * 100).round(2)
    high_null_cols = null_pct[
        null_pct > 20].to_dict()
    logger.info(
        f"Columns with >20% nulls after "
        f"sentinel conversion: {high_null_cols}")

    imb = imbalance_report(df)
    logger.info(f"Imbalance report: {imb}")
    logger.warning(
        f"Positive rate is "
        f"{imb['positive_rate_pct']}%. Any "
        f"aggregate report on 'claim rate' "
        f"downstream must state this baseline "
        f"or it will be misread by stakeholders.")

    out = f"{cfg.get('silver_path','./silver')}/porto_claims_bronze_clean.csv"
    df.to_csv(out, index=False)
    logger.info(f"Saved: {out}")

if __name__ == "__main__":
    main()
