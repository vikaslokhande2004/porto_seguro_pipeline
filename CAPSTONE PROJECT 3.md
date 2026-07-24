━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATA ENGINEERING — KODE-X
CAPSTONE PROJECT 3
Insurance Dimensional Analytics Pipeline
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Project Title:
PORTO SEGURO — POLICY RISK & CLAIMS
DIMENSIONAL WAREHOUSE

Consulting Company: KloudOcean IT Services
Client: Porto Seguro (simulated) — Brazil's
        third-largest auto and home insurer

WHY THIS CAPSTONE IS DIFFERENT FROM 1 AND 2:

Capstones 1 (Olist) and 2 (NFC Banking) taught
you the full pipeline motion: ingest, clean,
transform, analyse, orchestrate. You now do
that motion without thinking about it.

This capstone assumes that motion and spends
its weight on FOUR things that research into
2026 hiring shows are the actual gaps between
"can write PySpark" and "gets hired":

1. DIMENSIONAL MODELING — star schema,
   fact/dimension tables, Slowly Changing
   Dimensions. Roughly one in three data
   engineering interview loops in 2026 include
   a dedicated data modeling round, and it is
   the round most candidates fail even when
   their SQL is strong. This has never been
   formally taught in KODE-X. It is now the
   spine of this entire project.

2. THE MODERN TRANSFORMATION LAYER — Dataform
   (Google's native tool, free inside BigQuery)
   AND dbt (the industry-standard, warehouse-
   agnostic version most job descriptions name
   directly). You build the same models in both,
   so you can walk into a Snowflake shop or a
   BigQuery shop and not miss a step.

3. MEDALLION ARCHITECTURE VOCABULARY — Bronze,
   Silver, Gold. You already do this pattern in
   Capstones 1 and 2 as raw/processed/reports.
   This time the folders and the language match
   what interviewers actually say.

4. A LIGHT TOUCH OF AI-AUGMENTED INGESTION —
   using an LLM to turn messy free-text notes
   into structured fields as one pipeline stage.
   This is a real, growing pattern in 2026 data
   engineering, not a detour into building an
   AI product. One exercise. Contained.

Everything else — Git, config.env, logging,
PySpark cache/persist, shell scripting, Airflow —
you already own from Capstones 1 and 2. Reuse
your utilities. Rebuilding them a third time
teaches nothing new; a real engineer would
reuse their own library too.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATASETS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DATASET 1 — Porto Seguro Safe Driver Prediction
Used for: Bronze/Silver ingestion at real scale
URL:
https://www.kaggle.com/c/
porto-seguro-safe-driver-prediction

595,212 rows, 38 columns. Column names are
grouped by prefix: ps_ind_*, ps_reg_*, ps_car_*,
ps_calc_*, with _bin and _cat suffixes marking
binary and categorical fields. -1 means missing.
Target column: binary, did this policyholder
file a claim next year. Severely imbalanced
(~3.6% positive class) — this alone forces a
real conversation about sampling and reporting
bias in Phase 3.

DATASET 2 — Auto Insurance Claims Data
Used for: dimensional model, star schema
URL:
https://www.kaggle.com/datasets/buntyshah/
auto-insurance-claims-data

~1,000 rows. Columns include: policy_number,
policy_bind_date, policy_state, policy_csl,
policy_deductable, policy_annual_premium,
umbrella_limit, insured_zip, insured_sex,
insured_education_level, insured_occupation,
insured_hobbies, insured_relationship,
incident_date, incident_type, collision_type,
incident_severity, incident_state, incident_city,
number_of_vehicles_involved, auto_make,
auto_model, auto_year, total_claim_amount,
injury_claim, property_claim, vehicle_claim,
fraud_reported.

This is small on purpose. Star schema design is
a modeling exercise, not a scale exercise — you
want to see the whole shape of the data while
you design the fact and dimension tables.

DATASET 3 — Constructed teaching data (2 items)

Real, freely-licensed, multi-snapshot enterprise
dimension data and real adjuster free-text notes
do not exist in public datasets — insurers do not
release either for privacy and competitive
reasons. This is a genuine, permanent constraint
in the real industry, not a shortcut here. Every
SCD Type 2 tutorial in existence teaches the
technique the same way: take a real base table,
hand-construct two or three later "snapshots"
showing a realistic change, then build the
history table from the differences.

3a. Policyholder snapshot files (you will build
    these in Phase 5) — three CSV snapshots of
    the same 30 policyholders taken 90 days apart,
    with deliberate, realistic changes: a handful
    of address changes, a few relationship status
    changes, one or two policy tier upgrades.

3b. Adjuster notes microdataset (you will build
    this in Phase 7) — 15 short, realistic free-
    text claim notes for the LLM extraction
    exercise. Provided in full below so you are
    not fabricating insurance-sounding text from
    scratch.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 1 — SETUP
Reuse from Capstone 1/2, do not rebuild
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

mkdir porto_seguro_pipeline
cd porto_seguro_pipeline

mkdir -p bronze silver gold
mkdir -p src/{ingestion,transform,models,utils}
mkdir -p sql/{dimensional,dataform,dbt,analytics}
mkdir -p dags scripts config logs archive
mkdir -p notebooks

Note the top-level folders: bronze, silver, gold.
Not raw/processed/reports this time. Same idea,
industry vocabulary. Say it out loud when you
talk about this project — "the silver layer",
not "the processed folder."

Copy src/utils/logger.py and src/utils/config.py
directly from the NFC Banking pipeline. Same
files, same interface. Update only the values
inside config/.env.dev:

PROJECT_NAME=porto_seguro_pipeline
ENV=dev
BRONZE_PATH=./bronze
SILVER_PATH=./silver
GOLD_PATH=./gold
LOGS_PATH=./logs
SPARK_APP_NAME=PortoSeguro_Pipeline
SPARK_MASTER=local[*]
CLAIMS_FILE=train.csv
DIM_SOURCE_FILE=insurance_claims.csv

git init
git checkout -b develop
# reuse your .gitignore from Capstone 2 as-is

git add .
git commit -m "chore: bootstrap porto seguro
  pipeline, reusing capstone 2 utilities"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 2 — LINUX PROFILING
Same tools, different questions this time
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

cp ~/Downloads/train.csv bronze/porto_claims.csv
cp ~/Downloads/insurance_claims.csv
   bronze/dim_source.csv

Reuse column_inventory.sh from Capstone 2
unchanged. Run it against porto_claims.csv.

Because every column is anonymized, the
inventory output IS the investigation this time.
You cannot look at a column name and know what
it means. You have to infer it from:

1. Task 2.1 — Group columns by prefix

   head -1 bronze/porto_claims.csv | tr ',' '\n' |
   sed 's/_[0-9]*$//' | sort | uniq -c | sort -rn

   Count how many ind_, reg_, car_, calc_ columns
   exist. This is your first real schema-reading
   exercise with zero semantic hints — exactly
   what "read someone else's schema cleanly" means
   in a hiring manager's actual test criteria.

2. Task 2.2 — Confirm -1 as the null marker

   awk -F',' 'NR>1 {
       for(i=1;i<=NF;i++)
           if($i=="-1") count++
   } END {print "Total -1 occurrences:", count}'
   bronze/porto_claims.csv

3. Task 2.3 — Confirm class imbalance from
   the command line before you ever open Python

   cut -d',' -f2 bronze/porto_claims.csv |
   tail -n +2 | sort | uniq -c

   Write down the ratio. You will need it in
   Phase 3 to justify your handling decision.

4. Task 2.4 — Profile the dimensional source

   ./scripts/column_inventory.sh
       bronze/dim_source.csv

   This file has readable column names. Compare
   the experience of profiling this file against
   profiling porto_claims.csv. That contrast is
   the lesson: real projects give you both kinds
   in the same week.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 3 — PYTHON: BRONZE INGESTION
Topics: anonymized schema handling, imbalance-
        aware profiling, reused DQ scoring
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Create: src/ingestion/porto_profiler.py

Reuse the safe_read(), calculate_dq_score()
pattern from the NFC Banking loan_profiler.py
directly. What is new is the cleaning logic
required by an anonymized schema.

import pandas as pd
import numpy as np
import re
from src.utils.config import load_config
from src.utils.logger import get_logger

cfg    = load_config("dev")
logger = get_logger("porto_profiler", cfg["env"])

# ─────────────────────────────────────────
# PROBLEM 1: -1 is not a real value.
# It must become an actual NULL before any
# aggregate function (AVG, SUM) is trusted.
# ─────────────────────────────────────────

def convert_sentinel_nulls(df: pd.DataFrame,
                           sentinel=-1
                           ) -> pd.DataFrame:
    numeric_cols = df.select_dtypes(
        include=["int64","float64"]).columns
    for col in numeric_cols:
        if col in ("id", "target"):
            continue
        df[col] = df[col].replace(
            sentinel, np.nan)
    return df

# ─────────────────────────────────────────
# PROBLEM 2: You need to classify 38 unnamed
# columns into feature groups programmatically,
# not by memorising the list.
# ─────────────────────────────────────────

def classify_columns(df: pd.DataFrame) -> dict:
    groups = {"ind": [], "reg": [],
             "car": [], "calc": [], "other": []}
    for col in df.columns:
        matched = False
        for prefix in ["ind","reg","car","calc"]:
            if f"_{prefix}_" in col or \
               col.startswith(f"ps_{prefix}"):
                groups[prefix].append(col)
                matched = True
                break
        if not matched and col not in \
           ("id","target"):
            groups["other"].append(col)
    return groups

# ─────────────────────────────────────────
# PROBLEM 3: Class imbalance must be reported
# BEFORE any modeling or aggregate reporting,
# or every downstream percentage is misleading.
# ─────────────────────────────────────────

def imbalance_report(df: pd.DataFrame,
                     target_col="target"
                     ) -> dict:
    counts = df[target_col].value_counts()
    total  = len(df)
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

Run:
APP_ENV=dev python3 src/ingestion/porto_profiler.py

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 4 — PYSPARK: BRONZE TO SILVER
Topics: reused cache/persist/repartition +
        NEW: schema drift defence
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Create: src/transform/spark_porto.py

Reuse create_spark(), demonstrate_cache_value()
from spark_banking.py without changes.


What is new: SCHEMA DRIFT DEFENCE. In the real
world, a client's "anonymized" export can change
which columns exist between monthly drops
without warning. Your pipeline must not crash —
it must log the drift and adapt.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def load_with_drift_check(spark, path,
                          expected_cols: set):
    df = spark.read.option("header", True) \
        .option("inferSchema", True) \
        .csv(path)

    actual_cols = set(df.columns)
    missing = expected_cols - actual_cols
    new_cols = actual_cols - expected_cols

    if missing:
        logger.warning(
            f"SCHEMA DRIFT: expected columns "
            f"missing: {missing}")
    if new_cols:
        logger.warning(
            f"SCHEMA DRIFT: unexpected new "
            f"columns found: {new_cols}. "
            f"These will be carried through "
            f"but not used downstream until "
            f"reviewed.")

    return df

def replace_sentinel_with_null(df):
    numeric_cols = [
        f.name for f in df.schema.fields
        if f.dataType in (IntegerType(),
                          DoubleType(),
                          LongType())
        and f.name not in ("id", "target")
    ]
    for c in numeric_cols:
        df = df.withColumn(
            c, when(col(c) == -1, None)
            .otherwise(col(c))
        )
    return df

def add_null_density_column(df, cols):
    null_check_expr = sum(
        [when(col(c).isNull(), 1)
         .otherwise(0) for c in cols]
    )
    return df.withColumn(
        "null_field_count", null_check_expr
    ).withColumn(
        "data_completeness_pct",
        round(
            (lit(len(cols)) -
             col("null_field_count"))
            * 100.0 / lit(len(cols)), 1
        )
    )

Write the full silver-layer script following
the same structure as spark_banking.py:
load → schema drift check → sentinel-to-null →
add derived columns → cache for the aggregation
demo → write partitioned parquet to silver/.

Save to: silver/porto_claims_silver/
(parquet, partitioned by a derived risk bucket
built from data_completeness_pct — reuse your
when()/otherwise() pattern from Capstone 2.)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 5 — DIMENSIONAL MODELING
This is the spine of the project. New topic.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

READ THIS SECTION SLOWLY BEFORE WRITING CODE.

─────────────────────────────────────────────
5.1 — WHY DIMENSIONAL MODELING EXISTS
─────────────────────────────────────────────

Every table you have built across all three
capstones so far has been "wide" — one flat
table with everything jammed into every row.
That works for a report. It falls apart for
a warehouse that many teams query differently.

A star schema splits data into two kinds of
tables:

FACT TABLE — records something that HAPPENED.
  One row = one event. Contains foreign keys
  to dimensions, plus numeric MEASURES.
  Example: fact_claims. One row per claim.
  Measures: total_claim_amount, injury_claim,
  property_claim, vehicle_claim.

DIMENSION TABLE — describes the WHO/WHAT/WHERE
  context around the event. Wide, descriptive,
  changes slowly.
  Example: dim_policyholder. One row per
  policyholder. Attributes: age, sex, education,
  occupation, relationship status, address.

Why split them apart at all? Because a fact
table with 500,000 claim rows should not
repeat "insured_education_level: Masters"
500,000 times if only 3,000 distinct
policyholders exist. You store the descriptive
context ONCE in the dimension, and the fact
table just references it by a small integer key.

This is not academic. It is the difference
between a warehouse that queries in 2 seconds
and one that queries in 40, at scale.

─────────────────────────────────────────────
5.2 — DESIGN THE STAR SCHEMA
─────────────────────────────────────────────

Using the Auto Insurance Claims Data columns,
design (on paper first, then in SQL) this
schema:

fact_claims
  claim_key        (surrogate key, generated)
  policy_key        → dim_policy
  policyholder_key  → dim_policyholder
  vehicle_key       → dim_vehicle
  incident_date_key → dim_date
  incident_type
  collision_type
  incident_severity
  number_of_vehicles_involved
  total_claim_amount    (MEASURE)
  injury_claim          (MEASURE)
  property_claim        (MEASURE)
  vehicle_claim         (MEASURE)
  fraud_reported

dim_policyholder   (SCD TYPE 2 — see 5.3)
  policyholder_key  (surrogate key)
  policyholder_id   (natural/business key)
  age
  sex
  education_level
  occupation
  hobbies
  relationship_status
  zip_code
  valid_from
  valid_to
  is_current

dim_policy
  policy_key
  policy_number
  policy_state
  policy_csl
  policy_deductable
  policy_annual_premium
  umbrella_limit
  policy_bind_date

dim_vehicle
  vehicle_key
  auto_make
  auto_model
  auto_year

dim_date
  date_key    (YYYYMMDD integer, standard
               convention — never a real date
               type as the join key)
  full_date
  day_of_week
  month
  quarter
  year
  is_weekend

Grain statement — write this down, it is the
single most important sentence in the whole
model: "One row in fact_claims represents one
insurance claim filed against one policy."

If you cannot state the grain in one sentence,
your fact table is wrong. This is the #1
mistake graders and interviewers look for.

─────────────────────────────────────────────
5.3 — SLOWLY CHANGING DIMENSIONS (SCD)
─────────────────────────────────────────────

Dimensions are not static. A policyholder moves.
Their relationship status changes. A policy gets
upgraded. The question SCD answers: when that
happens, do you overwrite history, or keep it?

TYPE 1 — Overwrite.
  Simplest. You lose the old value entirely.
  Use when history genuinely does not matter
  (correcting a typo in a name, for instance).

TYPE 2 — Add a new row, keep history.
  The workhorse. You add valid_from, valid_to,
  and is_current columns. When a change happens,
  you close out the old row (set valid_to and
  is_current=false) and insert a new row.
  This lets you answer: "what did this
  policyholder's profile look like AT THE TIME
  the claim was filed?" — which matters
  enormously for fraud investigation and
  underwriting audits.

TYPE 3 — Add a column for "previous value".
  Rare. Only tracks ONE prior state, not full
  history. Rarely used in practice; know it
  exists, do not build it here.

BUILD SNAPSHOT 1 (Day 0) — 30 policyholders

Create: silver/policyholder_snapshot_day0.csv
policyholder_id,age,sex,education_level,
occupation,relationship,zip_code,snapshot_date
1,32,MALE,Masters,craft-repair,husband,
466132,2024-01-01
2,45,FEMALE,College,exec-managerial,wife,
468332,2024-01-01
... (continue for 30 rows using values sampled
from the real dataset)

BUILD SNAPSHOT 2 (Day 90) — same 30 people

Copy snapshot 1, then deliberately change:
  - 4 policyholders: zip_code changed (moved)
  - 3 policyholders: relationship changed
    (e.g. "husband" → "not-in-family",
    modeling a divorce)
  - 2 policyholders: occupation changed

Save as: silver/policyholder_snapshot_day90.csv

BUILD SNAPSHOT 3 (Day 180) — same 30 people

Repeat with a different 5-6 changes.
Save as: silver/policyholder_snapshot_day180.csv

WRITE THE SCD TYPE 2 BUILDER — Python first

Create: src/models/scd2_builder.py

import pandas as pd
from datetime import datetime

def build_scd2_history(snapshots: list,
                       natural_key="policyholder_id",
                       tracked_cols=None):
    """
    snapshots: list of (df, snapshot_date) tuples
               in chronological order
    Returns a full SCD Type 2 history table.
    """
    if tracked_cols is None:
        tracked_cols = [
            "zip_code", "relationship",
            "occupation"
        ]

    history = []
    current_state = {}
    surrogate_key = 1

    for df, snap_date in snapshots:
        for _, row in df.iterrows():
            nk = row[natural_key]
            current_values = {
                c: row[c] for c in tracked_cols
            }

            if nk not in current_state:
                # first time seeing this person
                record = row.to_dict()
                record["policyholder_key"] = \
                    surrogate_key
                record["valid_from"] = snap_date
                record["valid_to"] = None
                record["is_current"] = True
                history.append(record)
                current_state[nk] = (
                    surrogate_key, current_values)
                surrogate_key += 1
                continue

            prev_key, prev_values = \
                current_state[nk]

            if prev_values != current_values:
                # something tracked changed —
                # close the old row, open a new one
                for h in history:
                    if h["policyholder_key"] == \
                       prev_key and \
                       h["is_current"]:
                        h["valid_to"] = snap_date
                        h["is_current"] = False

                record = row.to_dict()
                record["policyholder_key"] = \
                    surrogate_key
                record["valid_from"] = snap_date
                record["valid_to"] = None
                record["is_current"] = True
                history.append(record)
                current_state[nk] = (
                    surrogate_key, current_values)
                surrogate_key += 1

    return pd.DataFrame(history)

Run it across all 3 snapshots. Print the
resulting dim_policyholder table. Count how many
surrogate key rows exist for the 30 natural
policyholder IDs — it should be MORE than 30,
because changed people now have 2 or 3 rows
each representing their history.

NOW WRITE THE SAME LOGIC IN SQL — this is the
version interviewers actually ask for on a
whiteboard.

CREATE TABLE dim_policyholder (
    policyholder_key   NUMBER GENERATED
        ALWAYS AS IDENTITY PRIMARY KEY,
    policyholder_id     NUMBER,
    age                 NUMBER,
    sex                 VARCHAR2(10),
    education_level     VARCHAR2(50),
    occupation          VARCHAR2(50),
    relationship        VARCHAR2(30),
    zip_code            VARCHAR2(10),
    valid_from          DATE,
    valid_to            DATE,
    is_current          NUMBER(1)
);

-- The SCD2 merge pattern, written as a
-- repeatable procedure a real pipeline runs
-- every time a new snapshot arrives:

-- Step 1: Close out changed records
UPDATE dim_policyholder d
SET valid_to = (
        SELECT snapshot_date
        FROM policyholder_snapshot_new s
        WHERE s.policyholder_id =
              d.policyholder_id
    ),
    is_current = 0
WHERE d.is_current = 1
AND EXISTS (
    SELECT 1
    FROM policyholder_snapshot_new s
    WHERE s.policyholder_id =
          d.policyholder_id
    AND (
        s.zip_code     != d.zip_code OR
        s.relationship != d.relationship OR
        s.occupation   != d.occupation
    )
);

-- Step 2: Insert new current rows for
-- anyone who changed OR is brand new
INSERT INTO dim_policyholder (
    policyholder_id, age, sex,
    education_level, occupation,
    relationship, zip_code,
    valid_from, valid_to, is_current
)
SELECT
    s.policyholder_id, s.age, s.sex,
    s.education_level, s.occupation,
    s.relationship, s.zip_code,
    s.snapshot_date, NULL, 1
FROM policyholder_snapshot_new s
WHERE NOT EXISTS (
    SELECT 1 FROM dim_policyholder d
    WHERE d.policyholder_id =
          s.policyholder_id
    AND d.is_current = 1
    AND d.zip_code     = s.zip_code
    AND d.relationship = s.relationship
    AND d.occupation   = s.occupation
);

-- Now the point-in-time query every
-- fraud/audit team actually asks:
-- "What did policyholder 7's profile
--  look like on the date THIS claim
--  was filed?"

SELECT c.claim_key, c.incident_date,
       d.occupation, d.relationship,
       d.zip_code
FROM   fact_claims c
JOIN   dim_policyholder d
       ON  c.policyholder_key =
           d.policyholder_key
WHERE  c.incident_date BETWEEN
       d.valid_from AND
       NVL(d.valid_to, DATE '9999-12-31');

Sit with that last query. That JOIN condition —
matching an event date INTO a validity range
instead of matching on equality — is the single
most tested SCD2 pattern in real interviews.
Practice writing it from memory.

─────────────────────────────────────────────
5.4 — BUILD THE REMAINING DIMENSIONS
             AND THE FACT TABLE
─────────────────────────────────────────────

Create dim_date generation (Python, standard
practice — this table is always generated,
never sourced):

import pandas as pd

def generate_dim_date(start="2023-01-01",
                      end="2025-12-31"):
    dates = pd.date_range(start, end)
    df = pd.DataFrame({"full_date": dates})
    df["date_key"] = df["full_date"].dt.strftime(
        "%Y%m%d").astype(int)
    df["day_of_week"] = df["full_date"].dt.day_name()
    df["month"]   = df["full_date"].dt.month
    df["quarter"] = df["full_date"].dt.quarter
    df["year"]    = df["full_date"].dt.year
    df["is_weekend"] = df["full_date"].dt.dayofweek \
        .isin([5,6])
    return df[["date_key","full_date","day_of_week",
              "month","quarter","year","is_weekend"]]

Build dim_vehicle and dim_policy directly from
the claims CSV using DISTINCT + surrogate key
generation — same ROW_NUMBER() pattern you
already know from Window Functions in earlier
days.

CREATE TABLE dim_vehicle AS
SELECT
    ROW_NUMBER() OVER(
        ORDER BY auto_make, auto_model, auto_year
    ) AS vehicle_key,
    auto_make, auto_model, auto_year
FROM (
    SELECT DISTINCT auto_make, auto_model, auto_year
    FROM raw_claims
);

Build fact_claims by joining the raw claims
data against all four dimension tables to
resolve surrogate keys — this final JOIN
chain is the payoff of the whole exercise.

INSERT INTO fact_claims
SELECT
    ROW_NUMBER() OVER(ORDER BY r.incident_date)
        AS claim_key,
    p.policy_key,
    ph.policyholder_key,
    v.vehicle_key,
    TO_NUMBER(TO_CHAR(r.incident_date,'YYYYMMDD'))
        AS incident_date_key,
    r.incident_type, r.collision_type,
    r.incident_severity,
    r.number_of_vehicles_involved,
    r.total_claim_amount, r.injury_claim,
    r.property_claim, r.vehicle_claim,
    r.fraud_reported
FROM raw_claims r
JOIN dim_policy p
     ON r.policy_number = p.policy_number
JOIN dim_policyholder ph
     ON r.policyholder_id = ph.policyholder_id
     AND ph.is_current = 1
JOIN dim_vehicle v
     ON r.auto_make = v.auto_make
     AND r.auto_model = v.auto_model
     AND r.auto_year = v.auto_year;

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 6 — THE TRANSFORMATION LAYER
Dataform AND dbt, side by side. New topic.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Both tools solve the same problem: turning
raw SQL scripts into version-controlled,
tested, dependency-aware models. You will
build the SAME staging → mart pipeline twice.

─────────────────────────────────────────────
6.1 — DATAFORM (BigQuery-native)
─────────────────────────────────────────────

In Google Cloud Console: BigQuery → Dataform →
Create Repository → name it porto_seguro_dwh.

Folder structure inside the repository:
definitions/
  staging/
    stg_claims.sqlx
    stg_policyholders.sqlx
  marts/
    fact_claims.sqlx
    dim_policyholder.sqlx

definitions/staging/stg_claims.sqlx:

config {
  type: "view",
  description: "Cleaned claims staging layer"
}

SELECT
  policy_number,
  CAST(incident_date AS DATE) AS incident_date,
  incident_type,
  collision_type,
  incident_severity,
  total_claim_amount,
  injury_claim,
  property_claim,
  vehicle_claim,
  fraud_reported
FROM ${ref("raw_claims")}
WHERE total_claim_amount IS NOT NULL

definitions/marts/fact_claims.sqlx:

config {
  type: "table",
  assertions: {
    uniqueKey: ["claim_key"],
    nonNull: ["total_claim_amount"]
  }
}

SELECT
  ROW_NUMBER() OVER(
    ORDER BY incident_date
  ) AS claim_key,
  policy_number,
  incident_date,
  incident_type,
  total_claim_amount,
  injury_claim,
  property_claim,
  vehicle_claim,
  fraud_reported
FROM ${ref("stg_claims")}

Notice the "assertions" block — that IS the
data quality test. Dataform will fail the run
if claim_key is not unique, or if
total_claim_amount is ever NULL. This is the
"shift left" testing pattern: catch the break
at the transformation step, not three reports
downstream when someone notices a wrong number.

Compile and run:
  Click "Start execution" in the Dataform UI,
  or from CLI:
  dataform run --tags marts

─────────────────────────────────────────────
6.2 — dbt (the version most job posts name)
─────────────────────────────────────────────

pip install dbt-bigquery
dbt init porto_seguro_dbt

Folder structure:
models/
  staging/
    stg_claims.sql
    schema.yml
  marts/
    fact_claims.sql
    schema.yml

models/staging/stg_claims.sql:

SELECT
    policy_number,
    CAST(incident_date AS DATE) AS incident_date,
    incident_type,
    collision_type,
    incident_severity,
    total_claim_amount,
    injury_claim,
    property_claim,
    vehicle_claim,
    fraud_reported
FROM {{ source('raw', 'claims') }}
WHERE total_claim_amount IS NOT NULL

models/staging/schema.yml:

version: 2
sources:
  - name: raw
    tables:
      - name: claims

models/marts/fact_claims.sql:

SELECT
    ROW_NUMBER() OVER(
        ORDER BY incident_date
    ) AS claim_key,
    policy_number,
    incident_date,
    incident_type,
    total_claim_amount,
    injury_claim,
    property_claim,
    vehicle_claim,
    fraud_reported
FROM {{ ref('stg_claims') }}

models/marts/schema.yml:

version: 2
models:
  - name: fact_claims
    columns:
      - name: claim_key
        tests: [unique, not_null]
      - name: total_claim_amount
        tests: [not_null]

Run:
dbt run
dbt test

─────────────────────────────────────────────
6.3 — THE COMPARISON TABLE — memorise this
─────────────────────────────────────────────

Dataform:
  Free, built into BigQuery, zero extra infra
  Uses SQLX (SQL + light JavaScript templating)
  Tests are called "assertions"
  Best when: BigQuery is your only warehouse

dbt:
  Open-source core is free; dbt Cloud is paid
  Uses Jinja-templated SQL ({{ ref() }})
  Tests are just called "tests"
  Larger ecosystem, warehouse-agnostic
  Best when: client runs Snowflake, Redshift,
  Databricks — or the job description explicitly
  names dbt (which most currently do)

Both compile down to the same idea: ref()
between models builds an automatic dependency
graph, tests run before anything downstream
consumes a broken table, and every change is
versioned in Git. Learn the concept once. The
syntax difference is a Tuesday afternoon.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 7 — LLM-AUGMENTED EXTRACTION
One small, contained exercise. Bonus, not core.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Real claim adjuster notes are never public.
Use this constructed microdataset instead —
15 realistic notes, written to mirror the
style real adjusters actually use.

Create: silver/adjuster_notes.csv

claim_id,note_text
1,"Insured rear-ended at signal on Elm St.
  Minor bumper damage. No injuries reported.
  Other party at fault per police report."
2,"Vehicle stolen from driveway overnight.
  Insured reported to PD same morning.
  Case number obtained. Total loss suspected."
3,"Hail damage to hood and roof following
  storm on 3/14. Multiple dents, no glass
  breakage. Insured has photos."
4,"Two vehicle collision at intersection.
  Insured claims other driver ran red light.
  Witness statement obtained. Moderate front
  end damage, airbags deployed."
5,"Garage fire spread to vehicle parked
  inside. Significant smoke and heat damage
  throughout cabin. Insured was not present."
6,"Single vehicle rollover on rural highway.
  Insured reports deer in roadway caused
  swerve. Vehicle totaled. No injuries."
7,"Parking lot scrape, unknown other party.
  Insured found note left on windshield with
  partial license plate only."
8,"Flood damage from recent storm surge.
  Water reached above dashboard. Engine
  compartment fully submerged."
9,"Rear window shattered, believed vandalism.
  Occurred overnight in insured's own
  driveway. No other visible damage."
10,"Multi car pileup on highway during fog.
   Insured struck from behind while stopped
   in traffic. Neck strain reported, minor."
11,"Insured backed into stationary object in
   own garage. Rear bumper and taillight
   damage. Single vehicle, no third party."
12,"Catalytic converter stolen from vehicle
   overnight. Discovered when insured started
   car next morning, unusual loud noise."
13,"Tree branch fell on vehicle during high
   wind advisory. Roof and windshield
   damaged. Vehicle parked on street at time."
14,"Insured struck a pothole at high speed on
   the interstate. Two tires blown, rim
   damage confirmed on inspection."
15,"Collision with a deer on rural road at
   dusk. Front bumper and headlight assembly
   damaged. No injuries to occupants."

The task: extract structured fields from
this free text WITHOUT writing 15 regex rules
by hand. This is exactly the kind of messy,
inconsistent unstructured input where an LLM
genuinely outperforms manual parsing rules —
and exactly the pattern showing up in 2026
data engineering job descriptions: LLM-based
extraction as one stage feeding a traditional
pipeline, not replacing the pipeline.

Create: src/models/note_extractor.py

import os
import json
import pandas as pd
from anthropic import Anthropic

client = Anthropic()  # reads ANTHROPIC_API_KEY
                       # from environment

EXTRACTION_PROMPT = """
Extract structured fields from this insurance
adjuster note. Return ONLY valid JSON, no
other text, in this exact shape:

{{
  "incident_category": one of ["collision",
      "weather", "theft", "vandalism",
      "fire", "animal_strike", "road_hazard"],
  "at_fault_party": one of ["insured",
      "other_party", "unknown", "not_applicable"],
  "injury_mentioned": true or false,
  "severity_hint": one of ["minor", "moderate",
      "severe", "total_loss"]
}}

Note: {note_text}
"""

def extract_fields(note_text: str) -> dict:
    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=200,
            messages=[{
                "role": "user",
                "content": EXTRACTION_PROMPT.format(
                    note_text=note_text)
            }]
        )
        raw = response.content[0].text.strip()
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"error": "parse_failed",
               "raw_response": raw}
    except Exception as e:
        return {"error": str(e)}

def process_all_notes(filepath: str
                      ) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    results = []
    for _, row in df.iterrows():
        extracted = extract_fields(
            row["note_text"])
        extracted["claim_id"] = row["claim_id"]
        results.append(extracted)
    return pd.DataFrame(results)

if __name__ == "__main__":
    result_df = process_all_notes(
        "silver/adjuster_notes.csv")
    print(result_df)
    result_df.to_csv(
        "silver/adjuster_notes_structured.csv",
        index=False)

Run it. Look at the output columns:
incident_category, at_fault_party,
injury_mentioned, severity_hint — four new
structured columns extracted from free text,
ready to JOIN onto fact_claims by claim_id.

This is the whole exercise. One script, one
small dataset, one clean idea: an LLM call is
just another transformation step in a pipeline,
with the same failure modes (rate limits,
malformed output, cost per call) any API
integration has. Log every call. Handle the
JSON parse failure case exactly as written
above — it will happen.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 8 — GOLD LAYER: SQL ANALYTICS
Querying the star schema you built
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Notice how much simpler these queries are now
that fact and dimension tables exist, compared
to the flat-table queries in Capstones 1 and 2.

REPORT 1 — Claims by policyholder occupation,
           using point-in-time dimension join

WITH claims_with_context AS (
    SELECT
        f.claim_key,
        f.total_claim_amount,
        f.fraud_reported,
        d.occupation,
        d.relationship
    FROM fact_claims f
    JOIN dim_date dt
         ON f.incident_date_key = dt.date_key
    JOIN dim_policyholder d
         ON f.policyholder_key =
            d.policyholder_key
         AND dt.full_date BETWEEN
             d.valid_from AND
             NVL(d.valid_to, DATE '9999-12-31')
)
SELECT
    occupation,
    COUNT(*)                        AS claims,
    ROUND(SUM(total_claim_amount),2)
                                     AS total_paid,
    ROUND(AVG(total_claim_amount),2)
                                     AS avg_claim,
    SUM(CASE WHEN fraud_reported='Y'
        THEN 1 ELSE 0 END)          AS fraud_flags,
    RANK() OVER(
        ORDER BY SUM(total_claim_amount) DESC
    )                                AS payout_rank
FROM claims_with_context
GROUP BY occupation
ORDER BY payout_rank;

REPORT 2 — Vehicle risk profile

SELECT
    v.auto_make, v.auto_year,
    COUNT(*)               AS claims,
    ROUND(AVG(f.total_claim_amount),2)
                            AS avg_claim,
    DENSE_RANK() OVER(
        ORDER BY AVG(f.total_claim_amount) DESC
    )                       AS risk_rank
FROM fact_claims f
JOIN dim_vehicle v ON f.vehicle_key = v.vehicle_key
GROUP BY v.auto_make, v.auto_year
HAVING COUNT(*) > 2
ORDER BY risk_rank;

REPORT 3 — Monthly claims trend with LAG

WITH monthly AS (
    SELECT dt.year, dt.month,
           COUNT(*) AS claims,
           ROUND(SUM(f.total_claim_amount),2)
               AS total_paid
    FROM fact_claims f
    JOIN dim_date dt
         ON f.incident_date_key = dt.date_key
    GROUP BY dt.year, dt.month
)
SELECT *,
    LAG(total_paid) OVER(
        ORDER BY year, month
    ) AS prev_month,
    ROUND(total_paid - LAG(total_paid)
        OVER(ORDER BY year, month), 2)
        AS change
FROM monthly
ORDER BY year, month;

REPORT 4 — Join the LLM-extracted fields
           back onto the fact table

SELECT
    n.incident_category,
    n.at_fault_party,
    COUNT(*)                        AS claims,
    ROUND(AVG(f.total_claim_amount),2)
                                     AS avg_claim,
    SUM(CASE WHEN n.injury_mentioned
        THEN 1 ELSE 0 END)          AS injury_claims
FROM fact_claims f
JOIN adjuster_notes_structured n
     ON f.claim_key = n.claim_id
GROUP BY n.incident_category, n.at_fault_party
ORDER BY avg_claim DESC;

This last query is the point of Phase 7 — the
unstructured note is now just another dimension
you can group and filter by.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 9 — AUTOMATION AND AIRFLOW
Reuse the pattern, add the new tasks
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Take run_nfc_pipeline.sh from Capstone 2,
rename it run_porto_pipeline.sh, and add two
new steps after the existing Spark step:

run_step "Dataform Compile + Run" \
    "dataform run --tags marts"

run_step "dbt Test" \
    "dbt run && dbt test"

In dags/porto_pipeline_dag.py, extend the
Capstone 2 DAG pattern with two new
BashOperator tasks for dataform and dbt,
placed after the Spark task and before
archiving:

task_dataform = BashOperator(
    task_id="dataform_run",
    bash_command="dataform run --tags marts"
)

task_dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd porto_seguro_dbt && "
                 "dbt run && dbt test"
)

task_spark_silver >> task_dataform >> \
    task_dbt_test >> task_archive

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DELIVERABLES CHECKLIST
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Phase 1-2   Setup reused, bronze/silver/gold
            folders in place, both datasets
            profiled via shell

Phase 3     porto_profiler.py runs clean.
            Column groups classified. -1
            converted to real NULL. Imbalance
            ratio logged and explained.

Phase 4     Spark silver layer built. Schema
            drift check demonstrated (rename
            one column and confirm the
            warning fires).

Phase 5     Star schema diagram drawn (paper
            or tool, does not need to be
            digital). Grain statement written.
            3 snapshots built. SCD2 builder
            runs in Python AND the SQL
            MERGE-style pattern runs on
            freesql.com. Point-in-time join
            query executes correctly.

Phase 6     Same staging + fact_claims model
            built in BOTH Dataform (with
            assertions) and dbt (with tests).
            Comparison table can be recited
            from memory.

Phase 7     15 notes processed through the
            LLM extractor. Output CSV has 4
            new structured columns. JSON
            parse failure handled without
            crashing the script.

Phase 8     All 4 Gold reports run. Report 1
            correctly uses the point-in-time
            dimension join, not a plain join.

Phase 9     Airflow DAG includes Dataform and
            dbt tasks after Spark, before
            archive.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WHY EACH NEW TOPIC WAS CHOSEN
Grounded in 2026 hiring research, not guesswork
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Dimensional modeling — a dedicated data
modeling round appears in roughly a third of
2026 data engineering interview loops, and
candidates who skip modeling prep tend to fail
it even with strong SQL. It did not exist
anywhere in KODE-X before this capstone.

SCD Type 2 point-in-time joins — this specific
JOIN pattern (matching a date into a validity
range instead of matching on equality) is one
of the most consistently tested modeling
questions once a candidate clears the basic
star-schema-definition stage.

Dataform + dbt — dbt is described in current
hiring analysis as quietly becoming a hard
requirement at any company running a modern
data stack, appearing on the large majority of
data engineering job descriptions. Dataform is
Google's direct, zero-cost answer for teams
already committed to BigQuery, which is exactly
the profile of every client you will work with
professionally.

Medallion vocabulary — Bronze/Silver/Gold is now
the standard way interviewers and job
descriptions describe pipeline layering, across
Databricks, Snowflake, and BigQuery shops alike.

LLM-augmented extraction — an emerging but
real 2026 interview topic: designing pipelines
that use an LLM to structure messy documents
as one stage feeding a traditional ETL flow,
distinct from replacing the pipeline with AI.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ESTIMATED TIMELINE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Phase 1-2  Setup + Linux           : 30 min
Phase 3    Python ingestion        : 60 min
Phase 4    PySpark silver          : 60 min
Phase 5    Dimensional modeling    : 120 min
           (the anchor phase — do
           not compress this one)
Phase 6    Dataform + dbt          : 90 min
Phase 7    LLM extraction          : 30 min
Phase 8    SQL analytics           : 45 min
Phase 9    Automation + Airflow    : 30 min

Total                              : ~8 hours
Spread across                      : 2-3 days
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━