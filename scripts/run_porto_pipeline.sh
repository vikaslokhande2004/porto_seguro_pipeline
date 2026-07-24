#!/bin/bash

set -e

run_step() {
    STEP_NAME="$1"
    COMMAND="$2"

    echo "=========================================="
    echo "STARTING: $STEP_NAME"
    echo "=========================================="

    eval "$COMMAND"

    echo "=========================================="
    echo "COMPLETED: $STEP_NAME"
    echo "=========================================="
}

# ------------------------------------------
# Project root
# ------------------------------------------
cd /mnt/d/porto_seguro_pipeline


# ------------------------------------------
# Step 1: python3 snapshots
# ------------------------------------------
run_step "Create Snapshots" \
    "python3 -m src.models.snapshot_builder"


# ------------------------------------------
# Step 2: Build SCD2 Policyholder
# ------------------------------------------
run_step "Build SCD2 Policyholder" \
    "python3 -m src.models.scd2_builder"


# ------------------------------------------
# Step 3: Generate Date Dimension
# ------------------------------------------
run_step "Generate Date Dimension" \
    "python3 -m src.models.date_dimension"


# ------------------------------------------
# Step 4: Build Dimensions
# ------------------------------------------
run_step "Build Dimensions" \
    "python3 -m src.models.dimension_builder"


# ------------------------------------------
# Step 5: Build Fact Table
# ------------------------------------------
run_step "Build Fact Claims" \
    "python3 -m src.models.fact_builder"


# ------------------------------------------
# Step 6: Spark Bronze → Silver
# ------------------------------------------
run_step "Spark Silver Transformation" \
    "python3 -m src.transform.spark_porto"


# ------------------------------------------
# Step 7: dbt Run + Test
# ------------------------------------------
run_step "dbt Run + Test" \
    "cd sql/dbt && dbt run && dbt test"


echo ""
echo "=========================================="
echo "PORTO SEGURO PIPELINE COMPLETED"
echo "=========================================="