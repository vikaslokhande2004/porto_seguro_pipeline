#!/usr/bin/env bash

set -euo pipefail

# ============================================================
# Phase 5 - Dimensional Modeling Pipeline
# ============================================================

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$PROJECT_ROOT"

echo "============================================================"
echo "PHASE 5 - DIMENSIONAL MODELING PIPELINE"
echo "============================================================"
echo "Project root: $PROJECT_ROOT"
echo "Started at: $(date)"
echo

# ------------------------------------------------------------
# Step 1: Create Policyholder Snapshots
# ------------------------------------------------------------

echo "------------------------------------------------------------"
echo "STEP 1/5 - Creating Policyholder Snapshots"
echo "------------------------------------------------------------"

python3 -m src.models.snapshot_builder

echo
echo "✓ Step 1 completed successfully"
echo


# ------------------------------------------------------------
# Step 2: Build SCD Type 2 Policyholder Dimension
# ------------------------------------------------------------

echo "------------------------------------------------------------"
echo "STEP 2/5 - Building SCD Type 2 Policyholder Dimension"
echo "------------------------------------------------------------"

python3 -m src.models.scd2_builder

echo
echo "✓ Step 2 completed successfully"
echo


# ------------------------------------------------------------
# Step 3: Generate Date Dimension
# ------------------------------------------------------------

echo "------------------------------------------------------------"
echo "STEP 3/5 - Generating Date Dimension"
echo "------------------------------------------------------------"

python3 -m src.models.date_dimension

echo
echo "✓ Step 3 completed successfully"
echo


# ------------------------------------------------------------
# Step 4: Build Policy and Vehicle Dimensions
# ------------------------------------------------------------

echo "------------------------------------------------------------"
echo "STEP 4/5 - Building Policy and Vehicle Dimensions"
echo "------------------------------------------------------------"

python3 -m src.models.dimension_builder

echo
echo "✓ Step 4 completed successfully"
echo


# ------------------------------------------------------------
# Step 5: Build Fact Claims
# ------------------------------------------------------------

echo "------------------------------------------------------------"
echo "STEP 5/5 - Building Fact Claims"
echo "------------------------------------------------------------"

python3 -m src.models.fact_builder

echo
echo "✓ Step 5 completed successfully"
echo


# ------------------------------------------------------------
# Pipeline Completed
# ------------------------------------------------------------

echo "============================================================"
echo "PHASE 5 PIPELINE COMPLETED SUCCESSFULLY"
echo "============================================================"

echo "Completed at: $(date)"
echo

echo "Generated Gold Tables:"
echo

if [[ -d "gold" ]]; then
    find gold -maxdepth 1 -type f -name "*.csv" -printf "  ✓ %f\n" | sort
else
    echo "  WARNING: gold directory not found"
fi

echo
echo "============================================================"