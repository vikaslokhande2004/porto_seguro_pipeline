#!/bin/bash
# ============================================
# column_inventory.sh
# Purpose: Generate column-level inventory
#          for wide CSV files
# Usage  : ./scripts/column_inventory.sh
#          bronze/porto_claims.csv
# ============================================

set -euo pipefail

FILE=${1:-""}
if [ -z "$FILE" ]; then
    echo "Usage: $0 <csv_file>"
    exit 1
fi

FILENAME=$(basename "$FILE")
LOG_DIR="./logs"
OUT="$LOG_DIR/column_inventory_$(date +%Y%m%d_%H%M%S).txt"
mkdir -p "$LOG_DIR"

echo "Column Inventory: $FILENAME"  | tee "$OUT"
echo "Generated: $(date)"           | tee -a "$OUT"
echo "==============================" | tee -a "$OUT"

# Get header row
HEADER=$(head -1 "$FILE")
COL_COUNT=$(echo "$HEADER" | tr ',' '\n' | wc -l)
echo "Total columns: $COL_COUNT"    | tee -a "$OUT"
echo ""                             | tee -a "$OUT"

# Process each column
IFS=',' read -ra COLS <<< "$HEADER"
COL_NUM=1

for col in "${COLS[@]}"; do
    col=$(echo "$col" | tr -d '"' | xargs)

    # Get 5 sample values for this column
    # Using awk to extract column by position
    samples=$(awk -F',' -v n="$COL_NUM" \
        'NR>1 && NR<=6 {
            gsub(/"/, "", $n)
            print $n
        }' "$FILE" | paste -sd '|' -)
    
    # Count empty/null values in first 1000 rows
    null_count=$(awk -F',' -v n="$COL_NUM" \
        'NR>1 && NR<=1001 {
            val=$n
            gsub(/"/, "", val)
            gsub(/^ +| +$/, "", val)
            if (val=="" || val=="NA" || val=="N/A" || val=="null")
                count++
        }
        END { print count+0 }' "$FILE")

    echo "[$COL_NUM] $col" | tee -a "$OUT"
    echo "    Samples : $samples" | tee -a "$OUT"
    echo "    Nulls(1k): $null_count" | tee -a "$OUT"
    echo "" | tee -a "$OUT"

    COL_NUM=$((COL_NUM + 1))
done

echo "Report saved: $OUT"

# ---- end of script ----