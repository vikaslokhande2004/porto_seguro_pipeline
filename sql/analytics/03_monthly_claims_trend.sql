WITH monthly AS (
    SELECT
        dt.year,
        dt.month,
        COUNT(*) AS claims,
        ROUND(SUM(f.total_claim_amount), 2) AS total_paid
    FROM fact_claims f
    JOIN dim_date dt
        ON f.incident_date_key = dt.date_key
    GROUP BY
        dt.year,
        dt.month
)
SELECT
    *,
    LAG(total_paid) OVER (
        ORDER BY year, month
    ) AS prev_month,
    ROUND(
        total_paid
        - LAG(total_paid) OVER (
            ORDER BY year, month
        ),
        2
    ) AS change
FROM monthly
ORDER BY
    year,
    month;