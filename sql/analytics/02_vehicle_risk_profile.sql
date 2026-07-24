SELECT
    v.auto_make,
    v.auto_year,
    COUNT(*) AS claims,
    ROUND(AVG(f.total_claim_amount), 2) AS avg_claim,
    DENSE_RANK() OVER (
        ORDER BY AVG(f.total_claim_amount) DESC
    ) AS risk_rank
FROM fact_claims f
JOIN dim_vehicle v
    ON f.vehicle_key = v.vehicle_key
GROUP BY
    v.auto_make,
    v.auto_year
HAVING COUNT(*) > 2
ORDER BY risk_rank;