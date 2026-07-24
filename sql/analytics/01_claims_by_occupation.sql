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
        ON f.policyholder_key = d.policyholder_key
        AND dt.full_date BETWEEN
            d.valid_from
            AND COALESCE(d.valid_to, DATE '9999-12-31')
)
SELECT
    occupation,
    COUNT(*) AS claims,
    ROUND(SUM(total_claim_amount), 2) AS total_paid,
    ROUND(AVG(total_claim_amount), 2) AS avg_claim,
    SUM(
        CASE
            WHEN fraud_reported = 'Y' THEN 1
            ELSE 0
        END
    ) AS fraud_flags,
    RANK() OVER (
        ORDER BY SUM(total_claim_amount) DESC
    ) AS payout_rank
FROM claims_with_context
GROUP BY occupation
ORDER BY payout_rank;