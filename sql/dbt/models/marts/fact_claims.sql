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