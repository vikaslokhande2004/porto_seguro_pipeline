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
FROM {{ source('raw', 'raw_claims') }}
WHERE total_claim_amount IS NOT NULL