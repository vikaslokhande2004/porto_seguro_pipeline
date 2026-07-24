-- ============================================================
-- PHASE 5 - DIMENSIONAL MODELING
-- 04_analytics_queries.sql
-- ============================================================


-- ============================================================
-- QUERY 1
-- TOTAL CLAIM AMOUNT
-- ============================================================

SELECT
    SUM(total_claim_amount)
        AS total_claim_amount

FROM fact_claims;


-- ============================================================
-- QUERY 2
-- CLAIMS BY POLICY STATE
-- ============================================================

SELECT
    p.policy_state,

    COUNT(*) AS claim_count,

    SUM(f.total_claim_amount)
        AS total_claim_amount

FROM fact_claims f

JOIN dim_policy p
    ON f.policy_key =
       p.policy_key

GROUP BY
    p.policy_state

ORDER BY
    total_claim_amount DESC;


-- ============================================================
-- QUERY 3
-- FRAUD RATE BY INCIDENT SEVERITY
-- ============================================================

SELECT
    incident_severity,

    COUNT(*) AS total_claims,

    SUM(
        CASE
            WHEN fraud_reported = 'Y'
            THEN 1
            ELSE 0
        END
    ) AS fraudulent_claims,

    ROUND(
        100.0 *
        SUM(
            CASE
                WHEN fraud_reported = 'Y'
                THEN 1
                ELSE 0
            END
        )
        / COUNT(*),
        2
    ) AS fraud_rate_percent

FROM fact_claims

GROUP BY
    incident_severity

ORDER BY
    fraud_rate_percent DESC;


-- ============================================================
-- QUERY 4
-- CLAIM AMOUNT BY VEHICLE MAKE
-- ============================================================

SELECT
    v.auto_make,

    COUNT(*) AS claim_count,

    SUM(
        f.total_claim_amount
    ) AS total_claim_amount,

    AVG(
        f.total_claim_amount
    ) AS average_claim_amount

FROM fact_claims f

JOIN dim_vehicle v

    ON f.vehicle_key =
       v.vehicle_key

GROUP BY
    v.auto_make

ORDER BY
    total_claim_amount DESC;


-- ============================================================
-- QUERY 5
-- CLAIMS BY YEAR
-- ============================================================

SELECT
    d.year,

    COUNT(*) AS claim_count,

    SUM(
        f.total_claim_amount
    ) AS total_claim_amount

FROM fact_claims f

JOIN dim_date d

    ON f.incident_date_key =
       d.date_key

GROUP BY
    d.year

ORDER BY
    d.year;


-- ============================================================
-- QUERY 6
-- CLAIMS BY MONTH
-- ============================================================

SELECT
    d.year,

    d.month,

    COUNT(*) AS claim_count,

    SUM(
        f.total_claim_amount
    ) AS total_claim_amount

FROM fact_claims f

JOIN dim_date d

    ON f.incident_date_key =
       d.date_key

GROUP BY
    d.year,
    d.month

ORDER BY
    d.year,
    d.month;


-- ============================================================
-- QUERY 7
-- CLAIMS BY POLICYHOLDER EDUCATION
-- ============================================================

SELECT
    ph.education_level,

    COUNT(*) AS claim_count,

    SUM(
        f.total_claim_amount
    ) AS total_claim_amount

FROM fact_claims f

JOIN dim_policyholder ph

    ON f.policyholder_key =
       ph.policyholder_key

GROUP BY
    ph.education_level

ORDER BY
    total_claim_amount DESC;


-- ============================================================
-- QUERY 8
-- CLAIMS BY POLICYHOLDER OCCUPATION
-- ============================================================

SELECT
    ph.occupation,

    COUNT(*) AS claim_count,

    AVG(
        f.total_claim_amount
    ) AS average_claim_amount

FROM fact_claims f

JOIN dim_policyholder ph

    ON f.policyholder_key =
       ph.policyholder_key

GROUP BY
    ph.occupation

ORDER BY
    average_claim_amount DESC;


-- ============================================================
-- QUERY 9
-- FRAUD BY VEHICLE MAKE
-- ============================================================

SELECT
    v.auto_make,

    COUNT(*) AS total_claims,

    SUM(
        CASE
            WHEN f.fraud_reported = 'Y'
            THEN 1
            ELSE 0
        END
    ) AS fraudulent_claims

FROM fact_claims f

JOIN dim_vehicle v

    ON f.vehicle_key =
       v.vehicle_key

GROUP BY
    v.auto_make

ORDER BY
    fraudulent_claims DESC;


-- ============================================================
-- QUERY 10
-- SCD TYPE 2 POINT-IN-TIME QUERY
-- ============================================================

SELECT

    f.claim_key,

    d.full_date AS incident_date,

    ph.policyholder_id,

    ph.occupation,

    ph.relationship,

    ph.zip_code,

    ph.valid_from,

    ph.valid_to

FROM fact_claims f


JOIN dim_date d

    ON f.incident_date_key =
       d.date_key


JOIN dim_policyholder ph

    ON f.policyholder_key =
       ph.policyholder_key

ORDER BY
    f.claim_key;


-- ============================================================
-- QUERY 11
-- FRAUD RATE BY POLICY STATE
-- ============================================================

SELECT

    p.policy_state,

    COUNT(*) AS total_claims,

    SUM(
        CASE
            WHEN f.fraud_reported = 'Y'
            THEN 1
            ELSE 0
        END
    ) AS fraud_claims,

    ROUND(

        100.0 *

        SUM(
            CASE
                WHEN f.fraud_reported = 'Y'
                THEN 1
                ELSE 0
            END
        )

        / COUNT(*),

        2

    ) AS fraud_rate_percent

FROM fact_claims f

JOIN dim_policy p

    ON f.policy_key =
       p.policy_key

GROUP BY
    p.policy_state

ORDER BY
    fraud_rate_percent DESC;