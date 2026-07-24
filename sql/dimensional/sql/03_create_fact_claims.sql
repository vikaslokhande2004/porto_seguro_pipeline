-- ============================================================
-- PHASE 5 - DIMENSIONAL MODELING
-- 03_create_fact_claims.sql
-- ============================================================


-- ============================================================
-- FACT CLAIMS TABLE
-- ============================================================

CREATE TABLE fact_claims (

    claim_key
        INTEGER PRIMARY KEY,

    policy_key
        INTEGER NOT NULL,

    policyholder_key
        INTEGER NOT NULL,

    vehicle_key
        INTEGER NOT NULL,

    incident_date_key
        INTEGER NOT NULL,


    -- Degenerate / descriptive dimensions

    incident_type
        VARCHAR(100),

    collision_type
        VARCHAR(100),

    incident_severity
        VARCHAR(100),


    -- Event information

    number_of_vehicles_involved
        INTEGER,


    -- Measures

    total_claim_amount
        DECIMAL(15,2),

    injury_claim
        DECIMAL(15,2),

    property_claim
        DECIMAL(15,2),

    vehicle_claim
        DECIMAL(15,2),


    -- Fraud indicator

    fraud_reported
        VARCHAR(10),


    -- Foreign keys

    FOREIGN KEY (
        policy_key
    )
    REFERENCES dim_policy(policy_key),


    FOREIGN KEY (
        policyholder_key
    )
    REFERENCES dim_policyholder(policyholder_key),


    FOREIGN KEY (
        vehicle_key
    )
    REFERENCES dim_vehicle(vehicle_key),


    FOREIGN KEY (
        incident_date_key
    )
    REFERENCES dim_date(date_key)
);


-- ============================================================
-- INSERT CLAIMS INTO FACT TABLE
-- ============================================================

INSERT INTO fact_claims (

    claim_key,

    policy_key,

    policyholder_key,

    vehicle_key,

    incident_date_key,

    incident_type,

    collision_type,

    incident_severity,

    number_of_vehicles_involved,

    total_claim_amount,

    injury_claim,

    property_claim,

    vehicle_claim,

    fraud_reported
)


SELECT

    ROW_NUMBER() OVER (
        ORDER BY r.incident_date,
                 r.policy_number
    ) AS claim_key,


    -- Policy surrogate key

    p.policy_key,


    -- Historical SCD2 policyholder key

    ph.policyholder_key,


    -- Vehicle surrogate key

    v.vehicle_key,


    -- Date surrogate key

    d.date_key,


    -- Claim descriptive attributes

    r.incident_type,

    r.collision_type,

    r.incident_severity,


    r.number_of_vehicles_involved,


    -- Measures

    r.total_claim_amount,

    r.injury_claim,

    r.property_claim,

    r.vehicle_claim,


    -- Fraud indicator

    r.fraud_reported


FROM raw_claims r


-- ============================================================
-- JOIN POLICY DIMENSION
-- ============================================================

JOIN dim_policy p

    ON r.policy_number =
       p.policy_number


-- ============================================================
-- JOIN SCD2 POLICYHOLDER
-- POINT-IN-TIME JOIN
-- ============================================================

JOIN dim_policyholder ph

    ON r.policyholder_id =
       ph.policyholder_id

    AND r.incident_date >=
        ph.valid_from

    AND (
        r.incident_date <
        ph.valid_to

        OR ph.valid_to IS NULL
    )


-- ============================================================
-- JOIN VEHICLE DIMENSION
-- ============================================================

JOIN dim_vehicle v

    ON r.auto_make =
       v.auto_make

    AND r.auto_model =
        v.auto_model

    AND r.auto_year =
        v.auto_year


-- ============================================================
-- JOIN DATE DIMENSION
-- ============================================================

JOIN dim_date d

    ON r.incident_date =
       d.full_date;