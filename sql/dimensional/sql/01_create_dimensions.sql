-- ============================================================
-- PHASE 5 - DIMENSIONAL MODELING
-- 01_create_dimensions.sql
-- ============================================================

-- ============================================================
-- DIM_DATE
-- ============================================================

CREATE TABLE dim_date (
    date_key       INTEGER PRIMARY KEY,
    full_date      DATE NOT NULL,
    day_of_week    VARCHAR(20),
    month          INTEGER,
    quarter        INTEGER,
    year           INTEGER,
    is_weekend     BOOLEAN
);


-- ============================================================
-- DIM_POLICY
-- ============================================================

CREATE TABLE dim_policy (
    policy_key              INTEGER PRIMARY KEY,
    policy_number           VARCHAR(50) NOT NULL,
    policy_state             VARCHAR(10),
    policy_csl               VARCHAR(20),
    policy_deductable        DECIMAL(12,2),
    policy_annual_premium    DECIMAL(12,2),
    umbrella_limit           DECIMAL(15,2),
    policy_bind_date         DATE
);


-- ============================================================
-- DIM_VEHICLE
-- ============================================================

CREATE TABLE dim_vehicle (
    vehicle_key    INTEGER PRIMARY KEY,
    auto_make      VARCHAR(100),
    auto_model     VARCHAR(100),
    auto_year      INTEGER
);


-- ============================================================
-- DIM_POLICYHOLDER
-- ============================================================

CREATE TABLE dim_policyholder (
    policyholder_key    INTEGER PRIMARY KEY,
    policyholder_id     VARCHAR(50) NOT NULL,
    age                 INTEGER,
    sex                 VARCHAR(20),
    education_level     VARCHAR(100),
    occupation          VARCHAR(100),
    hobbies             VARCHAR(200),
    relationship        VARCHAR(50),
    zip_code            VARCHAR(20),

    valid_from          DATE NOT NULL,
    valid_to            DATE,

    is_current          BOOLEAN NOT NULL
);