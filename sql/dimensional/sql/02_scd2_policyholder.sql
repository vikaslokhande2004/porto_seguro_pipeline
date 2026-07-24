-- ============================================================
-- PHASE 5 - DIMENSIONAL MODELING
-- 02_scd2_policyholder.sql
-- ============================================================

-- ============================================================
-- EXPECTED SOURCE TABLE
-- ============================================================
--
-- policyholder_snapshot_new
--
-- Columns:
--
-- policyholder_id
-- age
-- sex
-- education_level
-- occupation
-- hobbies
-- relationship
-- zip_code
-- snapshot_date
--
-- ============================================================


-- ============================================================
-- STEP 1
-- CLOSE EXISTING CURRENT RECORDS
-- WHEN TRACKED ATTRIBUTES HAVE CHANGED
-- ============================================================

UPDATE dim_policyholder d
SET
    valid_to = s.snapshot_date,
    is_current = FALSE

FROM policyholder_snapshot_new s

WHERE d.policyholder_id =
      s.policyholder_id

AND d.is_current = TRUE

AND (
       COALESCE(d.zip_code, '') <>
       COALESCE(s.zip_code, '')

    OR COALESCE(d.relationship, '') <>
       COALESCE(s.relationship, '')

    OR COALESCE(d.occupation, '') <>
       COALESCE(s.occupation, '')
);


-- ============================================================
-- STEP 2
-- INSERT NEW OR CHANGED POLICYHOLDER RECORDS
-- ============================================================

INSERT INTO dim_policyholder (
    policyholder_id,
    age,
    sex,
    education_level,
    occupation,
    hobbies,
    relationship,
    zip_code,
    valid_from,
    valid_to,
    is_current
)

SELECT
    s.policyholder_id,
    s.age,
    s.sex,
    s.education_level,
    s.occupation,
    s.hobbies,
    s.relationship,
    s.zip_code,

    s.snapshot_date,

    NULL,

    TRUE

FROM policyholder_snapshot_new s

WHERE NOT EXISTS (

    SELECT 1

    FROM dim_policyholder d

    WHERE d.policyholder_id =
          s.policyholder_id

    AND d.is_current = TRUE

    AND COALESCE(d.zip_code, '') =
        COALESCE(s.zip_code, '')

    AND COALESCE(d.relationship, '') =
        COALESCE(s.relationship, '')

    AND COALESCE(d.occupation, '') =
        COALESCE(s.occupation, '')
);


-- ============================================================
-- STEP 3
-- CHECK SCD TYPE 2 HISTORY
-- ============================================================

SELECT
    policyholder_id,
    COUNT(*) AS version_count

FROM dim_policyholder

GROUP BY policyholder_id

ORDER BY policyholder_id;


-- ============================================================
-- STEP 4
-- VERIFY ONLY ONE CURRENT VERSION EXISTS
-- ============================================================

SELECT
    policyholder_id,
    COUNT(*) AS current_version_count

FROM dim_policyholder

WHERE is_current = TRUE

GROUP BY policyholder_id

HAVING COUNT(*) > 1;