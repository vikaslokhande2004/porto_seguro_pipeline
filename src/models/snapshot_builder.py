from pathlib import Path
import pandas as pd


BRONZE_FILE = Path("bronze/insurance_claims.csv")
SILVER_DIR = Path("silver")

DAY0_DATE = "2024-01-01"
DAY90_DATE = "2024-03-31"
DAY180_DATE = "2024-06-29"

SAMPLE_SIZE = 30


def load_claims():
    """Load claims from Bronze layer."""
    df = pd.read_csv(BRONZE_FILE)

    required_columns = [
        "policy_number",
        "age",
        "insured_sex",
        "insured_education_level",
        "insured_occupation",
        "insured_hobbies",
        "insured_relationship",
        "insured_zip",
    ]

    missing = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required columns: {missing}"
        )

    return df


def create_policyholder_base(df):
    """
    Create one unique policyholder record
    from the Bronze claims data.

    policy_number is used as the stable
    policyholder business identifier for
    this project.
    """

    policyholders = (
        df[
            [
                "policy_number",
                "age",
                "insured_sex",
                "insured_education_level",
                "insured_occupation",
                "insured_hobbies",
                "insured_relationship",
                "insured_zip",
            ]
        ]
        .drop_duplicates(
            subset=["policy_number"]
        )
        .head(SAMPLE_SIZE)
        .copy()
    )

    policyholders = policyholders.rename(
        columns={
            "policy_number": "policyholder_id",
            "insured_sex": "sex",
            "insured_education_level": "education_level",
            "insured_occupation": "occupation",
            "insured_hobbies": "hobbies",
            "insured_relationship": "relationship",
            "insured_zip": "zip_code",
        }
    )

    # Keep zip_code as string because it is a
    # descriptive attribute, not a numeric measure.
    policyholders["zip_code"] = (
        policyholders["zip_code"]
        .astype("string")
    )

    # Convert policyholder ID to string as well.
    # This makes the natural key consistent.
    policyholders["policyholder_id"] = (
        policyholders["policyholder_id"]
        .astype("string")
    )

    return policyholders

def create_snapshot(
    base_df,
    snapshot_date,
    changes=None
):
    """Create one policyholder snapshot."""

    snapshot = base_df.copy()

    snapshot["snapshot_date"] = snapshot_date

    if changes:
        for policyholder_id, column_changes in changes.items():

            mask = (
                snapshot["policyholder_id"]
                == policyholder_id
            )

            for column, value in column_changes.items():
                snapshot.loc[mask, column] = value

    return snapshot[
        [
            "policyholder_id",
            "age",
            "sex",
            "education_level",
            "occupation",
            "hobbies",
            "relationship",
            "zip_code",
            "snapshot_date",
        ]
    ]


def main():

    SILVER_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    df = load_claims()

    base = create_policyholder_base(df)

    if len(base) < SAMPLE_SIZE:
        raise ValueError(
            f"Only {len(base)} unique policyholders found. "
            f"Need at least {SAMPLE_SIZE}."
        )

    # Day 0
    day0 = create_snapshot(
        base,
        DAY0_DATE
    )

    # Day 90
    day90_changes = {
        1: {"zip_code": "411001"},
        2: {"zip_code": "411002"},
        3: {"zip_code": "411003"},
        4: {"zip_code": "411004"},

        5: {"relationship": "not-in-family"},
        6: {"relationship": "not-in-family"},
        7: {"relationship": "not-in-family"},

        8: {"occupation": "exec-managerial"},
        9: {"occupation": "tech-support"},
    }

    day90 = create_snapshot(
        base,
        DAY90_DATE,
        day90_changes
    )

    # Day 180
    day180_changes = {
        1: {
            "zip_code": "411010"
        },
        5: {
            "relationship": "wife"
        },
        8: {
            "occupation": "manager"
        },
        10: {
            "zip_code": "411020"
        },
        11: {
            "relationship": "husband"
        },
        12: {
            "occupation": "craft-repair"
        },
    }

    day180 = create_snapshot(
        base,
        DAY180_DATE,
        day180_changes
    )

    day0.to_csv(
        SILVER_DIR / "policyholder_snapshot_day0.csv",
        index=False
    )

    day90.to_csv(
        SILVER_DIR / "policyholder_snapshot_day90.csv",
        index=False
    )

    day180.to_csv(
        SILVER_DIR / "policyholder_snapshot_day180.csv",
        index=False
    )

    print(
        "Created policyholder snapshots:"
    )

    print(
        f"Day 0:   {len(day0)} rows"
    )

    print(
        f"Day 90:  {len(day90)} rows"
    )

    print(
        f"Day 180: {len(day180)} rows"
    )


if __name__ == "__main__":
    main()