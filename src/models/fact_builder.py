from pathlib import Path
import pandas as pd


BRONZE_FILE = Path(
    "bronze/insurance_claims.csv"
)

GOLD_DIR = Path("gold")


def build_fact_claims(
    claims,
    dim_policy,
    dim_policyholder,
    dim_vehicle,
    dim_date,
):

    # --------------------------------------------------
    # 1. Prepare claim date
    # --------------------------------------------------

    claims = claims.copy()

    claims[
        "incident_date"
    ] = pd.to_datetime(
        claims[
            "incident_date"
        ]
    )

    # --------------------------------------------------
    # 2. Join dim_policy
    # --------------------------------------------------

    fact = claims.merge(
        dim_policy[
            [
                "policy_key",
                "policy_number",
            ]
        ],
        on="policy_number",
        how="left",
        validate="many_to_one",
    )

    # --------------------------------------------------
    # 3. Join dim_vehicle
    # --------------------------------------------------

    fact = fact.merge(
        dim_vehicle[
            [
                "vehicle_key",
                "auto_make",
                "auto_model",
                "auto_year",
            ]
        ],
        on=[
            "auto_make",
            "auto_model",
            "auto_year",
        ],
        how="left",
        validate="many_to_one",
    )

    # --------------------------------------------------
    # 4. Resolve SCD Type 2 policyholder key
    # --------------------------------------------------

    ph = dim_policyholder.copy()

    ph[
        "valid_from"
    ] = pd.to_datetime(
        ph[
            "valid_from"
        ]
    )

    ph[
        "valid_to"
    ] = pd.to_datetime(
        ph[
            "valid_to"
        ]
    )

    fact = fact.merge(
        ph[
            [
                "policyholder_key",
                "policyholder_id",
                "valid_from",
                "valid_to",
            ]
        ],
        left_on="policy_number",
        right_on="policyholder_id",
        how="left",
    )

    valid_mask = (
        (fact["incident_date"]
         >= fact["valid_from"])
        &
        (
            fact["valid_to"].isna()
            |
            (
                fact["incident_date"]
                < fact["valid_to"]
            )
        )
    )

    fact = fact[
        valid_mask
    ].copy()

    # --------------------------------------------------
    # 5. Join dim_date
    # --------------------------------------------------

    dim_date = dim_date.copy()

    dim_date[
        "full_date"
    ] = pd.to_datetime(
        dim_date[
            "full_date"
        ]
    )

    fact = fact.merge(
        dim_date[
            [
                "date_key",
                "full_date",
            ]
        ],
        left_on="incident_date",
        right_on="full_date",
        how="left",
        validate="many_to_one",
    )

    # --------------------------------------------------
    # 6. Generate claim surrogate key
    # --------------------------------------------------

    fact = fact.reset_index(
        drop=True
    )

    fact.insert(
        0,
        "claim_key",
        range(
            1,
            len(fact) + 1
        )
    )

    # --------------------------------------------------
    # 7. Select final fact columns
    # --------------------------------------------------

    fact = fact[
        [
            "claim_key",
            "policy_key",
            "policyholder_key",
            "vehicle_key",
            "date_key",
            "incident_type",
            "collision_type",
            "incident_severity",
            "number_of_vehicles_involved",
            "total_claim_amount",
            "injury_claim",
            "property_claim",
            "vehicle_claim",
            "fraud_reported",
        ]
    ]

    fact = fact.rename(
        columns={
            "date_key":
                "incident_date_key"
        }
    )

    return fact


def main():

    GOLD_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    claims = pd.read_csv(
        BRONZE_FILE
    )

    dim_policy = pd.read_csv(
        GOLD_DIR
        / "dim_policy.csv"
    )

    dim_vehicle = pd.read_csv(
        GOLD_DIR
        / "dim_vehicle.csv"
    )

    dim_policyholder = pd.read_csv(
        GOLD_DIR
        / "dim_policyholder.csv"
    )

    dim_date = pd.read_csv(
        GOLD_DIR
        / "dim_date.csv"
    )

    fact_claims = build_fact_claims(
        claims,
        dim_policy,
        dim_policyholder,
        dim_vehicle,
        dim_date,
    )

    output = (
        GOLD_DIR
        / "fact_claims.csv"
    )

    fact_claims.to_csv(
        output,
        index=False
    )

    print(
        f"Created: {output}"
    )

    print(
        f"Fact rows: "
        f"{len(fact_claims)}"
    )

    print(
        "\nNull foreign keys:"
    )

    print(
        fact_claims[
            [
                "policy_key",
                "policyholder_key",
                "vehicle_key",
                "incident_date_key",
            ]
        ]
        .isna()
        .sum()
    )


if __name__ == "__main__":
    main()