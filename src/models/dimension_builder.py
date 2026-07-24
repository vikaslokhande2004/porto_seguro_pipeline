from pathlib import Path
import pandas as pd


BRONZE_FILE = Path(
    "bronze/insurance_claims.csv"
)

GOLD_DIR = Path("gold")


def build_dim_policy(df):

    columns = [
        "policy_number",
        "policy_state",
        "policy_csl",
        "policy_deductable",
        "policy_annual_premium",
        "umbrella_limit",
        "policy_bind_date",
    ]

    policy = (
        df[columns]
        .drop_duplicates(
            subset=["policy_number"]
        )
        .sort_values(
            "policy_number"
        )
        .reset_index(drop=True)
    )

    policy.insert(
        0,
        "policy_key",
        range(
            1,
            len(policy) + 1
        )
    )

    return policy


def build_dim_vehicle(df):

    columns = [
        "auto_make",
        "auto_model",
        "auto_year",
    ]

    vehicle = (
        df[columns]
        .drop_duplicates()
        .sort_values(
            [
                "auto_make",
                "auto_model",
                "auto_year",
            ]
        )
        .reset_index(drop=True)
    )

    vehicle.insert(
        0,
        "vehicle_key",
        range(
            1,
            len(vehicle) + 1
        )
    )

    return vehicle


def main():

    GOLD_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    df = pd.read_csv(
        BRONZE_FILE
    )

    dim_policy = build_dim_policy(
        df
    )

    dim_vehicle = build_dim_vehicle(
        df
    )

    policy_output = (
        GOLD_DIR
        / "dim_policy.csv"
    )

    vehicle_output = (
        GOLD_DIR
        / "dim_vehicle.csv"
    )

    dim_policy.to_csv(
        policy_output,
        index=False
    )

    dim_vehicle.to_csv(
        vehicle_output,
        index=False
    )

    print(
        f"Created: {policy_output}"
    )

    print(
        f"Policy rows: "
        f"{len(dim_policy)}"
    )

    print(
        f"Created: {vehicle_output}"
    )

    print(
        f"Vehicle rows: "
        f"{len(dim_vehicle)}"
    )


if __name__ == "__main__":
    main()