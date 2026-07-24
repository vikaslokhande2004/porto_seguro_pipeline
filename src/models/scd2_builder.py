from pathlib import Path
import pandas as pd


SILVER_DIR = Path("silver")
GOLD_DIR = Path("gold")


TRACKED_COLUMNS = [
    "zip_code",
    "relationship",
    "occupation",
]


def normalize_value(value):
    """
    Normalize values so that NaN and None
    are treated consistently.
    """
    if pd.isna(value):
        return None

    return str(value).strip()


def values_changed(
    previous_values,
    current_values
):
    """Check whether tracked attributes changed."""

    for column in TRACKED_COLUMNS:

        previous = normalize_value(
            previous_values.get(column)
        )

        current = normalize_value(
            current_values.get(column)
        )

        if previous != current:
            return True

    return False


def build_scd2_history(
    snapshots,
    natural_key="policyholder_id",
):
    """
    Build a complete SCD Type 2 history.

    snapshots:
        List of (DataFrame, snapshot_date)
        in chronological order.
    """

    history = []

    current_state = {}

    surrogate_key = 1

    for df, snapshot_date in snapshots:

        df = df.copy()

        for _, row in df.iterrows():

            natural_key_value = row[
                natural_key
            ]

            current_values = {
                column: row[column]
                for column in TRACKED_COLUMNS
            }

            # First time seeing policyholder
            if natural_key_value not in current_state:

                record = row.to_dict()

                record[
                    "policyholder_key"
                ] = surrogate_key

                record[
                    "valid_from"
                ] = snapshot_date

                record[
                    "valid_to"
                ] = None

                record[
                    "is_current"
                ] = True

                history.append(record)

                current_state[
                    natural_key_value
                ] = (
                    surrogate_key,
                    current_values,
                )

                surrogate_key += 1

                continue

            previous_key, previous_values = (
                current_state[
                    natural_key_value
                ]
            )

            # Check whether tracked attributes changed
            if values_changed(
                previous_values,
                current_values
            ):

                # Close previous SCD2 version
                for record in history:

                    if (
                        record[
                            "policyholder_key"
                        ]
                        == previous_key
                        and record[
                            "is_current"
                        ]
                    ):

                        record[
                            "valid_to"
                        ] = snapshot_date

                        record[
                            "is_current"
                        ] = False

                        break

                # Create new version
                record = row.to_dict()

                record[
                    "policyholder_key"
                ] = surrogate_key

                record[
                    "valid_from"
                ] = snapshot_date

                record[
                    "valid_to"
                ] = None

                record[
                    "is_current"
                ] = True

                history.append(record)

                current_state[
                    natural_key_value
                ] = (
                    surrogate_key,
                    current_values,
                )

                surrogate_key += 1

    result = pd.DataFrame(history)

    result = result[
        [
            "policyholder_key",
            "policyholder_id",
            "age",
            "sex",
            "education_level",
            "occupation",
            "hobbies",
            "relationship",
            "zip_code",
            "valid_from",
            "valid_to",
            "is_current",
        ]
    ]

    return result


def main():

    GOLD_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    day0 = pd.read_csv(
        SILVER_DIR
        / "policyholder_snapshot_day0.csv"
    )

    day90 = pd.read_csv(
        SILVER_DIR
        / "policyholder_snapshot_day90.csv"
    )

    day180 = pd.read_csv(
        SILVER_DIR
        / "policyholder_snapshot_day180.csv"
    )

    snapshots = [
        (
            day0,
            "2024-01-01"
        ),
        (
            day90,
            "2024-03-31"
        ),
        (
            day180,
            "2024-06-29"
        ),
    ]

    dim_policyholder = build_scd2_history(
        snapshots
    )

    dim_policyholder[
        "valid_from"
    ] = pd.to_datetime(
        dim_policyholder[
            "valid_from"
        ]
    )

    dim_policyholder[
        "valid_to"
    ] = pd.to_datetime(
        dim_policyholder[
            "valid_to"
        ]
    )

    output = (
        GOLD_DIR
        / "dim_policyholder.csv"
    )

    dim_policyholder.to_csv(
        output,
        index=False
    )

    print(
        f"Created: {output}"
    )

    print(
        f"Total SCD2 rows: "
        f"{len(dim_policyholder)}"
    )

    print(
        "\nVersions per policyholder:"
    )

    print(
        dim_policyholder
        .groupby(
            "policyholder_id"
        )
        .size()
        .value_counts()
        .sort_index()
    )

    print(
        "\nSCD2 dimension:"
    )

    print(
        dim_policyholder.to_string(
            index=False
        )
    )


if __name__ == "__main__":
    main()