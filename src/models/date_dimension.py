from pathlib import Path
import pandas as pd


GOLD_DIR = Path("gold")


def generate_dim_date(
    start="2023-01-01",
    end="2025-12-31"
):

    dates = pd.date_range(
        start=start,
        end=end,
        freq="D"
    )

    df = pd.DataFrame(
        {
            "full_date": dates
        }
    )

    df[
        "date_key"
    ] = (
        df["full_date"]
        .dt.strftime("%Y%m%d")
        .astype(int)
    )

    df[
        "day_of_week"
    ] = (
        df["full_date"]
        .dt.day_name()
    )

    df[
        "month"
    ] = (
        df["full_date"]
        .dt.month
    )

    df[
        "quarter"
    ] = (
        df["full_date"]
        .dt.quarter
    )

    df[
        "year"
    ] = (
        df["full_date"]
        .dt.year
    )

    df[
        "is_weekend"
    ] = (
        df["full_date"]
        .dt.dayofweek
        .isin([5, 6])
    )

    return df[
        [
            "date_key",
            "full_date",
            "day_of_week",
            "month",
            "quarter",
            "year",
            "is_weekend",
        ]
    ]


def main():

    GOLD_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    dim_date = generate_dim_date()

    output = (
        GOLD_DIR
        / "dim_date.csv"
    )

    dim_date.to_csv(
        output,
        index=False
    )

    print(
        f"Created: {output}"
    )

    print(
        f"Total dates: {len(dim_date)}"
    )

    print(
        dim_date.head()
    )


if __name__ == "__main__":
    main()