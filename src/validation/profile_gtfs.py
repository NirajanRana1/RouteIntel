from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

from src.ingestion.load_gtfs import load_gtfs_tables


def profile_gtfs():
    """
    Generate a GTFS profiling report.
    """

    tables = load_gtfs_tables()

    report = []

    for table_name, df in tables.items():

        rows = df.count()
        columns = len(df.columns)

        missing = (
            df.select([
                F.count(F.when(F.col(c).isNull(), c)).alias(c)
                for c in df.columns
            ])
            .collect()[0]
            .asDict()
        )

        total_missing = sum(missing.values())

        report.append({
            "Table": table_name,
            "Rows": rows,
            "Columns": columns,
            "Missing Cells": total_missing
        })

    output_dir = Path("outputs/data_profiles")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "gtfs_profile.csv"

    profile = pd.DataFrame(report)

    profile.to_csv(output_file, index=False)

    print("\nGTFS Profile\n")

    print(profile)

    print(f"\nProfile saved to:\n{output_file}")


if __name__ == "__main__":
    profile_gtfs()