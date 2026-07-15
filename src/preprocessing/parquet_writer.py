from pathlib import Path

from pyspark.sql import DataFrame

from src.config.settings import (
    PARQUET_OUTPUT_DIR,
    PARQUET_WRITE_MODE,
    PARQUET_COMPRESSION,
)


def save_table(table_name: str, df: DataFrame) -> None:
    """
    Save a PySpark DataFrame as a Parquet dataset.
    """

    output_path = Path(PARQUET_OUTPUT_DIR) / table_name

    print(f"Saving {table_name}...")

    (
        df
        .repartition(8)
        .write
        .mode(PARQUET_WRITE_MODE)
        .option("compression", PARQUET_COMPRESSION)
        .parquet(str(output_path))
    )


def save_all_tables(tables: dict[str, DataFrame]) -> None:
    """
    Save all GTFS tables as Parquet files.
    """

    print("\nSaving Parquet files...\n")

    Path(PARQUET_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    for table_name, df in tables.items():
        save_table(table_name, df)

    print("\nAll Parquet files saved successfully.")