"""
RouteIntel Preprocessing Pipeline.

Pipeline Flow
-------------
1. Load extracted GTFS files
2. Clean data
3. Save cleaned data as Parquet
"""

import time

from src.ingestion.load_gtfs import load_gtfs_tables
from src.preprocessing.cleaner import clean_all_tables
from src.preprocessing.parquet_writer import save_all_tables


def run_preprocessing_pipeline() -> None:
    """
    Execute the complete preprocessing pipeline.
    """

    start_time = time.time()

    print("\n" + "=" * 70)
    print("ROUTEINTEL PREPROCESSING PIPELINE")
    print("=" * 70)

    # -------------------------------------------------
    # Step 1 - Load GTFS tables
    # -------------------------------------------------

    print("\nStep 1/3 - Loading GTFS tables...")

    tables = load_gtfs_tables()

    print(f"Loaded {len(tables)} tables.")

    # -------------------------------------------------
    # Step 2 - Clean GTFS tables
    # -------------------------------------------------

    print("\nStep 2/3 - Cleaning GTFS tables...")

    cleaned_tables = clean_all_tables(tables)

    print("Cleaning completed.")

    # -------------------------------------------------
    # Step 3 - Save as Parquet
    # -------------------------------------------------

    print("\nStep 3/3 - Saving Parquet files...")

    save_all_tables(cleaned_tables)

    elapsed = time.time() - start_time

    print("\n" + "=" * 70)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)

    print(f"Tables Processed : {len(cleaned_tables)}")
    print(f"Execution Time   : {elapsed:.2f} seconds")


if __name__ == "__main__":
    run_preprocessing_pipeline()