from src.ingestion.load_gtfs import load_gtfs_tables


def validate_tables():
    """
    Generate a simple validation report for every GTFS table.
    """

    dataframes = load_gtfs_tables()

    print("\n" + "=" * 60)
    print("GTFS DATA VALIDATION REPORT")
    print("=" * 60)

    for table_name, df in dataframes.items():

        print(f"\nTable: {table_name}")
        print("-" * 40)

        print(f"Rows: {df.count():,}")
        print(f"Columns: {len(df.columns)}")

        print("\nSchema:")
        df.printSchema()

        print("\nFirst 5 Records:")
        df.show(5, truncate=False)

    print("\nValidation Complete")


if __name__ == "__main__":
    validate_tables()