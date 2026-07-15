from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.ingestion.load_gtfs import load_gtfs_tables


def validate_table(table_name: str, df: DataFrame) -> None:
    """
    Validate a single GTFS table.
    """

    print("\n" + "=" * 70)
    print(f"TABLE: {table_name.upper()}")
    print("=" * 70)

    print(f"Rows    : {df.count():,}")
    print(f"Columns : {len(df.columns)}")

    print("\nMissing Values")

    missing = df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])

    missing.show(truncate=False)

    print("\nSchema")

    df.printSchema()


def validate_gtfs():
    """
    Validate every GTFS table.
    """

    tables = load_gtfs_tables()

    for table_name, df in tables.items():
        validate_table(table_name, df)


if __name__ == "__main__":
    validate_gtfs()