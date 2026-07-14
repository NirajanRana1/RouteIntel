from pathlib import Path

from pyspark.sql import DataFrame

from src.config.settings import GTFS_EXTRACTED_DIR
from src.utils.spark_session import create_spark_session


def load_gtfs_tables() -> dict[str, DataFrame]:
    """
    Load every GTFS text file into a PySpark DataFrame.
    """

    spark = create_spark_session()

    dataframes = {}

    txt_files = sorted(Path(GTFS_EXTRACTED_DIR).glob("*.txt"))

    if not txt_files:
        print("No GTFS files found.")
        return dataframes

    print("\nLoading GTFS files...\n")

    for file in txt_files:

        table_name = file.stem

        print(f"Loading {table_name}...")

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(file))
        )

        dataframes[table_name] = df

        print(f"Rows: {df.count():,}")
        print(f"Columns: {len(df.columns)}")
        print("-" * 40)

    return dataframes


def main():

    dfs = load_gtfs_tables()

    print("\nLoaded Tables")

    for table in dfs:
        print(f"✓ {table}")


if __name__ == "__main__":
    main()