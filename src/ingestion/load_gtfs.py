from pathlib import Path

from pyspark.sql import DataFrame

from src.config.settings import GTFS_EXTRACTED_DIR
from src.utils.spark_session import create_spark_session


def load_gtfs_tables() -> dict[str, DataFrame]:
    """
    Load all GTFS text files into Spark DataFrames.
    """

    spark = create_spark_session()

    dataframes: dict[str, DataFrame] = {}

    txt_files = sorted(Path(GTFS_EXTRACTED_DIR).glob("*.txt"))

    if not txt_files:
        raise FileNotFoundError(
            f"No GTFS files found in {GTFS_EXTRACTED_DIR}"
        )

    for file in txt_files:

        table_name = file.stem

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(file))
        )

        dataframes[table_name] = df

    return dataframes


def main():

    dfs = load_gtfs_tables()

    print("\nLoaded Tables\n")

    for table in sorted(dfs):
        print(f"✓ {table}")


if __name__ == "__main__":
    main()