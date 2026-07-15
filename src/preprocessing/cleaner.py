"""
Generic GTFS cleaning engine.

This module applies common cleaning operations to every GTFS table:
- Remove duplicate rows
- Remove rows with missing required fields
- Return cleaned Spark DataFrames
"""

from pyspark.sql import DataFrame

from src.preprocessing.rules import REQUIRED_COLUMNS


def clean_table(table_name: str, df: DataFrame) -> DataFrame:
    """
    Clean a single GTFS table.

    Parameters
    ----------
    table_name : str
        Name of the GTFS table.

    df : DataFrame
        Spark DataFrame to clean.

    Returns
    -------
    DataFrame
        Cleaned Spark DataFrame.
    """

    # Remove duplicate rows
    cleaned_df = df.dropDuplicates()

    # Get required columns for this table
    required_columns = REQUIRED_COLUMNS.get(table_name, [])

    # Remove rows with missing required values
    if required_columns:
        cleaned_df = cleaned_df.dropna(subset=required_columns)

    return cleaned_df


def clean_all_tables(
    tables: dict[str, DataFrame],
) -> dict[str, DataFrame]:
    """
    Clean every GTFS table.

    Parameters
    ----------
    tables : dict[str, DataFrame]
        Dictionary of Spark DataFrames.

    Returns
    -------
    dict[str, DataFrame]
        Dictionary of cleaned Spark DataFrames.
    """

    cleaned_tables = {}

    for table_name, df in tables.items():

        print(f"Cleaning {table_name}...")

        cleaned_tables[table_name] = clean_table(
            table_name,
            df,
        )

    return cleaned_tables