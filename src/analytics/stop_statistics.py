"""
=========================================================
RouteIntel
Stop Statistics
=========================================================
"""

from pyspark.sql import functions as F

from src.config.settings import PARQUET_OUTPUT_DIR


def load_tables(spark):
    """
    Load required GTFS Parquet tables.
    """

    stops = spark.read.parquet(
        f"{PARQUET_OUTPUT_DIR}/stops"
    )

    stop_times = spark.read.parquet(
        f"{PARQUET_OUTPUT_DIR}/stop_times"
    )

    return stops, stop_times


def busiest_stops(stops, stop_times, top_n=20):
    """
    Top busiest stops.
    """

    return (
        stop_times
        .groupBy("stop_id")
        .count()
        .withColumnRenamed(
            "count",
            "total_arrivals"
        )
        .join(
            stops,
            "stop_id",
            "left"
        )
        .select(
            "stop_id",
            "stop_name",
            "total_arrivals"
        )
        .orderBy(
            F.desc("total_arrivals")
        )
        .limit(top_n)
    )


def average_stop_sequence(stops, stop_times):
    """
    Average stop sequence.
    """

    return (
        stop_times
        .groupBy("stop_id")
        .agg(
            F.avg("stop_sequence").alias(
                "average_stop_sequence"
            )
        )
        .join(
            stops,
            "stop_id",
            "left"
        )
        .select(
            "stop_id",
            "stop_name",
            "average_stop_sequence"
        )
        .orderBy(
            F.desc("average_stop_sequence")
        )
    )


def stop_usage_summary(stops, stop_times):
    """
    Stop usage summary.
    """

    return (
        stop_times
        .groupBy("stop_id")
        .agg(
            F.count("*").alias(
                "total_visits"
            ),
            F.min("stop_sequence").alias(
                "first_sequence"
            ),
            F.max("stop_sequence").alias(
                "last_sequence"
            )
        )
        .join(
            stops,
            "stop_id",
            "left"
        )
        .select(
            "stop_id",
            "stop_name",
            "total_visits",
            "first_sequence",
            "last_sequence"
        )
        .orderBy(
            F.desc("total_visits")
        )
    )


def main(spark):
    """
    Execute stop statistics.
    """

    stops, stop_times = load_tables(spark)

    print("=" * 70)
    print("ROUTEINTEL - STOP STATISTICS")
    print("=" * 70)

    print("\nTop 20 Busiest Stops\n")

    busiest_stops(
        stops,
        stop_times
    ).show(
        truncate=False
    )

    print("\nAverage Stop Sequence\n")

    average_stop_sequence(
        stops,
        stop_times
    ).show(
        20,
        truncate=False
    )

    print("\nStop Usage Summary\n")

    stop_usage_summary(
        stops,
        stop_times
    ).show(
        20,
        truncate=False
    )