"""
=========================================================
RouteIntel
Travel Time Analysis
=========================================================
"""

from pyspark.sql import functions as F

from src.utils.spark_session import get_spark
from src.config.settings import PARQUET_OUTPUT_DIR


def load_tables(spark):
    """
    Load GTFS tables.
    """

    trips = spark.read.parquet(
        f"{PARQUET_OUTPUT_DIR}/trips"
    )

    routes = spark.read.parquet(
        f"{PARQUET_OUTPUT_DIR}/routes"
    )

    stop_times = spark.read.parquet(
        f"{PARQUET_OUTPUT_DIR}/stop_times"
    )

    return trips, routes, stop_times


def add_time_columns(df):
    """
    Convert GTFS HH:MM:SS into seconds.
    """

    arrival_seconds = (
        F.split(F.col("arrival_time"), ":").getItem(0).cast("int") * 3600
        + F.split(F.col("arrival_time"), ":").getItem(1).cast("int") * 60
        + F.split(F.col("arrival_time"), ":").getItem(2).cast("int")
    )

    departure_seconds = (
        F.split(F.col("departure_time"), ":").getItem(0).cast("int") * 3600
        + F.split(F.col("departure_time"), ":").getItem(1).cast("int") * 60
        + F.split(F.col("departure_time"), ":").getItem(2).cast("int")
    )

    return (
        df
        .withColumn("arrival_seconds", arrival_seconds)
        .withColumn("departure_seconds", departure_seconds)
    )


def build_trip_duration(trips, routes, stop_times):
    """
    Build trip duration dataframe once.
    """

    stop_times = add_time_columns(stop_times)

    duration = (
        stop_times
        .groupBy("trip_id")
        .agg(
            F.min("arrival_seconds").alias("start"),
            F.max("departure_seconds").alias("end"),
            F.count("*").alias("total_stops")
        )
        .withColumn(
            "travel_minutes",
            (F.col("end") - F.col("start")) / 60
        )
        .filter(
            F.col("travel_minutes") >= 1
        )
        .filter(
            F.col("travel_minutes") <= 480
        )
        .join(
            trips,
            "trip_id"
        )
        .join(
            routes,
            "route_id"
        )
        .cache()
    )

    duration.count()

    return duration


def longest_trips(duration):
    """
    Longest trips.
    """

    return (
        duration
        .select(
            "trip_id",
            "route_short_name",
            "service_id",
            "direction_id",
            "total_stops",
            F.round(
                "travel_minutes",
                2
            ).alias("travel_minutes")
        )
        .orderBy(
            F.desc("travel_minutes")
        )
    )


def shortest_trips(duration):
    """
    Shortest trips.
    """

    return (
        duration
        .select(
            "trip_id",
            "route_short_name",
            "service_id",
            "direction_id",
            "total_stops",
            F.round(
                "travel_minutes",
                2
            ).alias("travel_minutes")
        )
        .orderBy(
            F.asc("travel_minutes")
        )
    )


def average_route_duration(duration):
    """
    Average duration per route.
    """

    return (
        duration
        .groupBy(
            "route_id",
            "route_short_name",
            "route_long_name"
        )
        .agg(
            F.round(
                F.avg("travel_minutes"),
                2
            ).alias("average_minutes")
        )
        .select(
            "route_short_name",
            "average_minutes"
        )
        .orderBy(
            F.desc("average_minutes")
        )
    )


def travel_time_summary(duration):
    """
    Overall travel time summary.
    """

    return (
        duration
        .select(
            F.round(
                F.avg("travel_minutes"),
                2
            ).alias("average_minutes"),
            F.round(
                F.min("travel_minutes"),
                2
            ).alias("minimum_minutes"),
            F.round(
                F.max("travel_minutes"),
                2
            ).alias("maximum_minutes")
        )
    )


def main(spark):
    """
    Execute travel time analysis.
    """

    trips, routes, stop_times = load_tables(spark)

    duration = build_trip_duration(
        trips,
        routes,
        stop_times
    )

    print("=" * 70)
    print("ROUTEINTEL - TRAVEL TIME ANALYSIS")
    print("=" * 70)

    print("\nLongest Trips\n")

    longest_trips(
        duration
    ).show(
        20,
        truncate=False
    )

    print("\nShortest Trips\n")

    shortest_trips(
        duration
    ).show(
        20,
        truncate=False
    )

    print("\nAverage Route Duration\n")

    average_route_duration(
        duration
    ).show(
        20,
        truncate=False
    )

    print("\nTravel Time Summary\n")

    travel_time_summary(
        duration
    ).show(
        truncate=False
    )


if __name__ == "__main__":
    spark = get_spark()
    main(spark)