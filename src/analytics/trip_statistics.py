"""
=========================================================
RouteIntel
Trip Statistics
=========================================================
"""

from pyspark.sql import functions as F

from src.config.settings import PARQUET_OUTPUT_DIR


def load_tables(spark):
    """
    Load required GTFS Parquet tables.
    """

    trips = spark.read.parquet(
        f"{PARQUET_OUTPUT_DIR}/trips"
    )

    stop_times = spark.read.parquet(
        f"{PARQUET_OUTPUT_DIR}/stop_times"
    )

    routes = spark.read.parquet(
        f"{PARQUET_OUTPUT_DIR}/routes"
    )

    return trips, stop_times, routes


def stops_per_trip(trips, stop_times, routes):
    """
    Number of stops in each trip.
    """

    return (
        stop_times
        .groupBy("trip_id")
        .agg(
            F.count("*").alias("total_stops")
        )
        .join(trips, "trip_id", "left")
        .join(routes, "route_id", "left")
        .select(
            "trip_id",
            "route_id",
            "route_short_name",
            "service_id",
            "direction_id",
            "total_stops"
        )
        .orderBy(
            F.desc("total_stops")
        )
    )


def first_last_stop(trips, stop_times, routes):
    """
    First and last stop sequence for each trip.
    """

    return (
        stop_times
        .groupBy("trip_id")
        .agg(
            F.min("stop_sequence").alias("first_stop"),
            F.max("stop_sequence").alias("last_stop")
        )
        .join(trips, "trip_id", "left")
        .join(routes, "route_id", "left")
        .select(
            "trip_id",
            "route_short_name",
            "service_id",
            "first_stop",
            "last_stop"
        )
        .orderBy(
            F.desc("last_stop")
        )
    )


def average_stop_sequence(trips, stop_times, routes):
    """
    Average stop sequence for every trip.
    """

    return (
        stop_times
        .groupBy("trip_id")
        .agg(
            F.avg("stop_sequence").alias(
                "average_stop_sequence"
            )
        )
        .join(trips, "trip_id", "left")
        .join(routes, "route_id", "left")
        .select(
            "trip_id",
            "route_short_name",
            "average_stop_sequence"
        )
        .orderBy(
            F.desc("average_stop_sequence")
        )
    )


def trip_summary(trips, stop_times, routes):
    """
    Overall trip statistics.
    """

    return (
        stop_times
        .groupBy("trip_id")
        .agg(
            F.count("*").alias("total_stops"),
            F.min("arrival_time").alias("first_arrival"),
            F.max("departure_time").alias("last_departure")
        )
        .join(trips, "trip_id", "left")
        .join(routes, "route_id", "left")
        .select(
            "trip_id",
            "route_short_name",
            "service_id",
            "direction_id",
            "total_stops",
            "first_arrival",
            "last_departure"
        )
        .orderBy(
            F.desc("total_stops")
        )
    )


def main(spark):
    """
    Execute trip statistics.
    """

    trips, stop_times, routes = load_tables(spark)

    print("=" * 70)
    print("ROUTEINTEL - TRIP STATISTICS")
    print("=" * 70)

    print("\nTrips with Most Stops\n")
    stops_per_trip(
        trips,
        stop_times,
        routes
    ).show(
        20,
        truncate=False
    )

    print("\nFirst and Last Stop Sequence\n")
    first_last_stop(
        trips,
        stop_times,
        routes
    ).show(
        20,
        truncate=False
    )

    print("\nAverage Stop Sequence\n")
    average_stop_sequence(
        trips,
        stop_times,
        routes
    ).show(
        20,
        truncate=False
    )

    print("\nTrip Summary\n")
    trip_summary(
        trips,
        stop_times,
        routes
    ).show(
        20,
        truncate=False
    )


if __name__ == "__main__":
    from src.utils.spark_session import get_spark
    spark = get_spark()
    main(spark)