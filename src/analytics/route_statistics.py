"""
=========================================================
RouteIntel
Route Statistics
=========================================================
"""

from pyspark.sql import functions as F

from src.config.settings import PARQUET_OUTPUT_DIR


def load_tables(spark):
    """
    Load required GTFS Parquet tables.
    """

    routes = spark.read.parquet(
        f"{PARQUET_OUTPUT_DIR}/routes"
    )

    trips = spark.read.parquet(
        f"{PARQUET_OUTPUT_DIR}/trips"
    )

    stop_times = spark.read.parquet(
        f"{PARQUET_OUTPUT_DIR}/stop_times"
    )

    return routes, trips, stop_times


def trips_per_route(routes, trips):
    """
    Number of trips for each route.
    """

    return (
        trips
        .groupBy("route_id")
        .count()
        .withColumnRenamed("count", "total_trips")
        .join(routes, "route_id", "left")
        .select(
            "route_id",
            "route_short_name",
            "route_long_name",
            "total_trips"
        )
        .orderBy(
            F.desc("total_trips")
        )
    )


def stops_per_route(routes, trips, stop_times):
    """
    Number of unique stops for each route.
    """

    return (
        trips
        .join(stop_times, "trip_id")
        .groupBy("route_id")
        .agg(
            F.countDistinct("stop_id").alias("unique_stops")
        )
        .join(routes, "route_id", "left")
        .select(
            "route_id",
            "route_short_name",
            "route_long_name",
            "unique_stops"
        )
        .orderBy(
            F.desc("unique_stops")
        )
    )


def busiest_routes(routes, trips, stop_times):
    """
    Routes with the most stop visits.
    """

    return (
        trips
        .join(stop_times, "trip_id")
        .groupBy("route_id")
        .count()
        .withColumnRenamed(
            "count",
            "total_stop_visits"
        )
        .join(routes, "route_id", "left")
        .select(
            "route_id",
            "route_short_name",
            "route_long_name",
            "total_stop_visits"
        )
        .orderBy(
            F.desc("total_stop_visits")
        )
    )


def route_summary(routes, trips, stop_times):
    """
    Summary statistics for every route.
    """

    return (
        trips
        .join(stop_times, "trip_id")
        .groupBy("route_id")
        .agg(
            F.countDistinct("trip_id").alias("trips"),
            F.countDistinct("stop_id").alias("stops"),
            F.count("*").alias("stop_visits")
        )
        .join(routes, "route_id", "left")
        .select(
            "route_id",
            "trips",
            "stops",
            "stop_visits",
            "agency_id",
            "route_short_name",
            "route_long_name",
            "route_type"
        )
        .orderBy(
            F.desc("stop_visits")
        )
    )


def main(spark):
    """
    Execute Route Statistics using an existing Spark session.
    """

    routes, trips, stop_times = load_tables(spark)

    print("=" * 70)
    print("ROUTEINTEL - ROUTE STATISTICS")
    print("=" * 70)

    print("\nTop Routes by Trips\n")

    trips_per_route(
        routes,
        trips
    ).show(
        20,
        truncate=False
    )

    print("\nRoutes with Most Stops\n")

    stops_per_route(
        routes,
        trips,
        stop_times
    ).show(
        20,
        truncate=False
    )

    print("\nBusiest Routes\n")

    busiest_routes(
        routes,
        trips,
        stop_times
    ).show(
        20,
        truncate=False
    )

    print("\nRoute Summary\n")

    route_summary(
        routes,
        trips,
        stop_times
    ).show(
        20,
        truncate=False
    )