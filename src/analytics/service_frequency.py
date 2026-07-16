"""
=========================================================
RouteIntel
Service Frequency Analysis
=========================================================
"""

from pyspark.sql import functions as F

from src.config.settings import PARQUET_OUTPUT_DIR


def load_tables(spark):
    """
    Load required Parquet tables.
    """

    trips = spark.read.parquet(
        f"{PARQUET_OUTPUT_DIR}/trips"
    )

    calendar = spark.read.parquet(
        f"{PARQUET_OUTPUT_DIR}/calendar"
    )

    return trips, calendar


def trips_per_service(trips, calendar):
    """
    Number of trips for each service.
    """

    return (
        trips
        .groupBy("service_id")
        .count()
        .withColumnRenamed(
            "count",
            "total_trips"
        )
        .join(calendar, "service_id", "left")
        .orderBy(
            F.desc("total_trips")
        )
    )


def weekday_frequency(trips, calendar):
    """
    Trips operating on weekdays.
    """

    return (
        trips
        .join(calendar, "service_id")
        .groupBy(
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday"
        )
        .count()
        .orderBy(
            F.desc("count")
        )
    )


def weekend_frequency(trips, calendar):
    """
    Weekend services.
    """

    return (
        trips
        .join(calendar, "service_id")
        .groupBy(
            "saturday",
            "sunday"
        )
        .count()
        .orderBy(
            F.desc("count")
        )
    )


def service_summary(trips, calendar):
    """
    Summary for every service.
    """

    return (
        trips
        .groupBy("service_id")
        .agg(
            F.count("*").alias("total_trips"),
            F.countDistinct("route_id").alias("routes"),
            F.countDistinct("direction_id").alias("directions")
        )
        .join(calendar, "service_id", "left")
        .orderBy(
            F.desc("total_trips")
        )
    )


def main(spark):
    """
    Execute Service Frequency Analysis using an existing Spark session.
    """

    trips, calendar = load_tables(spark)

    print("=" * 70)
    print("ROUTEINTEL - SERVICE FREQUENCY")
    print("=" * 70)

    print("\nTrips Per Service\n")

    trips_per_service(
        trips,
        calendar
    ).show(
        20,
        truncate=False
    )

    print("\nWeekday Frequency\n")

    weekday_frequency(
        trips,
        calendar
    ).show(
        truncate=False
    )

    print("\nWeekend Frequency\n")

    weekend_frequency(
        trips,
        calendar
    ).show(
        truncate=False
    )

    print("\nService Summary\n")

    service_summary(
        trips,
        calendar
    ).show(
        20,
        truncate=False
    )