from src.utils.spark_session import create_spark_session

from src.analytics.route_statistics import main as route_stats
from src.analytics.trip_statistics import main as trip_stats
from src.analytics.stop_statistics import main as stop_stats
from src.analytics.travel_time_analysis import main as travel_time
from src.analytics.service_frequency import main as service_frequency


def main():

    spark = create_spark_session()

    print("=" * 50)
    print("RouteIntel")
    print("=" * 50)
    print(f"Spark Version: {spark.version}")

    route_stats(spark)
    trip_stats(spark)
    stop_stats(spark)
    travel_time(spark)
    service_frequency(spark)

    spark.stop()


if __name__ == "__main__":
    main()