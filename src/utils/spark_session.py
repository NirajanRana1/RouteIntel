from pyspark.sql import SparkSession


def create_spark_session():
    """
    Create and configure a local Spark session.
    """

    spark = (
        SparkSession.builder
        .appName("RouteIntel")
        .master("local[*]")

        # Memory settings (recommended for 8GB RAM)
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")

        # Performance
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark