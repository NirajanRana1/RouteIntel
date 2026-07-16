from pyspark.sql import SparkSession


def create_spark_session():
    """
    Create and configure Spark session.
    """

    spark = (
        SparkSession.builder
        .appName("RouteIntel")
        .master("local[*]")

        # Memory
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")

        # Performance
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def get_spark():
    """
    Return existing Spark session if available.
    """

    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = create_spark_session()

    return spark