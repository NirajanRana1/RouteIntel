from pyspark.sql import SparkSession

from src.config.settings import SPARK_APP_NAME


def create_spark_session() -> SparkSession:
    """
    Create and configure the Spark session.
    """

    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.repl.eagerEval.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark