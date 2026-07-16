from src.utils.spark_session import get_spark
from src.config.settings import PARQUET_OUTPUT_DIR

spark = get_spark()

routes = spark.read.parquet(f"{PARQUET_OUTPUT_DIR}/routes")

routes.select(
    "route_id",
    "route_short_name",
    "route_long_name"
).show(20, truncate=False)