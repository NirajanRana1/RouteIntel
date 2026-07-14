from src.utils.spark_session import create_spark_session


def main():
    spark = create_spark_session()

    print("=" * 50)
    print("RouteIntel")
    print("=" * 50)
    print(f"Spark Version: {spark.version}")

    spark.stop()


if __name__ == "__main__":
    main()