"""Shared Spark settings; connector follows the installed Spark version."""
import os
import pyspark
from pyspark.sql import SparkSession

KAFKA_PACKAGE = os.environ.get("KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.13:" + pyspark.__version__)

def get_spark(app_name="imdb-project", driver_memory="4g", kafka=False):
    builder = (SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "32"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1"))
    if kafka:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    return builder.getOrCreate()
