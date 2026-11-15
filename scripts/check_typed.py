from src.loaders import read_typed
from src.spark_session import get_spark
spark = get_spark("check-typed")
try:
    spark.sparkContext.setLogLevel("WARN")
    ratings = read_typed(spark, "title.ratings")
    ratings.printSchema()
    ratings.show(5, truncate=False)
finally:
    spark.stop()