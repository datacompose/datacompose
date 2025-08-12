# %%

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("DatacomposeTest") \
    .getOrCreate()

# Test it
df = spark.range(10)
df.show()

# %%
