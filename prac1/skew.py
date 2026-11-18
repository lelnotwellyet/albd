import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand, sum as _sum

# Start Spark on 4 cores, with 8 shuffle partitions
spark = (SparkSession.builder.appName("SkewDemo").master("local[4]")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# 5 million rows. 90% get key 0, the rest get keys 1 to 999
n = 5_000_000
df = spark.range(n).withColumn(
    "key", when(col("id") < n * 0.9, 0).otherwise(col("id") % 999 + 1))

print("PROBLEM: one key has 90% of the rows")
df.groupBy("key").count().orderBy(col("count").desc()).show(5)

# FIX step 1: add a random salt from 0 to 7
salted = df.withColumn("salt", (rand() * 8).cast("int"))

# FIX step 2: count per (key, salt) -> key 0 is now 8 small groups
part = salted.groupBy("key", "salt").count()
print("After salting: key 0 is split into 8 small groups")
part.filter(col("key") == 0).orderBy("salt").show()

# FIX step 3: add the small counts back together per key
final = (part.groupBy("key").agg(_sum("count").alias("count"))
         .orderBy(col("count").desc()))
print("FINAL: same answer as before")
final.show(5)

print("Open the Spark UI: http://localhost:4040")
time.sleep(120)   # keeps the Spark UI alive for 2 minutes
spark.stop()
