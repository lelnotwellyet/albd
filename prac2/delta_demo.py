from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip, DeltaTable

# Start Spark with Delta Lake switched on
builder = (SparkSession.builder.appName("DeltaDemo").master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

path = "delta-students"          # folder where the table is saved

# 1. Create the table  -> this is VERSION 0
data = [(1, "Siddhant", 90), (2, "Shreya", 88), (3, "Shruti", 75),
        (4, "Chinmay", 90), (5, "Vishnu", 88), (6, "Nikita", 75)]
df = spark.createDataFrame(data, ["id", "name", "marks"])
df.write.format("delta").mode("overwrite").save(path)
print("Version 0: table created")
spark.read.format("delta").load(path).orderBy("id").show()

# 2. ACID transaction: update one row  -> this is VERSION 1
dt = DeltaTable.forPath(spark, path)
dt.update("name = 'Nikita'", {"marks": "80"})
print("Version 1: after UPDATE")
spark.read.format("delta").load(path).orderBy("id").show()

# 3. Time travel: read the table as it was at version 0
print("TIME TRAVEL: reading version 0 again")
spark.read.format("delta").option("versionAsOf", 0).load(path).orderBy("id").show()

# 4. History: the list of all versions
print("History of the table")
dt.history().select("version", "timestamp", "operation").show(truncate=False)

spark.stop()
