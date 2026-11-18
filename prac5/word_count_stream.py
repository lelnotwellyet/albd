import os
import tempfile

# Windows: give Spark a temp folder it is allowed to write to
os.makedirs(r"C:\bigdata\prac5\spark-temp", exist_ok=True)
tempfile.tempdir = r"C:\bigdata\prac5\spark-temp"

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# local[2] = 2 threads: one to receive data, one to process it
spark = (SparkSession.builder.master("local[2]").appName("WordCount")
         .config("spark.sql.shuffle.partitions", "2")   # makes each batch fast
         .config("spark.hadoop.fs.permissions.umask-mode", "000")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# 1. Read lines from the socket. Each line arrives in a column called "value"
lines = (spark.readStream.format("socket")
         .option("host", "localhost").option("port", 9999).load())

# 2. split cuts the line into words, explode puts each word on its own row
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# 3. Count how many times each word came
counts = words.groupBy("word").count()

# 4. Print the full result on the console after every batch
query = counts.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()   # keep running until Ctrl+C
