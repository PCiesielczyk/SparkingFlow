import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

parser = argparse.ArgumentParser()

parser.add_argument('input_path')
parser.add_argument('output_path')

args = parser.parse_args()

spark = SparkSession.builder.appName("NormalizingSession").getOrCreate()

df = spark.read.csv(args.input_path, header=True, inferSchema=True)

df = df.withColumn("Login Date", split(col("Login Timestamp"), " ").getItem(0)) \
       .withColumn("Login Time", split(col("Login Timestamp"), " ").getItem(1))

columns_to_select = ["Login Date", "Login Time", "Device Type", "Login Successful", "Country", "Region", "City"]
df = df.select(*columns_to_select)

df.createOrReplaceTempView("session")

df.write.mode("overwrite").option("header", "true").csv(args.output_path)

df.show()
spark.stop()
