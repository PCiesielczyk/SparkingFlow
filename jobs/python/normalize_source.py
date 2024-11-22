import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

parser = argparse.ArgumentParser()

parser.add_argument('input_path')
parser.add_argument('output_path')

args = parser.parse_args()

spark = SparkSession.builder.appName("NormalizingSource").getOrCreate()

df = spark.read.csv(args.input_path, header=True, inferSchema=True)

columns_to_select = ["ASN", "Is Attack IP", "Is Account Takeover",
                     "Browser Name and Version", "OS Name and Version", "Device Type"]
df = df.select(*columns_to_select)

df = df.withColumn("Browser Name", regexp_replace(col("Browser Name and Version"), r"\s\d.*", "")) \
       .withColumn("OS Name", regexp_replace(col("OS Name and Version"), r"\s\d.*", ""))

df = df.drop("Browser Name and Version", "OS Name and Version")

df.write.mode("overwrite").option("header", "true").csv(args.output_path)

df.show()
spark.stop()
