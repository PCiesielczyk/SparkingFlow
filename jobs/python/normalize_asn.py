import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

parser = argparse.ArgumentParser()

parser.add_argument('input_path')
parser.add_argument('output_path')

args = parser.parse_args()

spark = SparkSession.builder.appName("NormalizingAsn").getOrCreate()

df = spark.read.csv(args.input_path, header=True, inferSchema=True)

df = df.select("asn", "name", "domain")

df = df.withColumn("asn", regexp_replace(col("asn"), r"^AS", ""))

df.write.mode("overwrite").option("header", "true").csv(args.output_path)


df.show()
spark.stop()
