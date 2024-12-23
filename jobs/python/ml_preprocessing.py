import argparse
import os

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()

parser.add_argument('input_path')
parser.add_argument('output_path')

args = parser.parse_args()
output_path = args.output_path

spark = SparkSession.builder.appName("MLPreprocessing").getOrCreate()

df = spark.read.csv(args.input_path, header=True, inferSchema=True)
os.makedirs(args.output_path, exist_ok=True)

selected_cols = [
    "Country",
    "ASN",
    "Device Type",
    "Login Successful",
    "Is Attack IP"
]

df = df.select(*selected_cols, "Is Account Takeover")

df = df.fillna({"Country": "unknown", "Device Type": "unknown"})

df.show()
df.write.mode("overwrite").option("header", "true").csv(args.output_path)
spark.stop()
