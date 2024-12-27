import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument('input_path')
args = parser.parse_args()

spark = SparkSession.builder.appName("HealthCheck").getOrCreate()

df = spark.read.csv(args.input_path, header=True, inferSchema=True)
df.show()

spark.stop()
