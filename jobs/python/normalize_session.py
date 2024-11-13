import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()

parser.add_argument('input_path')
parser.add_argument('output_path')

args = parser.parse_args()

spark = SparkSession.builder.appName("NormalizingSession").getOrCreate()

# TODO: how to read csv partition dir
df = spark.read.csv(args.input_path, header=True, inferSchema=True)

# extracting columns

# extracting date/time

# save in output_path
spark.stop()
