import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()

parser.add_argument('partition_num', type=int)
parser.add_argument('input_path')
parser.add_argument('output_path')

args = parser.parse_args()

spark = SparkSession.builder.appName("Partitioning").getOrCreate()

df = spark.read.csv(args.input_path, header=True, inferSchema=True)

df = df.repartition(args.partition_num)
df.show()
df.write.mode("overwrite").csv(args.output_path)

spark.stop()
