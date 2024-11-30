import argparse

from pyspark.sql import SparkSession
from utils.session_processors import prepare_logins_per_minut_graph, \
    prepare_logins_per_day_graph, \
    prepare_location_table, \
    prepare_device_usage_table
import os

parser = argparse.ArgumentParser()

parser.add_argument('input_path')
parser.add_argument('output_path')

args = parser.parse_args()
output_path = args.output_path

spark = SparkSession.builder.appName("ProcessSession").getOrCreate()

df = spark.read.csv(args.input_path, header=True, inferSchema=True)
os.makedirs(args.output_path, exist_ok=True)

prepare_logins_per_minut_graph(df, output_path)
prepare_logins_per_day_graph(df, output_path)
prepare_location_table(df, output_path)
prepare_device_usage_table(df, output_path)

spark.stop()
