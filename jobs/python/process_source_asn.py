import argparse

from pyspark.sql import SparkSession
import os

from pyspark.sql.functions import col

from utils.source_asn_processors import prepare_top_org_by_logs_graph, prepare_top_attacks_per_org, \
    prepare_account_takeover_graph, prepare_os_account_takeover_graph, prepare_os_attack_graph

parser = argparse.ArgumentParser()

parser.add_argument('input_source_path')
parser.add_argument('input_asn_path')
parser.add_argument('output_path')

args = parser.parse_args()
output_path = args.output_path

spark = SparkSession.builder.appName("ProcessSession").getOrCreate()

source_df = spark.read.csv(args.input_source_path, header=True, inferSchema=True)
asn_df = spark.read.csv(args.input_asn_path, header=True, inferSchema=True)

filtered_asn_df = asn_df.filter((col("name").isNotNull()) & (col("name") != ""))
enriched_df = source_df.join(filtered_asn_df, source_df["ASN"] == filtered_asn_df["asn"], "left_outer").drop("asn")


enriched_df.show(100)

os.makedirs(args.output_path, exist_ok=True)

prepare_top_org_by_logs_graph(enriched_df, output_path)
prepare_top_attacks_per_org(enriched_df, output_path)
prepare_account_takeover_graph(enriched_df, output_path)
prepare_os_account_takeover_graph(enriched_df, output_path)
prepare_os_attack_graph(enriched_df, output_path)

spark.stop()
