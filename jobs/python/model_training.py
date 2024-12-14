import argparse

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import os

parser = argparse.ArgumentParser()

parser.add_argument('input_path')
parser.add_argument('output_path')

args = parser.parse_args()
output_path = args.output_path

spark = SparkSession.builder.appName("ModelTraining").getOrCreate()

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

indexer_cols = ["Country", "Device Type"]
for col_name in indexer_cols:
    indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_index")
    df = indexer.fit(df).transform(df)

feature_cols = [
    "ASN",
    "Login Successful",
    "Is Attack IP",
    "Country_index",
    "Device Type_index"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df = assembler.transform(df)

df = df.withColumn("label", col("Is Account Takeover").cast("integer"))
df = df.select("features", "label")

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

lr = LogisticRegression(featuresCol="features", labelCol="label")

model = lr.fit(train_df)

test_df.show(100)

filtered_df = test_df.filter(test_df["label"] == 1)
filtered_df.show()

predictions = model.transform(test_df)
filtered_predictions = predictions.filter(predictions["label"] == 1)
filtered_predictions.show()

filtered_predictions.show(100)

evaluatorMulti = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")

auc = evaluator.evaluate(predictions)
acc = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "accuracy"})
f1 = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "f1"})
weightedPrecision = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "weightedPrecision"})
weightedRecall = evaluatorMulti.evaluate(predictions, {evaluatorMulti.metricName: "weightedRecall"})

print(f"AUC: {auc}")
print(f"ACC: {acc}")
print(f"F1: {f1}")
print(f"Precision: {weightedPrecision}")
print(f"Recall: {weightedRecall}")

spark.stop()
