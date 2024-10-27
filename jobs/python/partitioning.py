from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Partitioning").getOrCreate()

df = spark.read.csv("/opt/airflow/archive/rba-dataset-sample.csv", header=True, inferSchema=True)

df = df.repartition(10)
df.show()
df.write.mode("overwrite").option("header", "true").csv("/data/partitions/rba_part")

spark.stop()
