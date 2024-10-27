from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Partitioning").getOrCreate()

df = spark.read.csv("/opt/airflow/archive/rba-dataset-sample.csv", header=True, inferSchema=True)

df = df.repartition(10)
df.show()
df.write.option("header", "true").csv("/opt/airflow/partitions/rba_part")


spark.stop()