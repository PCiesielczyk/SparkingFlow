import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "risk_pipeline",
    default_args = {
        "owner": "Piotr, Adrian, Filip",
        "start_date": airflow.utils.dates.days_ago(1)
    },
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

health_check = SparkSubmitOperator(
    task_id="health_check",
    conn_id="spark-conn",
    application="jobs/python/health_check.py",
    application_args=["/data/rba-dataset-sample.csv"],
    dag=dag
)

rba_partitioning = SparkSubmitOperator(
    task_id="rba_partitioning",
    conn_id="spark-conn",
    application="jobs/python/partitioning.py",
    application_args=["10", "/data/rba-dataset-sample.csv", "/data/rba_partitions"],
    dag=dag
)

ml_preprocessing = SparkSubmitOperator(
    task_id="ml_preprocessing",
    conn_id="spark-conn",
    application="jobs/python/ml_preprocessing.py",
    application_args=["/data/rba_partitions", "/data/classification"],
    dag=dag
)

model_training = SparkSubmitOperator(
    task_id="model_training",
    conn_id="spark-conn",
    application="jobs/python/model_training.py",
    application_args=["/data/classification"],
    dag=dag
)

asn_partitioning = SparkSubmitOperator(
    task_id="asn_partitioning",
    conn_id="spark-conn",
    application="jobs/python/partitioning.py",
    application_args=["10", "/data/asn.csv", "/data/asn_partitions"],
    dag=dag
)

normalize_session = SparkSubmitOperator(
    task_id="normalize_session",
    conn_id="spark-conn",
    application="jobs/python/normalize_session.py",
    application_args=["/data/rba_partitions", "/data/session"],
    dag=dag
)

normalize_source = SparkSubmitOperator(
    task_id="normalize_source",
    conn_id="spark-conn",
    application="jobs/python/normalize_source.py",
    application_args=["/data/rba_partitions", "/data/source"],
    dag=dag
)

normalize_asn = SparkSubmitOperator(
    task_id="normalize_asn",
    conn_id="spark-conn",
    application="jobs/python/normalize_asn.py",
    application_args=["/data/asn_partitions", "/data/asn_details"],
    dag=dag
)

process_session = SparkSubmitOperator(
    task_id="process_session",
    conn_id="spark-conn",
    application="jobs/python/process_session.py",
    application_args=["/data/session", "/data/session_output"],
    dag=dag
)

process_source_asn = SparkSubmitOperator(
    task_id="process_source_asn",
    conn_id="spark-conn",
    application="jobs/python/process_source_asn.py",
    application_args=["/data/source", "/data/asn_details", "/data/source_asn_output"],
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> health_check >> [rba_partitioning, asn_partitioning]
rba_partitioning >> [normalize_session, normalize_source, ml_preprocessing]
ml_preprocessing >> model_training
asn_partitioning >> [normalize_asn]
normalize_session >> process_session
[normalize_source, normalize_asn] >> process_source_asn
[model_training, process_session, process_source_asn] >> end

