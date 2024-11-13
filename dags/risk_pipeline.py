import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "risk_pipeline",
    default_args = {
        "owner": "Piotr, Adrian, Filip",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

#TODO: zmienic wordcount.py na cos ze sczytaniem df
health_check = SparkSubmitOperator(
    task_id="health_check",
    conn_id="spark-conn",
    application="jobs/python/wordcount.py",
    dag=dag
)

rba_partitioning = SparkSubmitOperator(
    task_id="rba_partitioning",
    conn_id="spark-conn",
    application="jobs/python/partitioning.py",
    application_args=["10", "/data/rba-dataset-sample.csv", "/data/rba_partitions"],
    dag=dag
)

asn_partitioning = SparkSubmitOperator(
    task_id="asn_partitioning",
    conn_id="spark-conn",
    application="jobs/python/partitioning.py",
    application_args=["10", "/data/asn.csv", "/data/asn_partitions"],
    dag=dag
)

normalize_session = EmptyOperator(
    task_id="normalize_session",
    dag=dag
)

merge_asn = EmptyOperator(
    task_id="merge_asn",
    dag=dag
)

normalize_asn = EmptyOperator(
    task_id="normalize_asn",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> health_check >> [rba_partitioning, asn_partitioning]
rba_partitioning >> [normalize_session, merge_asn, normalize_asn]
asn_partitioning >> [merge_asn, normalize_asn]
[normalize_session, merge_asn, normalize_asn] >> end

