from airflow import DAG
from kubernetes.client import models as k8s
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator, KubernetesPodOperator
#from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

from proof_of_concept.script.extract_load import extract_neo4j_to_minio
from prod_pipeline.extract_daily import extract_postgres_to_bronze_bucket, extract_cassandra_tables_to_bronze_bucket
from prod_pipeline.postgres_bronze_to_silver import transform_postgres_bronze_to_silver
from prod_pipeline.cassandra_bronze_to_silver import transform_cassandra_bronze_to_silver
from prod_pipeline.silver_to_notif_impact import transform_silver_to_notif_impact
from prod_pipeline.silver_to_users_activity import transform_silver_to_user_activity
from prod_pipeline.load_to_duckdb import load_to_duckdb

with DAG("prod_pipeline",
         start_date=datetime(2024, 1, 1),
         schedule_interval="@daily", # chaque jours
         catchup=False) as dag:


    # (1) Extract vers le bucket bronze :
    with TaskGroup("extract") as extract_group:
        extract_daily_from_postgres = extract_postgres_to_bronze_bucket()
        extract_daily_from_cassandra = extract_cassandra_tables_to_bronze_bucket()
        extract_daily_from_neo4j = extract_neo4j_to_minio()



    # (2) Nettoyage/harmonisation vers le bucket silver :
    with TaskGroup("bronze_to_silver") as bronze_to_silver_group:
        transform_task_postgres_bronze_to_silver = transform_postgres_bronze_to_silver()
        transform_task_cassandra_bronze_to_silver = transform_cassandra_bronze_to_silver()


    # (3) Transformation vers le bucket gold :
    with TaskGroup("silver_to_gold") as silver_to_gold_group:
        transform_task_silver_to_notif_impact = transform_silver_to_notif_impact()
        transform_task_silver_to_user_activity = transform_silver_to_user_activity()


    # (4) Chargement des tables vers le data warehouse :
    load_gold_to_duckdb = load_to_duckdb()

    # DAG steps :
    extract_group >> bronze_to_silver_group >> silver_to_gold_group >> load_gold_to_duckdb