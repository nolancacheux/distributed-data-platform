from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
# from cassandra.cluster import Cluster
# from cassandra.auth import PlainTextAuthProvider
from airflow.decorators import task
import os
import datetime


@task.virtualenv(
    use_dill=True,
    requirements=["minio", "psycopg2-binary", "pandas", "duckdb"],
    system_site_packages=False,
)
def extract_postgres_to_minio():
    # import virtuel
    from datetime import datetime
    from minio import Minio
    import duckdb
    import psycopg2
    import pandas as pd
    import os
    from datetime import datetime


    # Ici direct à la DB sans passer par le backend pour le moment
    conn = psycopg2.connect(
        host="postgres-minimal-cluster.postgresql.svc.cluster.local",
        dbname="demo",
        user="stmg",
        password="uzWe8yt6cihF9gbuJi7ot5whT2hsOQfikcCupPLRSA3a70JQQmBacfCspbjKUrwu"
    )
    df = pd.read_sql("SELECT * FROM users", conn)
    path = "/tmp/users.csv"
    df.to_csv(path, index=False)

    # Pareil pour le Tenant de MinIO
    client = Minio(
        "minio-tenant-hl.minio-tenant.svc.cluster.local:9000",
        access_key="minio",
        secret_key="minio123",
        secure=True,
        cert_check=False
    )
    if not client.bucket_exists("bronze"):
        client.make_bucket("bronze")
    
    client.fput_object("bronze", "demo/users.csv", path)


@task.virtualenv(
    use_dill=True,
    requirements=["cassandra-driver", "pandas", "minio"],
    system_site_packages=False,
)
def extract_cassandra_tables_to_minio():
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from minio import Minio
    import pandas as pd

    TABLES = ["group_messages", "user_notifications"]
    auth_provider = PlainTextAuthProvider("demo-superuser", "aNgSePPuVZs63BlFeS02")
    cluster = Cluster(
        ["demo-dc1-service.cassandra.svc.cluster.local"],
        port=9042,
        auth_provider=auth_provider
    )
    session = cluster.connect("cassandra")

    client = Minio(
        "minio-tenant-hl.minio-tenant.svc.cluster.local:9000",
        access_key="minio",
        secret_key="minio123",
        secure=True,
        cert_check=False
    )

    print("Cassandra")

    if not client.bucket_exists("bronze"):
        client.make_bucket("bronze")

    for table in TABLES:
        print(f"[...] Extracting {table} from Cassandra")
        rows = session.execute(f"SELECT * FROM {table}")
        df = pd.DataFrame(rows.all(), columns=rows.column_names)

        local_path = f"/tmp/{table}.csv"
        minio_path = f"demo/{table}.csv"

        df.to_csv(local_path, index=False)
        client.fput_object("bronze", minio_path, local_path)
        print(f"[✓] Uploaded {table}.csv to MinIO bucket 'bronze'")



#extract_neo4j_to_minio()
@task.virtualenv(
    use_dill=True,
    requirements=["pandas", "neo4j","minio"],
    system_site_packages=False,
)
def extract_neo4j_to_minio():
    from minio import Minio
    from neo4j import GraphDatabase
    import pandas as pd

    # Connexion à Neo4j
    uri = "neo4j://my-neo4j-release.neo4j.svc.cluster.local:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "TNtKAyXXPuYQ8e"))

    # Connexion à MinIO
    client = Minio(
        "minio-tenant-hl.minio-tenant.svc.cluster.local:9000",
        access_key="minio",
        secret_key="minio123",
        secure=True,
        cert_check=False
    )

    if not client.bucket_exists("bronze"):
        client.make_bucket("bronze")

    with driver.session() as session:
        result = session.run("MATCH (n) RETURN n")
        data = [record["n"] for record in result]

    df = pd.DataFrame(data)
    local_path = "/tmp/neo4j_data.csv"
    minio_path = "demo/neo4j_data.csv"

    df.to_csv(local_path, index=False)
    client.fput_object("bronze", minio_path, local_path)
    print(f"[✓] Uploaded neo4j_data.csv to MinIO bucket 'bronze'")

@task.virtualenv(
    use_dill=True,
    requirements=["pandas", "duckdb"],
    system_site_packages=False,
)
def load_to_duckdb():
    import pandas as pd
    import duckdb

    df = pd.read_parquet("s3a://silver/demo/users_clean.parquet")
    conn = duckdb.connect("/tmp/duckdb/analytics.duckdb")
    conn.execute("CREATE TABLE IF NOT EXISTS users AS SELECT * FROM df")