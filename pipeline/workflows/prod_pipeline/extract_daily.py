from airflow.decorators import task
from datetime import datetime


@task.virtualenv(
    use_dill=True,
    requirements=["pandas", "minio", "psycopg2-binary"],
    system_site_packages=False,
)
def extract_postgres_to_bronze_bucket():
    import pandas as pd
    import psycopg2
    from minio import Minio
    from datetime import datetime

    conn = psycopg2.connect(
        host="postgres-minimal-cluster.postgresql.svc.cluster.local",
        dbname="demo",
        user="stmg",
        password="uzWe8yt6cihF9gbuJi7ot5whT2hsOQfikcCupPLRSA3a70JQQmBacfCspbjKUrwu"
    )

    client = Minio(
        "minio-tenant-hl.minio-tenant.svc.cluster.local:9000",
        access_key="minio",
        secret_key="minio123",
        secure=True,
        cert_check=False
    )

    if not client.bucket_exists("bronze"):
        client.make_bucket("bronze")

    TABLES_WITH_DATE = {
        "users": "created_at",
        "groups": "created_at",
        "activities": "created_at",
        "notification_states": "updated_at"
    }
    TABLES_NO_DATE = ["groups_users_user"]
    today = datetime.today().strftime("%Y-%m-%d")

    # Tables avec filtre par date
    for table, date_col in TABLES_WITH_DATE.items():
        print(f"[INFO] Extracting {table} with filter on {date_col}")
        query = f"SELECT * FROM {table} WHERE DATE({date_col}) = '{today}'"
        df = pd.read_sql(query, conn)

        if not df.empty:
            local_path = f"/tmp/{table}.csv"
            minio_path = f"{table}/{today}/{table}.csv"
            df.to_csv(local_path, index=False)
            client.fput_object("bronze", minio_path, local_path)
            print(f"[UPLOAD] {table}.csv uploaded to bronze/{table}/{today}/")
        else:
            print(f"[SKIP] {table}: no data for {today}")

    # Tables sans filtre
    for table in TABLES_NO_DATE:
        print(f"[INFO] Extracting {table} (full table)")
        df = pd.read_sql(f"SELECT * FROM {table}", conn)

        if not df.empty:
            local_path = f"/tmp/{table}.csv"
            minio_path = f"{table}/{today}/{table}.csv"
            df.to_csv(local_path, index=False)
            client.fput_object("bronze", minio_path, local_path)
            print(f"[UPLOAD] {table}.csv uploaded to bronze/{table}/{today}/")
        else:
            print(f"[SKIP] {table}: empty")


@task.virtualenv(
    use_dill=True,
    requirements=["pandas", "minio", "cassandra-driver"],
    system_site_packages=False,
)
def extract_cassandra_tables_to_bronze_bucket():
    import pandas as pd
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from minio import Minio
    from datetime import datetime

    auth_provider = PlainTextAuthProvider("demo-superuser", "aNgSePPuVZs63BlFeS02")
    cluster = Cluster(["demo-dc1-service.cassandra.svc.cluster.local"], port=9042, auth_provider=auth_provider)
    session = cluster.connect("cassandra")

    client = Minio(
        "minio-tenant-hl.minio-tenant.svc.cluster.local:9000",
        access_key="minio",
        secret_key="minio123",
        secure=True,
        cert_check=False
    )

    if not client.bucket_exists("bronze"):
        client.make_bucket("bronze")

    TABLES = ["group_messages", "user_notifications", "user_purchases", "user_session_events"]
    today = datetime.today().strftime("%Y-%m-%d")

    for table in TABLES:
        print(f"[INFO] Extracting {table} from Cassandra...")
        rows = session.execute(f"SELECT * FROM {table}")
        df = pd.DataFrame(rows.all(), columns=rows.column_names)

        local_path = f"/tmp/{table}.csv"
        minio_path = f"{table}/{today}/{table}.csv"

        df.to_csv(local_path, index=False)
        client.fput_object("bronze", minio_path, local_path)

        print(f"Table {table}.csv uploaded dans le bucket : bronze/{table}/{today}/")