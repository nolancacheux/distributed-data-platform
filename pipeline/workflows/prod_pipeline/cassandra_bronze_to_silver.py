from airflow.decorators import task
from datetime import datetime

@task.virtualenv(
    use_dill=True,
    requirements=["pandas", "minio", "pyarrow"],
    system_site_packages=False,
)
def transform_cassandra_bronze_to_silver():
    import pandas as pd
    from minio import Minio
    from datetime import datetime
    import os

    today = datetime.today().strftime("%Y-%m-%d")

    client = Minio(
        "minio-tenant-hl.minio-tenant.svc.cluster.local:9000",
        access_key="minio",
        secret_key="minio123",
        secure=True,
        cert_check=False
    )

    def save_parquet(df, table_name):
        local_path = f"/tmp/{table_name}_clean.parquet"
        minio_path = f"{table_name}/{today}/{table_name}_clean.parquet"
        df.to_parquet(local_path, index=False)
        client.fput_object("silver", minio_path, local_path)
        print(f"Table {table_name} cleaned and saved to silver/{table_name}/{today}/")

    #### TABLE 1: group_messages ####
    if client.bucket_exists("bronze"):
        obj = client.get_object("bronze", f"group_messages/{today}/group_messages.csv")
        df = pd.read_csv(obj)

        df.dropna(subset=["group_id", "user_id", "message_id", "content"], inplace=True)
        df = df[df["content"].str.strip() != ""]

        save_parquet(df, "group_messages")

    #### TABLE 2: user_notifications ####
    if client.bucket_exists("bronze"):
        obj = client.get_object("bronze", f"user_notifications/{today}/user_notifications.csv")
        df = pd.read_csv(obj)

        # TODO debug
        print(df.shape, df.head(20))

        df.dropna(subset=["user_id", "notification_id", "content", "state"], inplace=True)
        df = df[df["state"].isin(["SEEN", "DELETED", "CLICKED"])]

        save_parquet(df, "user_notifications")

    #### TABLE 3: user_purchases ####
    if client.bucket_exists("bronze"):
        obj = client.get_object("bronze", f"user_purchases/{today}/user_purchases.csv")
        df = pd.read_csv(obj)

        df.dropna(subset=["user_id", "purchase_id", "product_type", "amount"], inplace=True)
        #df = df[df["amount"] >= 0]
        #df = df[df["currency"].isin(["EUR", "USD", "GBP"])]  # valeurs réalistes

        save_parquet(df, "user_purchases")

    #### TABLE 4: user_session_events ####
    if client.bucket_exists("bronze"):
        obj = client.get_object("bronze", f"user_session_events/{today}/user_session_events.csv")
        df = pd.read_csv(obj)
        
        # TODO debug
        print(df.shape, df.head())

        df.dropna(subset=["user_id", "session_id", "event_type"], inplace=True)
        df = df[df["event_type"].isin(["START", "END"])]

        save_parquet(df, "user_session_events")