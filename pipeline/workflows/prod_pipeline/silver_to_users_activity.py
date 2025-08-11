from airflow.decorators import task
from datetime import datetime


@task.virtualenv(
    use_dill=True,
    requirements=["pandas", "pyarrow", "minio"],
    system_site_packages=False,
)
def transform_silver_to_user_activity(**context):
    import pandas as pd
    from minio import Minio
    from io import BytesIO
    from datetime import timedelta, datetime
    import os

    ds = context["ds"]
    today = datetime.strptime(ds, "%Y-%m-%d").date()

    client = Minio(
        "minio-tenant-hl.minio-tenant.svc.cluster.local:9000",
        access_key="minio",
        secret_key="minio123",
        secure=True,
        cert_check=False
    )

    def read_parquet(bucket, path):
        response = client.get_object(bucket, path)
        return pd.read_parquet(BytesIO(response.read()))

    # Récupération des datasets
    sessions = read_parquet("silver", f"user_session_events/{ds}/user_session_events_clean.parquet")
    messages = read_parquet("silver", f"group_messages/{ds}/group_messages_clean.parquet")
    purchases = read_parquet("silver", f"user_purchases/{ds}/user_purchases_clean.parquet")

    # Connexions et temps passé
    sessions["event_time"] = pd.to_datetime(sessions["event_time"])
    sessions["date"] = sessions["event_time"].dt.date

    sessions_grouped = sessions.groupby(["user_id", "date"]).agg(
        total_nbr_connexion=("session_id", "nunique"),
        min_time=("event_time", "min"),
        max_time=("event_time", "max")
    ).reset_index()

    sessions_grouped["total_spend_time"] = sessions_grouped["max_time"] - sessions_grouped["min_time"]
    sessions_grouped.drop(columns=["min_time", "max_time"], inplace=True)

    # Messages envoyés
    messages["created_at"] = pd.to_datetime(messages["created_at"])
    messages["date"] = messages["created_at"].dt.date

    messages_grouped = messages.groupby(["user_id", "date"]).agg(
        total_message_send_this_day=("message_id", "count")
    ).reset_index()

    # Achats
    purchases["purchase_time"] = pd.to_datetime(purchases["purchase_time"])
    purchases["date"] = purchases["purchase_time"].dt.date

    purchases_grouped = purchases.groupby(["user_id", "date"]).agg(
        total_money_spend=("amount", "sum")
    ).reset_index()

    # Fusion des 3 sources
    merged = pd.merge(sessions_grouped, messages_grouped, on=["user_id", "date"], how="outer")
    merged = pd.merge(merged, purchases_grouped, on=["user_id", "date"], how="outer")

    merged.fillna({
        "total_nbr_connexion": 0,
        "total_spend_time": timedelta(seconds=0),
        "total_message_send_this_day": 0,
        "total_money_spend": 0.0
    }, inplace=True)

    final_df = merged[[
        "user_id", "date", "total_nbr_connexion", "total_spend_time",
        "total_message_send_this_day", "total_money_spend"
    ]]

    # Écriture
    out_path = f"/tmp/global_user_activity_per_day_{ds}.parquet"
    final_df.to_parquet(out_path, index=False)

    if not client.bucket_exists("gold"):
        client.make_bucket("gold")

    minio_path = f"global_user_activity_per_day/{ds}/global_user_activity_per_day.parquet"
    client.fput_object("gold", minio_path, out_path)

    print(f"Table global_user_activity_per_day créée dans gold/{minio_path}")
