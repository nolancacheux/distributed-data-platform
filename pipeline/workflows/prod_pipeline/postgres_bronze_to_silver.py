from airflow.decorators import task

@task.virtualenv(
    use_dill=True,
    requirements=["pandas", "minio", "pyarrow"],
    system_site_packages=False,
)
def transform_postgres_bronze_to_silver():
    import pandas as pd
    from pandas import Timestamp
    from minio import Minio
    from datetime import datetime
    import os

    today_utc = Timestamp.now(tz="UTC")

    client = Minio(
        "minio-tenant-hl.minio-tenant.svc.cluster.local:9000",
        access_key="minio",
        secret_key="minio123",
        secure=True,
        cert_check=False
    )

    today = datetime.today().strftime("%Y-%m-%d")

    def save_clean(df, table):
        local_path = f"/tmp/{table}_clean.parquet"
        minio_path = f"{table}/{today}/{table}_clean.parquet"
        df.to_parquet(local_path, index=False)
        client.fput_object("silver", minio_path, local_path)
        print(f"Table {table} cleaned and uploaded to silver/{table}/{today}/")

    def read_bronze(table):
        minio_path = f"{table}/{today}/{table}.csv"

        local_path = f"/tmp/{table}.csv"

        found = client.stat_object("bronze", minio_path)
        if not found:
            raise FileNotFoundError(f"File not found in MinIO: {minio_path}")

        client.fget_object("bronze", minio_path, local_path)
        return pd.read_csv(local_path)

    # USERS
    df_users = read_bronze("users")
    df_users = df_users.dropna(subset=["email"])
    df_users = df_users.drop_duplicates(subset="email")
    df_users["created_at"] = pd.to_datetime(df_users["created_at"], errors="coerce", utc=True)
    df_users = df_users[df_users["created_at"] <= today_utc]
    save_clean(df_users, "users")

    # GROUPS
    df_groups = read_bronze("groups")
    df_groups = df_groups.dropna(subset=["name"])
    df_groups = df_groups.drop_duplicates(subset="name")
    df_groups["created_at"] = pd.to_datetime(df_groups["created_at"], errors="coerce", utc=True)
    df_groups = df_groups[df_groups["created_at"] <= today_utc]
    save_clean(df_groups, "groups")

    # GROUPS_USERS_USER (bridge table)
    df_link = read_bronze("groups_users_user")
    df_link = df_link.dropna(subset=["groups_id", "users_id"])
    df_link = df_link.astype({"groups_id": str, "users_id": str})
    save_clean(df_link, "groups_users_user")

    # ACTIVITIES
    df_act = read_bronze("activities")
    df_act = df_act.dropna(subset=["title", "scheduled_at"])
    df_act = df_act[df_act["scheduled_at"] <= str(datetime.today())]
    # df_act = df_act[df_act["status"].isin(["planned", "ongoing", "done"])]
    # df_act = df_act[df_act["activity_type"].isin(["event", "meeting", "training"])]
    save_clean(df_act, "activities")

    # NOTIFICATION STATES
    df_notif = read_bronze("notification_states")
    df_notif = df_notif.dropna(subset=["user_id", "status"])

    # TODO debug
    print("Notif Shape : ", df_notif.shape, df_notif.head())

    #df_notif = df_notif[df_notif["status"].isin(["read", "unread"])]
    save_clean(df_notif, "notification_states")
