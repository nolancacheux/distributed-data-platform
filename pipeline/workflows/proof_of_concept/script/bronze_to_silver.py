from airflow.decorators import task
from datetime import datetime


@task.virtualenv(
    use_dill=True,
    requirements=["minio", "pandas", "pyarrow"],
    system_site_packages=False,
)
def transform_bronze_to_silver():
    from minio import Minio
    import pandas as pd

    client = Minio(
        "minio-tenant-hl.minio-tenant.svc.cluster.local:9000",
        access_key="minio",
        secret_key="minio123",
        secure=True,
        cert_check=False
    )
    path = "/tmp/users.csv"
    client.fget_object("bronze", "demo/users.csv", path)

    df = pd.read_csv(path)
    df_clean = df[df["email"].notnull()]
    out_path = "/tmp/users_clean.parquet"
    df_clean.to_parquet(out_path, engine="pyarrow")

    if not client.bucket_exists("silver"):
        client.make_bucket("silver")
    client.fput_object("silver", "demo/users_clean.parquet", out_path)

    print("Transformation pandas vers parquet OK")
