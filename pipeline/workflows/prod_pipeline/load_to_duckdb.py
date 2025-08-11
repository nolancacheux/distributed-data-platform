from airflow.decorators import task


@task.virtualenv(
    use_dill=True,
    requirements=["duckdb", "pandas", "pyarrow", "minio"],
    system_site_packages=False,
)
def load_to_duckdb(**context):
    """
    Charge les deux tables gold du jour (Parquet) dans DuckDB
    et sauvegarde un backup vers MinIO.
    """
    import duckdb
    import pandas as pd
    from minio import Minio
    from io import BytesIO
    from pathlib import Path
    import os

    ds: str = context["ds"]                    # ex. '2025-04-20'

    # ------------------------------------------------------------------ #
    # 1. Lecture des fichiers Parquet depuis MinIO (bucket gold)
    # ------------------------------------------------------------------ #
    client = Minio(
        "minio-tenant-hl.minio-tenant.svc.cluster.local:9000",
        access_key="minio",
        secret_key="minio123",
        secure=True,
        cert_check=False,
    )

    def read_parquet(path: str) -> pd.DataFrame:
        obj = client.get_object("gold", path)
        return pd.read_parquet(BytesIO(obj.read()))

    notif_df = read_parquet(
        f"global_notif_impact_per_day/{ds}/global_notif_impact_per_day.parquet"
    )
    activity_df = read_parquet(
        f"global_user_activity_per_day/{ds}/global_user_activity_per_day.parquet"
    )

    # Harmonise les types (important pour DuckDB) ---------------------- #
    notif_df["user_id"]    = notif_df["user_id"].astype("string")
    activity_df["user_id"] = activity_df["user_id"].astype("string")

    # time_spend_after_success est un timedelta64[ns],
    # DuckDB le convertit nativement en INTERVAL => rien à faire.

    # ------------------------------------------------------------------ #
    # 2. Connexion / initialisation DuckDB
    # ------------------------------------------------------------------ #
    duckdb_path = "/opt/airflow/duckdb/analytics.duckdb"
    Path(duckdb_path).parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(duckdb_path) as conn:
        # -------------------------------------------------------------- #
        # 2‑a. Création explicite des tables cibles (si besoin)
        # -------------------------------------------------------------- #
        conn.execute("DROP TABLE IF EXISTS gold_notif_impact_per_day;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_notif_impact_per_day (
                user_id TEXT,
                content_notif TEXT,
                notif_date DATE,
                is_success BOOLEAN,
                time_spend_after_success INTERVAL
            );
        """)
        conn.execute("DROP TABLE IF EXISTS gold_user_activity_per_day;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_user_activity_per_day (
                user_id TEXT,
                date DATE,
                total_nbr_connexion INTEGER,
                total_spend_time INTERVAL,
                total_message_send_this_day INTEGER,
                total_money_spend DOUBLE
            );
        """)

        # -------------------------------------------------------------- #
        # 2‑b. Enregistre les DataFrame comme vues DuckDB
        # -------------------------------------------------------------- #
        conn.register("notif_df", notif_df)
        conn.register("activity_df", activity_df)

        # -------------------------------------------------------------- #
        # 2‑c. UPSERT partition du jour  (on efface puis on insère)
        # -------------------------------------------------------------- #
        conn.execute(f"""
            DELETE FROM gold_notif_impact_per_day
            WHERE notif_date = DATE '{ds}';
        """)

        conn.execute(f"""
            DELETE FROM gold_user_activity_per_day
            WHERE date = DATE '{ds}';
        """)

        conn.execute("""
            INSERT INTO gold_notif_impact_per_day
            SELECT  user_id,
                    content_notif,
                    notif_date,
                    is_success,
                    time_spend_after_success
            FROM notif_df;
        """)

        conn.execute("""
            INSERT INTO gold_user_activity_per_day
            SELECT  user_id,
                    date,
                    total_nbr_connexion,
                    total_spend_time,
                    total_message_send_this_day,
                    total_money_spend
            FROM activity_df;
        """)

        print(f"[DuckDB] Données {ds} chargées avec succès.")

    # ------------------------------------------------------------------ #
    # 3. Sauvegarde du fichier DuckDB vers MinIO (bucket backup)
    # ------------------------------------------------------------------ #
    backup_obj = f"duckdb_backups/{ds}/analytics.duckdb"
    client.fput_object("backup", backup_obj, duckdb_path)
    print(f"[MinIO] Backup réalisé → backup/{backup_obj}")
