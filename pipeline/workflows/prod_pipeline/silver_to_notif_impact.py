from airflow.decorators import task
from datetime import datetime, timedelta
from uuid import UUID, uuid5


@task.virtualenv(
    use_dill=True,
    requirements=["pandas", "pyarrow", "minio"],
    system_site_packages=False,
)
def transform_silver_to_notif_impact(**context):
    """
    Produit la table gold/global_notif_impact_per_day :
        - user_id                   (UUID)
        - content_notif             (string)
        - notif_date                (date)
        - is_success                (bool)      <- CLICKED (Cassandra)  OU  READ (PostgreSQL)
        - time_spend_after_success  (Timedelta) <- temps passé en session le même jour après la réussite
    """
    import pandas as pd
    from minio import Minio
    from io import BytesIO
    from uuid import UUID, uuid5
    from datetime import timedelta, datetime

    # ------------------------------------------------------------------ #
    # 0. Infos de contexte et helpers MinIO
    # ------------------------------------------------------------------ #
    ds: str = context["ds"]                     # ex. "2025-04-20"
    today = datetime.strptime(ds, "%Y-%m-%d").date()

    client = Minio(
        "minio-tenant-hl.minio-tenant.svc.cluster.local:9000",
        access_key="minio",
        secret_key="minio123",
        secure=True,
        cert_check=False,
    )

    def read_parquet(bucket: str, path: str) -> pd.DataFrame:
        """Lecture directe depuis MinIO vers le DataFrame pandas."""
        obj = client.get_object(bucket, path)
        return pd.read_parquet(BytesIO(obj.read()))

    def write_parquet(df: pd.DataFrame, path: str):
        """Écrit un DataFrame parquet dans le bucket gold."""
        local_path = f"/tmp/{path.split('/')[-1]}"
        df.to_parquet(local_path, index=False)
        client.fput_object("gold", path, local_path)
        print(f"Table écrite : gold/{path}")

    # Namespace utilisé lors du seeding (fourni par ton collègue)
    M1_SEEDING_NAMESPACE = UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")

    def int_to_uuid(prefix: str, pg_id: int) -> UUID:
        """
        Reproduit exactement le même UUID v5 que lors du seeding :
        uuidv5(NAMESPACE, f"{prefix}-{pg_id}")
        """
        return uuid5(M1_SEEDING_NAMESPACE, f"{prefix}-{pg_id}")

    # ------------------------------------------------------------------ #
    # 1. Lecture des trois tables silver du jour (déjà *clean*)
    # ------------------------------------------------------------------ #
    notif_states = read_parquet(
        "silver",
        f"notification_states/{ds}/notification_states_clean.parquet",
    )
    user_notifs = read_parquet(
        "silver",
        f"user_notifications/{ds}/user_notifications_clean.parquet",
    )
    session_events = read_parquet(
        "silver",
        f"user_session_events/{ds}/user_session_events_clean.parquet",
    )

    # ------------------------------------------------------------------ #
    # 2. Normalisation des identifiants Postgres → UUID v5 (Cassandra)
    # ------------------------------------------------------------------ #
    notif_states["user_id"] = notif_states["user_id"].apply(
        lambda x: int_to_uuid("user", int(x))
    )
    notif_states["notification_id"] = notif_states["notification_id"].apply(
        lambda x: int_to_uuid("notification", int(x))
    )

    # ------------------------------------------------------------------ #
    # 3. Préparation des indicateurs de succès
    # ------------------------------------------------------------------ #
    notif_states["is_success_pg"] = (
        notif_states["status"].str.lower() == "read"
    )

    user_notifs["is_success_cas"] = user_notifs["state"] == "CLICKED"

    # On conserve la table Cassandra comme *source principale* pour le contenu
    notifs = user_notifs[
        [
            "user_id",
            "notification_id",
            "content",
            "notification_time",
            "is_success_cas",
        ]
    ].rename(
        columns={
            "content": "content_notif",
            "notification_time": "notif_ts",
        }
    )

    # Join (gauche) pour enrichir avec le flag Postgres
    notifs = notifs.merge(
        notif_states[["user_id", "notification_id", "is_success_pg"]],
        on=["user_id", "notification_id"],
        how="left",
    )

    # Succès = CLICKED  OU  READ
    notifs["is_success"] = (
        notifs["is_success_cas"].fillna(False) | notifs["is_success_pg"].fillna(False)
    )

    # Date (clé d’agrégation demandée)
    notifs["notif_date"] = pd.to_datetime(notifs["notif_ts"]).dt.date

    # ------------------------------------------------------------------ #
    # 4. Calcul du temps passé en session après le succès (même jour)
    # ------------------------------------------------------------------ #
    # 4‑a. Reconstitution des sessions (START + END → durée)
    session_events["event_time"] = pd.to_datetime(session_events["event_time"])

    start_evt = (
        session_events[session_events["event_type"] == "START"]
        .groupby("session_id")
        .agg(session_start=("event_time", "min"), user_id=("user_id", "first"))
    )
    end_evt = (
        session_events[session_events["event_type"] == "END"]
        .groupby("session_id")
        .agg(session_end=("event_time", "max"))
    )

    sessions = start_evt.join(end_evt, how="inner").reset_index()
    sessions["duration"] = sessions["session_end"] - sessions["session_start"]
    sessions["session_date"] = sessions["session_start"].dt.date

    # 4‑b. Jointure par (user_id, date) + filtre sur sessions après la notif
    #     On calcule ensuite la somme de durée pour chaque (user_id, notification)
    sessions_needed_cols = [
        "user_id",
        "session_start",
        "duration",
        "session_date",
    ]
    sessions = sessions[sessions_needed_cols]

    # On fait un merge (explosif) puis filtre -> somme des durées
    notifs = notifs.merge(
        sessions,
        left_on=["user_id", "notif_date"],
        right_on=["user_id", "session_date"],
        how="left",
    )

    # session_start >= notif_ts  =>  valable uniquement pour les succès
    mask_after_notif = notifs["session_start"] >= notifs["notif_ts"]
    notifs.loc[~mask_after_notif, "duration"] = pd.NaT

    # Agrégation finale par notification
    agg = (
        notifs.groupby(
            ["user_id", "content_notif", "notif_date", "is_success"], as_index=False
        )
        .agg(time_spend_after_success=("duration", "sum"))
    )

    # notifications non réussies => durée = 0
    agg["time_spend_after_success"] = agg["time_spend_after_success"].fillna(
        timedelta(seconds=0)
    )

    # ------------------------------------------------------------------ #
    # 5. Écriture dans le bucket gold (le load vers DuckDB se fait après)
    # ------------------------------------------------------------------ #
    out_path = (
        f"global_notif_impact_per_day/{ds}/global_notif_impact_per_day.parquet"
    )
    write_parquet(agg, out_path)
