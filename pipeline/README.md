# Documentation du projet ELT Data Pipeline

## Objectif

La pipeline ELT du projet a pour objectif d'extraire quotidiennement les données des bases opérationnelles PostgreSQL et Cassandra, de les transformer progressivement en données nettoyées et orientées métier, puis de les charger dans un entrepôt DuckDB pour analyse.

Le processus suit une architecture en médaillon :

- **Bronze** : Données brutes extraites.
- **Silver** : Données nettoyées et harmonisées.
- **Gold** : Données agrégées et structurées prêtes pour analyse métier.

---

## Architecture Générale

```
[PostgreSQL / Cassandra]
             |
         [Extract]
             v
     [MinIO / Bucket Bronze]
             |
    [Transform 1 (Nettoyage)]
             v
     [MinIO / Bucket Silver]
             |
 [Transform 2 (Tables métier)]
             v
     [MinIO / Bucket Gold]
             |
           [Load]
             v
     [DuckDB - entrepôt local]
```

---

## Sources des données

### PostgreSQL

- **PG_Users** : utilisateurs
- **PG_Groups** : groupes
- **PG_Activities** : activités
- **PG_NotificationStates** : état des notifications

### Cassandra

- **C_GroupMessages** : messages dans les groupes
- **C_UserNotifications** : notifications envoyées
- **C_UserPurchases** : achats utilisateurs
- **C_UserSessionEvents** : événements de session

---

## Détails des Tables

### PostgreSQL Tables

| Table                   | Colonnes principales                                                       |
|-------------------------|---------------------------------------------------------------------------|
| **PG_Users**            | id, first_name, last_name, email, password, created_at, updated_at        |
| **PG_Groups**           | id, name, description, created_at, updated_at                             |
| **PG_GroupsUsersUser**  | groups_id, users_id                                                       |
| **PG_Activities**       | id, group_id, user_id, activity_type, title, description, location, scheduled_at, status, created_at |
| **PG_NotificationStates** | id, user_id, notification_id, status, updated_at                         |

### Cassandra Tables

| Table                   | Colonnes principales                                                     |
|-------------------------|-------------------------------------------------------------------------|
| **C_GroupMessages**     | group_id, created_at, message_id, user_id, content                      |
| **C_UserNotifications** | user_id, notification_time, notification_id, content, state, modified_at |
| **C_UserPurchases**     | user_id, purchase_time, purchase_id, product_type, amount, currency     |
| **C_UserSessionEvents** | user_id, event_time, session_id, event_type                             |

---

## Pipeline ELT (Airflow)

### Étape 1 : Extraction (Bronze)

- **Objectif** : Extraction quotidienne vers MinIO bucket bronze, format CSV (un dossier par table recommandé pour facilité d'accès ultérieur).
- PostgreSQL via `psycopg2`
- Cassandra via `cassandra-driver`
- Status : Fonctionnel dans Airflow (`@task.virtualenv` pour isolation).

### Étape 2 : Transformation initiale (Silver)

- **Objectif** : Nettoyage, harmonisation, typage automatique.
- Nettoyages :
  - Suppression emails nuls
  - Filtrage des valeurs aberrantes (prix négatifs, valeurs illogiques)
  - Harmonisation des formats dates
- Sortie en Parquet vers bucket silver (conserver séparation dossiers par tables).
- Status : Fonctionnel (`pandas`, `minio`, `pyarrow`).

### Étape 3 : Transformation métier (Gold)
- **Objectif** : Production de tables analytiques métier.

#### Tables finales

1. **global_user_activity_per_day** :
   - user_id (UUID)
   - date (Date)
   - total_nbr_connexion (int)
   - total_spend_time (durée totale des sessions)
   - total_message_send_this_day (int)
   - total_money_spend (float)

   **Sources :**
   - `silver/user_session_events/{{ ds }}/user_session_events.parquet`
   - `silver/group_messages/{{ ds }}/group_messages.parquet`
   - `silver/user_purchases/{{ ds }}/user_purchases.parquet`

2. **global_notif_impact_per_day** :
   - user_id (UUID)
   - content_notif (string)
   - notif_date (Date)
   - is_success (boolean)
   - time_spend_after_success (durée)

   **Sources :**
   - `silver/notification_states/{{ ds }}/notification_states.parquet`
   - `silver/user_notifications/{{ ds }}/user_notifications.parquet`
   - `silver/user_session_events/{{ ds }}/user_session_events.parquet`

- Status : En développement

### Étape 4 : Load vers DuckDB
- **Objectif** : Chargement des fichiers Parquet depuis bucket gold vers DuckDB.
- Tables créées localement à `/tmp/duckdb/analytics.duckdb`
- Accès analystes via SQL (CLI, Python, Notebooks)
- Chargement via `duckdb.connect()` et `read_parquet()`

---

## Programmation & Orchestration
- Orchestration : Apache Airflow (`KubernetesExecutor`)
- Isolation des dépendances par `@task.virtualenv`
- Installation des packages via `extraPipPackages` (values.yaml)
- Planification quotidienne (`@daily`)

---

## Stack Technique
- **Orchestration** : Airflow
- **Stockage objet (Data Lake)** : MinIO
- **Entrepôt SQL local** : DuckDB
- **Transformations** : Pandas / PyArrow
- **Sources opérationnelles** : PostgreSQL / Cassandra

---