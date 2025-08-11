FROM bitnami/airflow:latest

USER root

RUN pip install --no-cache-dir \
    minio \
    duckdb \
    pandas \
    psycopg2-binary \
    apache-airflow-providers-cncf-kubernetes \
    pyspark \
    cassandra-driver \
    neo4j \
    requests \
    pyarrow \
    faker \
    sqlalchemy

USER 1001
