FROM bitnami/spark:latest

# Facultatif : install autres paquets utiles si tu veux
# RUN apt-get update && apt-get install -y <outils>

# Définir les variables d'environnement nécessaires
ENV SPARK_HOME=/opt/bitnami/spark
ENV JAVA_HOME=/opt/bitnami/java
ENV PATH="$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH"

# (Optionnel) Installer pyspark si tu veux avoir accès aux modules Python côté Airflow (pas nécessaire pour Spark en lui-même)
RUN pip install pyspark

# Tu peux
