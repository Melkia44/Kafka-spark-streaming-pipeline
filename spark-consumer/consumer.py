"""
Consommateur PySpark pour les tickets clients InduTechData.

Ce script lit les tickets depuis le topic Redpanda 'client_tickets',
applique des transformations et agrégations, puis exporte les résultats
en format Parquet pour une analyse ultérieure.

Pipeline :
    1. Lecture streaming depuis Redpanda (protocole Kafka)
    2. Parsing JSON et validation du schéma
    3. Enrichissement : assignation d'équipe de support
    4. Agrégations : comptages par type, priorité, volume horaire
    5. Export en Parquet (micro-batch)
"""

import os
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, when, lit,
    current_timestamp, hour, date_format, to_timestamp,
    expr, avg, max as spark_max, min as spark_min
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

# ── Configuration ──────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "client_tickets")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/app/output")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/app/checkpoints")

# ── Schéma des tickets (conforme à l'énoncé) ──────────────────
TICKET_SCHEMA = StructType([
    StructField("ticket_id", StringType(), False),
    StructField("client_id", StringType(), False),
    StructField("created_at", StringType(), False),
    StructField("request", StringType(), False),
    StructField("request_type", StringType(), False),
    StructField("priority", StringType(), False),
])

# ── Mapping métier : type de demande → équipe de support ──────
# (Transformation demandée explicitement dans l'énoncé)
TEAM_ASSIGNMENT = {
    "incident_technique": "Equipe Infrastructure",
    "demande_information": "Support Client N1",
    "demande_evolution": "Equipe Produit",
    "maintenance": "Equipe Operations",
    "facturation": "Service Comptabilite",
}


def assign_support_team(request_type_col):
    """Assigne une équipe de support en fonction du type de demande."""
    result = lit("Support General")
    for req_type, team in TEAM_ASSIGNMENT.items():
        result = when(request_type_col == req_type, lit(team)).otherwise(result)
    return result


def create_spark_session() -> SparkSession:
    """Crée et configure la session Spark."""
    return (
        SparkSession.builder
        .appName("InduTechData-TicketConsumer")
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def read_from_redpanda(spark: SparkSession):
    """Lit le flux de tickets depuis Redpanda via le protocole Kafka."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_enrich(raw_stream):
    """Parse le JSON et enrichit les données avec l'équipe de support."""
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")
        .select(
            from_json(col("json_str"), TICKET_SCHEMA).alias("ticket"),
            col("kafka_timestamp")
        )
        .select(
            col("ticket.ticket_id"),
            col("ticket.client_id"),
            to_timestamp(col("ticket.created_at")).alias("created_at"),
            col("ticket.request"),
            col("ticket.request_type"),
            col("ticket.priority"),
            col("kafka_timestamp"),
        )
    )

    # Enrichissement : ajout de l'équipe de support assignée
    enriched = parsed.withColumn(
        "assigned_team",
        assign_support_team(col("request_type"))
    ).withColumn(
        "processing_timestamp",
        current_timestamp()
    )

    return enriched


def write_enriched_tickets(enriched_stream):
    """Écrit les tickets enrichis en Parquet (mode append)."""
    query = (
        enriched_stream
        .writeStream
        .queryName("enriched_tickets")
        .outputMode("append")
        .format("parquet")
        .option("path", f"{OUTPUT_PATH}/enriched_tickets")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/enriched_tickets")
        .trigger(processingTime="30 seconds")
        .start()
    )
    return query


def write_tickets_by_type(enriched_stream):
    """Agrégation : nombre de tickets par type de demande."""
    agg = (
        enriched_stream
        .withWatermark("created_at", "1 minute")
        .groupBy(
            window(col("created_at"), "5 minutes"),
            col("request_type"),
            col("assigned_team")
        )
        .agg(
            count("*").alias("ticket_count"),
        )
    )

    query = (
        agg.writeStream
        .queryName("tickets_by_type")
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="30 seconds")
        .start()
    )
    return query


def write_tickets_by_priority(enriched_stream):
    """Agrégation : nombre de tickets par priorité."""
    agg = (
        enriched_stream
        .withWatermark("created_at", "1 minute")
        .groupBy(
            window(col("created_at"), "5 minutes"),
            col("priority")
        )
        .agg(
            count("*").alias("ticket_count"),
        )
    )

    query = (
        agg.writeStream
        .queryName("tickets_by_priority")
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="30 seconds")
        .start()
    )
    return query


def write_batch_aggregations(enriched_stream):
    """
    Exporte périodiquement des agrégations complètes en Parquet.
    Utilise foreachBatch pour produire des fichiers exploitables.
    """

    def process_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            return

        print(f"\n{'='*60}")
        print(f"  Traitement du batch #{batch_id}")
        print(f"  Nombre de tickets: {batch_df.count()}")
        print(f"{'='*60}")

        # ── Agrégation par type de demande ─────────────────────
        by_type = (
            batch_df
            .groupBy("request_type", "assigned_team")
            .agg(count("*").alias("ticket_count"))
            .orderBy(col("ticket_count").desc())
        )
        by_type.show(truncate=False)
        by_type.coalesce(1).write.mode("overwrite").parquet(
            f"{OUTPUT_PATH}/agg_by_type"
        )

        # ── Agrégation par priorité ───────────────────────────
        by_priority = (
            batch_df
            .groupBy("priority")
            .agg(count("*").alias("ticket_count"))
            .orderBy(col("ticket_count").desc())
        )
        by_priority.show(truncate=False)
        by_priority.coalesce(1).write.mode("overwrite").parquet(
            f"{OUTPUT_PATH}/agg_by_priority"
        )

        # ── Volume horaire ────────────────────────────────────
        hourly = (
            batch_df
            .withColumn("hour", hour(col("created_at")))
            .groupBy("hour")
            .agg(count("*").alias("ticket_count"))
            .orderBy("hour")
        )
        hourly.show(truncate=False)
        hourly.coalesce(1).write.mode("overwrite").parquet(
            f"{OUTPUT_PATH}/agg_hourly_volume"
        )

        # ── Top clients par nombre de tickets ─────────────────
        top_clients = (
            batch_df
            .groupBy("client_id")
            .agg(count("*").alias("ticket_count"))
            .orderBy(col("ticket_count").desc())
            .limit(10)
        )
        top_clients.show(truncate=False)
        top_clients.coalesce(1).write.mode("overwrite").parquet(
            f"{OUTPUT_PATH}/agg_top_clients"
        )

        # ── Export JSON pour visualisation ────────────────────
        by_type.coalesce(1).write.mode("overwrite").json(
            f"{OUTPUT_PATH}/json_by_type"
        )
        by_priority.coalesce(1).write.mode("overwrite").json(
            f"{OUTPUT_PATH}/json_by_priority"
        )

        print(f"\n✅ Batch #{batch_id} traité et exporté avec succès.\n")

    query = (
        enriched_stream
        .writeStream
        .queryName("batch_aggregations")
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/batch_agg")
        .trigger(processingTime="60 seconds")
        .start()
    )
    return query


def main():
    """Point d'entrée principal du consommateur PySpark."""
    print("=" * 60)
    print("  InduTechData - Consommateur PySpark")
    print(f"  Broker: {KAFKA_BROKER}")
    print(f"  Topic:  {TOPIC_NAME}")
    print(f"  Output: {OUTPUT_PATH}")
    print("=" * 60)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Lecture depuis Redpanda
    raw_stream = read_from_redpanda(spark)

    # 2. Parsing et enrichissement
    enriched_stream = parse_and_enrich(raw_stream)

    # 3. Lancement des queries de sortie
    q1 = write_enriched_tickets(enriched_stream)
    q2 = write_tickets_by_type(enriched_stream)
    q3 = write_tickets_by_priority(enriched_stream)
    q4 = write_batch_aggregations(enriched_stream)

    print("\n🚀 4 queries streaming lancées. En attente de données...\n")

    # Attend la terminaison de toutes les queries
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
