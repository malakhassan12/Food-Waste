"""Consume Spark JSON rows from Kafka into PostgreSQL database `analatics`.

Place this file in the Food-Waste project root.
Install: pip install kafka-python psycopg2-binary python-dotenv
Run:     python kafka_consumer.py

Each topic has the same name as its destination table, e.g. gold_kpis or
silver_observation. Each message is ONE flat JSON object with that table's
columns; omitted fields are stored as NULL. Spark must publish these messages.
The 32 table definitions match data/processed_by_spark/{dashboard,gold,silver}.

Uses DATABASE_URL from the environment or the existing .env for connection
settings, but always writes to database `analatics`. Defaults match Docker
Compose: airflow/airflow on localhost:5432 and Kafka on localhost:29092.
Inside an Airflow container, RUNNING_IN_DOCKER=true selects postgres and kafka.
KAFKA_BOOTSTRAP_SERVERS can override the Kafka address.

Rows are appended. A crash between the database commit and Kafka commit can
cause a row to be delivered again.
"""

import json
import os
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import parse_dsn
from dotenv import load_dotenv
from kafka import KafkaConsumer


DATABASE_NAME = "analatics"

# One table for each dataset in the three Spark output folders.
TABLE_COLUMNS = {
    "dashboard_food_waste_dashboard": "country,continent,m49_code,commodity,product_group,cpc_code,year,decade,time_period,is_reliable_year,activity,stage_group,method_data_collection,method_group,is_modelled,loss_percentage,median_loss,min_loss,max_loss,loss_sum,count,loss_band,is_merged_group".split(","),
    "gold_activity_count_profile": "activity_count,observations,percentage".split(","),
    "gold_continent": "continent,crops,records,countries,avg_loss_pct,median_loss_pct,p90_loss_pct".split(","),
    "gold_continent_year": "continent,year,records,countries,avg_loss_pct,median_loss_pct,p90_loss_pct".split(","),
    "gold_country": "country,m49_code,continent,records,crops,avg_loss_pct,median_loss_pct,meets_min_size,country_rank".split(","),
    "gold_country_crop_hotspots": "country,continent,commodity,records,median_loss_pct".split(","),
    "gold_crop_stage_hotspots": "commodity,product_group,stage_group,records,avg_loss,median_loss,p90_loss,countries".split(","),
    "gold_crop_variability": "commodity,product_group,records,avg_loss,stddev_loss,min_loss,max_loss,median_loss,iqr_loss,countries,loss_range".split(","),
    "gold_crops": "commodity,product_group,records,countries,avg_loss_pct,median_loss_pct,p90_loss_pct,first_year,last_year,meets_min_size,crop_rank".split(","),
    "gold_dashboard_grouped": "country,continent,m49_code,commodity,product_group,cpc_code,year,decade,time_period,is_reliable_year,activity,stage_group,method_data_collection,method_group,is_modelled,loss_percentage,median_loss,min_loss,max_loss,loss_sum,count,loss_band,is_merged_group".split(","),
    "gold_grouped": "m49_code,country,cpc_code,commodity,year,activity,method_data_collection,continent,product_group,method_group,time_period,loss_percentage,median_loss,min_loss,max_loss,loss_sum,count".split(","),
    "gold_kpis": "total_records,countries,commodities,sources,first_year,last_year,avg_loss_pct,median_loss_pct,p90_loss_pct,max_loss_pct,pct_modelled_records".split(","),
    "gold_like_for_like_continents": "continent,paired,avg_2000_2010,avg_2011_2021,change_pp,change_pct".split(","),
    "gold_like_for_like_countries": "country,paired,avg_2000_2010,avg_2011_2021,change_pp,change_pct".split(","),
    "gold_like_for_like_crops": "commodity,paired,avg_2000_2010,avg_2011_2021,change_pp,change_pct".split(","),
    "gold_loss_distribution": "loss_band,records,share_pct".split(","),
    "gold_method": "method_data_collection,method_group,records,avg_loss_pct,median_loss_pct,share_pct".split(","),
    "gold_method_year": "year,records,pct_modelled,pct_measured,pct_expert_or_unspecified".split(","),
    "gold_modelled_vs_measured_gap": "commodity,product_group,Measured_records,Measured_avg_loss,Measured_median_loss,Measured_countries,Modelled_records,Modelled_avg_loss,Modelled_median_loss,Modelled_countries,avg_loss_gap,median_loss_gap".split(","),
    "gold_product_group": "product_group,crops,records,countries,avg_loss_pct,median_loss_pct,p90_loss_pct".split(","),
    "gold_stage": "stage_group,crops,records,countries,avg_loss_pct,median_loss_pct,p90_loss_pct".split(","),
    "gold_stage_continent": "continent,stage_group,records,median_loss_pct".split(","),
    "gold_stage_product_group": "product_group,stage_group,records,median_loss_pct".split(","),
    "gold_stage_single_activity": "stage_group,activity_count,activities,records".split(","),
    "gold_top_activities": "activity,stage_group,records,countries,median_loss_pct".split(","),
    "gold_top_crops_measured": "commodity,product_group,records,avg_loss,median_loss,p90_loss,countries,first_year,last_year".split(","),
    "gold_top_crops_per_continent": "continent,commodity,product_group,records,avg_loss,median_loss,countries,crop_rank".split(","),
    "gold_top_crops_per_country": "country,m49_code,continent,commodity,product_group,records,avg_loss,median_loss,p90_loss,first_year,last_year,crop_rank".split(","),
    "gold_yearly": "year,records,countries,commodities,avg_loss_pct,p25_loss_pct,median_loss_pct,p75_loss_pct,is_reliable_year".split(","),
    "gold_yearly_trend": "year,records,countries,commodities,avg_loss_pct,p25_loss_pct,median_loss_pct,p75_loss_pct,is_reliable_year,change_vs_previous_reliable_year".split(","),
    "silver_observation": "record_id,m49_code,country,continent,cpc_code,commodity,product_group,year,time_period,loss_percentage,loss_band,activity,activity_count,method_data_collection,method_group,is_modelled,reference".split(","),
    "silver_observation_activity": "record_id,activity,stage_group,country,continent,commodity,product_group,year,loss_percentage,method_group,activity_count".split(","),
}

TEXT_COLUMNS = set("""
    m49_code country continent cpc_code commodity product_group time_period
    loss_band activity activities stage_group method_data_collection method_group
    reference decade
""".split())
INTEGER_COLUMNS = set("""
    record_id year first_year last_year activity_count observations total_records
    records countries crops commodities sources paired count crop_rank country_rank
    Measured_records Measured_countries Modelled_records Modelled_countries
""".split())
BOOLEAN_COLUMNS = {"is_modelled", "is_reliable_year", "is_merged_group", "meets_min_size"}


def column_type(name):
    if name in TEXT_COLUMNS:
        return "TEXT"
    if name in INTEGER_COLUMNS:
        return "BIGINT"
    if name in BOOLEAN_COLUMNS:
        return "BOOLEAN"
    return "DOUBLE PRECISION"


def connect_database():
    """Create analatics if needed, using the project's PostgreSQL server."""
    in_docker = os.getenv("RUNNING_IN_DOCKER") == "true"
    host = "postgres" if in_docker else "localhost"
    url = os.getenv("DATABASE_URL", f"postgresql://airflow:airflow@{host}:5432/postgres")
    settings = parse_dsn(url.replace("postgresql+psycopg2://", "postgresql://", 1))
    if not in_docker and settings.get("host") == "postgres":
        settings["host"] = "localhost"
    settings["connect_timeout"] = 10
    settings["dbname"] = "postgres"

    admin = psycopg2.connect(**settings)
    try:
        admin.autocommit = True  # CREATE DATABASE must run outside a transaction.
        with admin.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DATABASE_NAME,))
            if cursor.fetchone() is None:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DATABASE_NAME)))
    finally:
        admin.close()

    settings["dbname"] = DATABASE_NAME
    return psycopg2.connect(**settings)


def create_tables(connection):
    with connection:
        with connection.cursor() as cursor:
            for table, columns in TABLE_COLUMNS.items():
                definitions = sql.SQL(", ").join(
                    sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(column_type(name)))
                    for name in columns
                )
                cursor.execute(
                    sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                        sql.Identifier("public", table), definitions
                    )
                )


def store_row(connection, table, row):
    columns = TABLE_COLUMNS[table]
    if not isinstance(row, dict) or not row:
        raise ValueError(f"{table}: expected a JSON object containing a row")
    unknown = set(row) - set(columns)
    if unknown:
        raise ValueError(f"{table}: unexpected columns {sorted(unknown)}")

    statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier("public", table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(statement, [row.get(name) for name in columns])


def main():
    load_dotenv(Path(__file__).resolve().parent / ".env", encoding="utf-8-sig")
    in_docker = os.getenv("RUNNING_IN_DOCKER") == "true"
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092" if in_docker else "localhost:29092")
    connection = connect_database()
    consumer = None
    try:
        create_tables(connection)
        consumer = KafkaConsumer(
            *TABLE_COLUMNS,
            bootstrap_servers=broker.split(","),
            group_id="spark-to-analatics",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
        print(f"Listening on {broker}; destination: {DATABASE_NAME}, {len(TABLE_COLUMNS)} tables.")
        for message in consumer:
            store_row(connection, message.topic, message.value)
            consumer.commit()  # Acknowledge only after the database transaction succeeds.
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        try:
            if consumer is not None:
                consumer.close()
        finally:
            connection.close()


if __name__ == "__main__":
    main()
