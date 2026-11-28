import argparse
import importlib
import json
import os
import sys
from pathlib import Path
from uuid import UUID
import time
from dotenv import load_dotenv
from kafka import KafkaConsumer
from sqlalchemy import text


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env", encoding="utf-8-sig")

SCHEMA = "analytics"
GROUP_ID = "food-waste-layer-consumer"
TOPIC_BY_LAYER = {
    "silver": "food-waste-silver",
    "gold": "food-waste-gold",
    "dashboard": "food-waste-dashboard",
}

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

DATASET_LAYERS = {
    dataset: (
        "dashboard" if dataset.startswith("dashboard_")
        else dataset.split("_", maxsplit=1)[0]
    )
    for dataset in TABLE_COLUMNS
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
BOOLEAN_COLUMNS = {
    "is_modelled",
    "is_reliable_year",
    "is_merged_group",
    "meets_min_size",
}
METADATA_COLUMNS = (
    "dataset_name",
    "layer",
    "run_id",
    "source_topic",
    "source_partition",
    "source_offset",
)


def column_type(name):
    if name in TEXT_COLUMNS:
        return "TEXT"
    if name in INTEGER_COLUMNS:
        return "BIGINT"
    if name in BOOLEAN_COLUMNS:
        return "BOOLEAN"
    return "DOUBLE PRECISION"


def get_database_engine():
    connection_module = importlib.import_module("db.config.connection")
    return connection_module.engine


def ensure_destination_tables(engine):
    preparer = engine.dialect.identifier_preparer
    schema_name = preparer.quote_schema(SCHEMA)
    with engine.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))

        for dataset, columns in TABLE_COLUMNS.items():
            table_name = f"{schema_name}.{preparer.quote(dataset)}"
            definitions = [
                f"{preparer.quote(column)} {column_type(column)}"
                for column in columns
            ]
            definitions.extend([
                '"dataset_name" TEXT NOT NULL',
                '"layer" TEXT NOT NULL',
                '"run_id" UUID NOT NULL',
                '"source_topic" TEXT NOT NULL',
                '"source_partition" INTEGER NOT NULL',
                '"source_offset" BIGINT NOT NULL',
                'UNIQUE ("source_topic", "source_partition", "source_offset")',
            ])
            statement = (
                f"CREATE TABLE IF NOT EXISTS {table_name} "
                f"({', '.join(definitions)})"
            )
            connection.execute(text(statement))


def prepare_message(message):
    envelope = message.value
    if not isinstance(envelope, dict):
        raise ValueError("Kafka message must contain a JSON object")

    metadata = envelope.get("metadata")
    record = envelope.get("record")
    if not isinstance(metadata, dict) or not isinstance(record, dict):
        raise ValueError("Kafka message must contain metadata and record objects")

    dataset = metadata.get("dataset_name")
    layer = metadata.get("layer")
    run_id = metadata.get("run_id")
    if dataset not in TABLE_COLUMNS:
        raise ValueError(f"Unknown Spark dataset: {dataset!r}")
    if layer != DATASET_LAYERS[dataset]:
        raise ValueError(f"Dataset {dataset!r} does not belong to layer {layer!r}")
    if TOPIC_BY_LAYER.get(layer) != message.topic:
        raise ValueError(f"Dataset layer {layer!r} is not valid for topic {message.topic!r}")

    try:
        normalized_run_id = str(UUID(str(run_id)))
    except (ValueError, TypeError, AttributeError) as error:
        raise ValueError("Kafka message metadata.run_id must be a UUID") from error

    unknown_columns = set(record) - set(TABLE_COLUMNS[dataset])
    if unknown_columns:
        raise ValueError(
            f"{dataset}: unexpected record fields {sorted(unknown_columns)}"
        )

    values = {
        f"value_{index}": record.get(column)
        for index, column in enumerate(TABLE_COLUMNS[dataset])
    }
    metadata_offset = len(TABLE_COLUMNS[dataset])
    values.update({
        f"value_{metadata_offset}": dataset,
        f"value_{metadata_offset + 1}": layer,
        f"value_{metadata_offset + 2}": normalized_run_id,
        f"value_{metadata_offset + 3}": message.topic,
        f"value_{metadata_offset + 4}": message.partition,
        f"value_{metadata_offset + 5}": message.offset,
    })
    return dataset, values


def insert_statement(engine, dataset):
    preparer = engine.dialect.identifier_preparer
    table_name = (
        f"{preparer.quote_schema(SCHEMA)}."
        f"{preparer.quote(dataset)}"
    )
    columns = list(TABLE_COLUMNS[dataset]) + list(METADATA_COLUMNS)
    quoted_columns = ", ".join(preparer.quote(column) for column in columns)
    parameters = ", ".join(
        f":value_{index}" for index in range(len(columns))
    )
    return text(
        f"INSERT INTO {table_name} ({quoted_columns}) "
        f"VALUES ({parameters}) "
        "ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING"
    )


def store_batch(engine, consumer, messages):
    grouped = {}
    for message in messages:
        dataset, values = prepare_message(message)
        grouped.setdefault(dataset, []).append(values)

    if not grouped:
        return 0

    with engine.begin() as connection:
        for dataset, rows in grouped.items():
            connection.execute(insert_statement(engine, dataset), rows)

    consumer.commit()
    return sum(len(rows) for rows in grouped.values())



def consume_batches(
    consumer,
    engine,
    batch_size,
    poll_timeout_ms,
    idle_timeout_ms,
):
    total_saved = 0
    idle_since = time.monotonic()

    while True:
        records_by_partition = consumer.poll(
            timeout_ms=poll_timeout_ms,
            max_records=batch_size,
        )

        messages = [
            message
            for partition_records in records_by_partition.values()
            for message in partition_records
        ]

        if not messages:
            elapsed_ms = (
                time.monotonic() - idle_since
            ) * 1000

            if elapsed_ms >= idle_timeout_ms:
                print(
                    f"No new messages for {idle_timeout_ms} ms. "
                    "Consumer finished."
                )
                break

            continue

        idle_since = time.monotonic()

        saved = store_batch(
            engine,
            consumer,
            messages,
        )

        total_saved += saved

        print(
            f"Processed batch of {saved}; "
            f"total processed: {total_saved}."
        )

    return total_saved


def default_broker():
    in_docker = os.getenv("RUNNING_IN_DOCKER") == "true"
    return os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092" if in_docker else "localhost:29092",
    )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Consume Spark layer topics into separate analytics tables"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=default_broker(),
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Maximum number of Kafka records to process per batch",
    )
    parser.add_argument(
        "--poll-timeout-ms",
        type=int,
        default=1000,
        help="Kafka poll timeout in milliseconds",
    )
    
    parser.add_argument(
        "--idle-timeout-ms",
        type=int,
        default=10000,
        help="Stop consuming after this period without new messages",
    )
    args = parser.parse_args(argv)

    if args.batch_size <= 0:
        parser.error("--batch-size must be greater than 0")
    if args.poll_timeout_ms <= 0:
        parser.error("--poll-timeout-ms must be greater than 0")
    if args.idle_timeout_ms <= 0:
        parser.error("--idle-timeout-ms must be greater than 0")
    servers = [
        server.strip()
        for server in args.bootstrap_servers.split(",")
        if server.strip()
    ]
    if not servers:
        parser.error("No Kafka bootstrap servers configured")

    engine = get_database_engine()
    consumer = None
    try:
        ensure_destination_tables(engine)
        consumer = KafkaConsumer(
            *TOPIC_BY_LAYER.values(),
            bootstrap_servers=servers,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
        print(
            "Consuming layer topics into analytics tables: "
            + ", ".join(TOPIC_BY_LAYER.values())
        )
        consume_batches(
            consumer,
            engine,
            args.batch_size,
            args.poll_timeout_ms,
            args.idle_timeout_ms,
        )   
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        try:
            if consumer is not None:
                consumer.close()
        finally:
            engine.dispose()

    return 0


if __name__ == "__main__":
    sys.exit(main())
