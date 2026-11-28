import argparse
import importlib
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from sqlalchemy import text


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env", encoding="utf-8-sig")

TOPIC = "food-waste-raw"
PARTITIONS = 3
REPLICATION_FACTOR = 1
RETENTION_MS = 7 * 24 * 60 * 60 * 1000

RECORDS_QUERY = text("""
    SELECT
        o.record_id,
        o.year,
        o.loss_percentage,
        c.m49_code AS country_m49_code,
        c.country_name,
        co.cpc_code AS commodity_cpc_code,
        co.commodity_name,
        m.method_id,
        m.method_name,
        s.source_id,
        s.reference,
        s.url,
        COALESCE(
            (
                SELECT array_agg(a.activity_name ORDER BY a.activity_name)
                FROM food_waste.observation_activity AS oa
                JOIN food_waste.activity AS a
                    ON a.activity_id = oa.activity_id
                WHERE oa.record_id = o.record_id
            ),
            ARRAY[]::varchar[]
        ) AS activities
    FROM food_waste.observation AS o
    JOIN food_waste.country AS c
        ON c.m49_code = o.m49_code
    JOIN food_waste.commodity AS co
        ON co.cpc_code = o.cpc_code
    LEFT JOIN food_waste.method AS m
        ON m.method_id = o.method_id
    JOIN food_waste.source AS s
        ON s.source_id = o.source_id
    ORDER BY o.record_id
""")


def default_broker():
    in_docker = os.getenv("RUNNING_IN_DOCKER") == "true"
    return os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092" if in_docker else "localhost:29092",
    )


def get_database_engine():
    connection_module = importlib.import_module("db.config.connection")
    return connection_module.engine


def build_payload(row):
    loss_percentage = row["loss_percentage"]
    return {
        "record_id": row["record_id"],
        "year": row["year"],
        "loss_percentage": (
            float(loss_percentage) if loss_percentage is not None else None
        ),
        "country": {
            "m49_code": row["country_m49_code"],
            "country_name": row["country_name"],
        },
        "commodity": {
            "cpc_code": row["commodity_cpc_code"],
            "commodity_name": row["commodity_name"],
        },
        "method": {
            "method_id": row["method_id"],
            "method_name": row["method_name"],
        },
        "source": {
            "source_id": row["source_id"],
            "reference": row["reference"],
            "url": row["url"],
        },
        "activities": list(row["activities"] or []),
    }


def iter_records(engine, batch_size):
    with engine.connect() as connection:
        result = connection.execution_options(stream_results=True).execute(
            RECORDS_QUERY
        )
        mappings = result.mappings()

        while True:
            rows = mappings.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                yield build_payload(row)


def ensure_topic(servers, topic):
    admin = KafkaAdminClient(
        bootstrap_servers=servers,
        request_timeout_ms=30000,
    )

    try:
        if topic in admin.list_topics():
            return

        try:
            admin.create_topics([
                NewTopic(
                    name=topic,
                    num_partitions=PARTITIONS,
                    replication_factor=REPLICATION_FACTOR,
                    topic_configs={"retention.ms": str(RETENTION_MS)},
                )
            ])
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()


def send_records(producer, engine, topic, batch_size, limit=None):
    sent = 0
    pending = []

    def wait_for_batch(futures):
        producer.flush(timeout=120)
        for future in futures:
            future.get(timeout=120)

    for record in iter_records(engine, batch_size):
        if limit is not None and sent + len(pending) >= limit:
            break

        pending.append(
            producer.send(
                topic,
                key=str(record["record_id"]).encode("utf-8"),
                value=json.dumps(
                    record,
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8"),
            )
        )

        if len(pending) >= batch_size:
            wait_for_batch(pending)
            sent += len(pending)
            pending = []
            print(f"Delivered {sent} records.")

    if pending:
        wait_for_batch(pending)
        sent += len(pending)

    return sent


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Send food-waste observations from PostgreSQL to Kafka"
    )
    parser.add_argument("--topic", default=TOPIC, help="Kafka topic name")
    parser.add_argument(
        "--bootstrap-servers",
        default=default_broker(),
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of database records to send per batch",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of records to send",
    )
    args = parser.parse_args(argv)

    if args.batch_size <= 0:
        parser.error("--batch-size must be greater than 0")
    if args.limit is not None and args.limit < 0:
        parser.error("--limit cannot be negative")

    servers = [
        server.strip()
        for server in args.bootstrap_servers.split(",")
        if server.strip()
    ]
    if not servers:
        parser.error("No Kafka bootstrap servers configured")

    engine = get_database_engine()
    producer = None
    try:
        ensure_topic(servers, args.topic)
        producer = KafkaProducer(
            bootstrap_servers=servers,
            acks="all",
            retries=5,
            max_in_flight_requests_per_connection=1,
            batch_size=65536,
            linger_ms=10,
            compression_type="gzip",
        )
        sent = send_records(
            producer,
            engine,
            args.topic,
            args.batch_size,
            args.limit,
        )
        print(f"Finished: delivered {sent} records to '{args.topic}'.")
        return 0
    finally:
        if producer is not None:
            producer.close(timeout=120)
        engine.dispose()


if __name__ == "__main__":
    sys.exit(main())
