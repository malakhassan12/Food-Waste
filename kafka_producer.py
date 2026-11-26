import argparse
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

try:  # optional: pick up KAFKA_BOOTSTRAP_SERVERS from the project .env, like kafka_consumer.py
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env", encoding="utf-8-sig")
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CSV = BASE_DIR / "data" / "World-food-waste_cleaned_final.csv"
TOPIC = "food-waste-cleaned"
PARTITIONS = 3
REPLICATION_FACTOR = 1                    # single broker -> must be 1
RETENTION_MS = 7 * 24 * 60 * 60 * 1000    # 7 days


def default_broker():
    in_docker = os.getenv("RUNNING_IN_DOCKER") == "true"
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092" if in_docker else "localhost:29092")


def load_records(csv_path):
    """Read the cleaned CSV into a list of JSON-safe dicts (one per row).

    cpc_code stays text (e.g. "23161.02"). Missing values become None -> JSON null,
    because plain row.to_dict() gives NaN, which is not valid JSON and breaks Spark.
    """
    df = pd.read_csv(csv_path, dtype={"cpc_code": str})
    df = df.astype(object).where(df.notna(), None)
    return df.to_dict(orient="records")


def to_json_bytes(record):
    return json.dumps(record, ensure_ascii=False, allow_nan=False).encode("utf-8")


def ensure_topic(servers, topic):
    admin = KafkaAdminClient(bootstrap_servers=servers)  # raises a KafkaError if the broker is unreachable
    try:
        if topic in admin.list_topics():
            print(f"Topic '{topic}' already exists")
            return
        try:
            admin.create_topics([NewTopic(topic, num_partitions=PARTITIONS,
                                          replication_factor=REPLICATION_FACTOR,
                                          topic_configs={"retention.ms": str(RETENTION_MS)})])
            print(f"Created topic '{topic}' (partitions={PARTITIONS}, replication={REPLICATION_FACTOR}, retention=7 days)")
        except TopicAlreadyExistsError:
            print(f"Topic '{topic}' already exists")
    finally:
        admin.close()


def main(argv=None):
    ap = argparse.ArgumentParser(description="Send the cleaned food-waste CSV to Kafka")
    ap.add_argument("--csv", default=str(DEFAULT_CSV))
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--bootstrap-servers", default=default_broker())
    ap.add_argument("--limit", type=int, default=None, help="send only the first N rows")
    ap.add_argument("--delay", type=float, default=0.0, help="seconds between messages (simulate a stream)")
    args = ap.parse_args(argv)

    if not Path(args.csv).is_file():
        print(f"ERROR: CSV not found: {args.csv}")
        return 1
    records = load_records(args.csv)
    if args.limit is not None:
        records = records[: args.limit]
    total = len(records)
    servers = args.bootstrap_servers.split(",")
    print(f"Loaded {total} records from {args.csv}")
    print(f"Kafka broker: {args.bootstrap_servers}")

    try:
        ensure_topic(servers, args.topic)
        producer = KafkaProducer(
            bootstrap_servers=servers,
            acks="all",                                   # broker confirms every message
            retries=5,
            max_in_flight_requests_per_connection=1,      # keeps order when retrying
            compression_type="gzip",
            linger_ms=20,
        )
    except KafkaError as e:
        print(f"ERROR: cannot reach Kafka at {args.bootstrap_servers} -> {e!r}")
        print("Is the kafka container running? Use localhost:29092 from your machine, kafka:9092 inside Docker.")
        return 1

    stats = {"ok": 0, "failed": 0}
    errors = []

    def on_success(_metadata):
        stats["ok"] += 1

    def on_error(exc):
        stats["failed"] += 1
        errors.append(repr(exc))

    start = time.time()
    for i, record in enumerate(records, start=1):
        key = str(record["record_id"]).encode("utf-8")    # same record_id -> same partition
        try:
            producer.send(args.topic, key=key, value=to_json_bytes(record)) \
                    .add_callback(on_success).add_errback(on_error)
        except KafkaError as e:
            stats["failed"] += 1
            errors.append(repr(e))
        if args.delay:
            time.sleep(args.delay)
        if i % 5000 == 0:
            print(f"  queued {i}/{total}")

    try:
        producer.flush(timeout=60)                        # wait until everything is delivered
    except KafkaError as e:
        errors.append(repr(e))
    producer.close()
    elapsed = time.time() - start

    print("-" * 50)
    print(f"Topic        : {args.topic}")
    print(f"Rows in CSV  : {total}")
    print(f"Delivered OK : {stats['ok']}")
    print(f"Failed       : {stats['failed']}")
    print(f"Time         : {elapsed:.1f}s")
    if errors:
        print("First error  :", errors[0])
    if stats["ok"] != total or stats["failed"]:
        print("RESULT: FAILED")
        return 1
    print("RESULT: SUCCESS - all rows sent")
    return 0


if __name__ == "__main__":
    sys.exit(main())
