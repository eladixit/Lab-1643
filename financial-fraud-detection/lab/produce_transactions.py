#!/usr/bin/env python3
"""
Avro producer that reads transactions from a CSV and produces them to Kafka
using the card-present-transaction Avro schema.

Timestamps are adjusted to current time so events pass Flink watermarks.

Usage:
    python3 produce_transactions.py --csv-file path/to/transactions.csv
    python3 produce_transactions.py --csv-file path/to/transactions.csv --topic bank.transactions
"""

import argparse
import csv
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from dotenv import load_dotenv

# Load .env from this script's directory first, then root consumer-app/.env
_script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path=os.path.join(_script_dir, ".env"), override=False)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
LOGGER = logging.getLogger(__name__)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def format_banner(title: str, width: int = 72, fill: str = "=") -> str:
    return f" {title} ".center(width, fill)


# ---------------------------------------------------------------------------
# Kafka / Schema Registry builders
# ---------------------------------------------------------------------------

def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": require_env("KAFKA_BOOTSTRAP_SERVERS"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": require_env("KAFKA_API_KEY"),
        "sasl.password": require_env("KAFKA_API_SECRET"),
    })


def build_schema_registry_client() -> SchemaRegistryClient:
    return SchemaRegistryClient({
        "url": require_env("SCHEMA_REGISTRY_URL"),
        "basic.auth.user.info": (
            f"{require_env('SCHEMA_REGISTRY_API_KEY')}:{require_env('SCHEMA_REGISTRY_API_SECRET')}"
        ),
    })


def load_avro_schema() -> str:
    schema_path = os.path.join(_script_dir, "..", "avro_schemas", "card-present-transaction.avsc")
    with open(schema_path, encoding="utf-8") as f:
        return f.read()


def build_value_serializer() -> AvroSerializer:
    return AvroSerializer(
        schema_registry_client=build_schema_registry_client(),
        schema_str=load_avro_schema(),
        to_dict=lambda obj, ctx: obj,
        conf={
            "auto.register.schemas": False,
            "use.latest.version": True,
        },
    )


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def parse_timestamp(timestamp_str: str) -> datetime:
    if timestamp_str.endswith("Z"):
        timestamp_str = timestamp_str[:-1] + "+00:00"
    return datetime.fromisoformat(timestamp_str)


def adjust_timestamp(base_time: datetime, offset_seconds: int) -> int:
    """Return milliseconds since epoch for Flink TO_TIMESTAMP_LTZ."""
    return int((base_time + timedelta(seconds=offset_seconds)).timestamp() * 1000)


# ---------------------------------------------------------------------------
# CSV → transaction dict
# ---------------------------------------------------------------------------

def csv_row_to_transaction(
    row: Dict[str, str],
    base_time: datetime,
    time_offset: int,
    run_id: str,
) -> Dict[str, Any]:
    return {
        "transactionId": row["transactionId"],
        "cardHash": f"{row['cardHash']}_{run_id}",
        "accountId": row["accountId"],
        "eventTime": adjust_timestamp(base_time, time_offset),
        "amount": float(row["amount"]),
        "currency": row["currency"],
        "merchantId": row["merchantId"],
        "merchantName": row.get("merchantName") or None,
        "merchantCity": row["merchantCity"],
        "merchantState": row.get("merchantState") or None,
        "merchantCountry": row["merchantCountry"],
        "merchantLat": float(row["merchantLat"]),
        "merchantLon": float(row["merchantLon"]),
        "mcc": row.get("mcc") or None,
        "terminalId": row["terminalId"],
        "posEntryMode": row["posEntryMode"],
        "cardPresent": row.get("cardPresent", "true"),
        "authResult": row["authResult"],
        "channel": row.get("channel", "POS"),
    }


def read_transactions_from_csv(csv_path: str) -> List[Dict[str, Any]]:
    transactions: List[Dict[str, Any]] = []
    base_time = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())[:8]
    card_timings: Dict[str, list] = {}

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("CSV has no header row")

        required = {"transactionId", "cardHash", "accountId", "eventTime", "amount",
                    "currency", "merchantId", "merchantCity", "merchantCountry",
                    "merchantLat", "merchantLon", "terminalId", "posEntryMode", "authResult"}
        missing = sorted(required - set(reader.fieldnames))
        if missing:
            raise ValueError(f"CSV missing columns: {', '.join(missing)}")

        for index, row in enumerate(reader, start=2):
            card = row["cardHash"]
            card_timings.setdefault(card, []).append((index, row, parse_timestamp(row.get("eventTime", ""))))

    time_offset = 0
    for card, events in card_timings.items():
        events.sort(key=lambda x: x[2])
        for i, (_, row, orig_time) in enumerate(events):
            if i > 0:
                time_offset += int((orig_time - events[i - 1][2]).total_seconds())
            transactions.append(csv_row_to_transaction(row, base_time, time_offset, run_id))
        time_offset += 60  # 1-minute gap between cards

    return transactions


# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------

def delivery_report(err, msg) -> None:
    if err is not None:
        LOGGER.error("❌ Delivery failed: %s", err)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Produce CSV transactions to Kafka using Avro serialization."
    )
    parser.add_argument("--csv-file", dest="csv_file", required=True,
                        help="Path to the transactions CSV file.")
    parser.add_argument("--topic", dest="topic", default=None,
                        help="Kafka topic (overrides KAFKA_TOPIC env var).")
    parser.add_argument("--send-delay", dest="send_delay", type=float,
                        default=float(os.getenv("SEND_DELAY_SECONDS", "0")),
                        help="Optional delay in seconds between messages.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    topic = args.topic or require_env("KAFKA_TOPIC")

    producer = build_producer()
    value_serializer = build_value_serializer()
    key_serializer = StringSerializer("utf_8")
    transactions = read_transactions_from_csv(args.csv_file)

    LOGGER.info("\n%s\nProducing %d transactions to topic: %s\n%s",
                format_banner("TRANSACTION PRODUCER"), len(transactions), topic, "=" * 72)

    unique_cards = set()
    for i, payload in enumerate(transactions, start=1):
        unique_cards.add(payload["cardHash"])
        location = f"{payload['merchantCity']}, {payload['merchantCountry']}"
        LOGGER.info("[%d/%d] Card: %s | Location: %s | Amount: $%.2f",
                    i, len(transactions), payload["cardHash"], location, payload["amount"])
        producer.produce(
            topic=topic,
            key=key_serializer(payload["cardHash"]),
            value=value_serializer(payload, SerializationContext(topic, MessageField.VALUE)),
            callback=delivery_report,
        )
        producer.poll(0)
        if args.send_delay > 0 and i < len(transactions):
            time.sleep(args.send_delay)

    producer.flush()

    LOGGER.info("\n%s\nMessages sent: %d\n%s",
                format_banner("PRODUCER COMPLETE"), len(transactions), "=" * 72)
    LOGGER.info("Card hashes produced:")
    for card in sorted(unique_cards):
        LOGGER.info("  %s", card)


if __name__ == "__main__":
    main()
