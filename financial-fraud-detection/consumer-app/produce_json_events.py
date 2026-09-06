#!/usr/bin/env python3
"""
Produce one or more JSON transaction files to a Kafka topic.
Timestamps are reset to the current time so Flink watermarks accept them.
Each file represents one transaction; the relative time gap between files is
preserved using a 10-minute spacing so impossible-travel pairs still trigger.

Usage:
    python3 produce_json_events.py --topic bank.transactions.yourname.2 \
        ../confluent-setup/data/manual_test-01-nyc.json \
        ../confluent-setup/data/manual_test-02-london.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, cast

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)

LOGGER = logging.getLogger("produce-json-events")

# Gap applied between each file so Flink sees distinct timestamps
_EVENT_GAP_MINUTES = 10


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def format_banner(title: str, width: int = 72, fill: str = "=") -> str:
    return f" {title} ".center(width, fill)


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": require_env("KAFKA_BOOTSTRAP_SERVERS"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": require_env("KAFKA_API_KEY"),
            "sasl.password": require_env("KAFKA_API_SECRET"),
        }
    )


def build_schema_registry_client() -> SchemaRegistryClient:
    return SchemaRegistryClient(
        {
            "url": require_env("SCHEMA_REGISTRY_URL"),
            "basic.auth.user.info": (
                f"{require_env('SCHEMA_REGISTRY_API_KEY')}:{require_env('SCHEMA_REGISTRY_API_SECRET')}"
            ),
        }
    )


def load_transaction_schema() -> str:
    schema_path = Path(__file__).parent.parent / "confluent-setup" / "schemas" / "card-present-transaction.avsc"
    return schema_path.read_text(encoding="utf-8")


def build_value_serializer() -> AvroSerializer:
    serializer: AvroSerializer = AvroSerializer(  # type: ignore  # confluent-kafka stubs incomplete
        build_schema_registry_client(),
        load_transaction_schema(),
        lambda obj, ctx: cast(Dict[str, Any], obj),
        conf={
            "auto.register.schemas": False,
            "use.latest.version": True,
        },
    )
    return serializer


def load_json_file(path: str) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def coerce_transaction(raw: Dict[str, Any], event_time_ms: int) -> Dict[str, Any]:
    """Normalize types to match card-present-transaction.avsc and apply current timestamp.

    cardPresent is stored as a string in the Avro schema ("true"/"false").
    Nullable fields (merchantName, merchantState, mcc) use None, not "".
    """
    card_present = raw.get("cardPresent", True)
    if isinstance(card_present, bool):
        card_present_str = "true" if card_present else "false"
    else:
        card_present_str = "true" if str(card_present).strip().lower() in {"true", "1", "yes"} else "false"

    return {
        "transactionId": str(raw["transactionId"]),
        "cardHash": str(raw["cardHash"]),
        "accountId": str(raw["accountId"]),
        "eventTime": event_time_ms,
        "amount": float(raw["amount"]),
        "currency": str(raw["currency"]),
        "merchantId": str(raw["merchantId"]),
        "merchantName": raw.get("merchantName") or None,
        "merchantCity": str(raw["merchantCity"]),
        "merchantState": raw.get("merchantState") or None,
        "merchantCountry": str(raw["merchantCountry"]),
        "merchantLat": float(raw["merchantLat"]),
        "merchantLon": float(raw["merchantLon"]),
        "mcc": raw.get("mcc") or None,
        "terminalId": str(raw["terminalId"]),
        "posEntryMode": str(raw["posEntryMode"]),
        "cardPresent": card_present_str,
        "authResult": str(raw["authResult"]),
        "channel": str(raw.get("channel", "POS")),
    }


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        LOGGER.error("Delivery failed for %s: %s", msg.key(), err)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Produce JSON transaction files to a Kafka topic with current timestamps."
    )
    parser.add_argument("--topic", required=False, default=None, help="Kafka topic (overrides KAFKA_TOPIC env var).")
    parser.add_argument("--send-delay", dest="send_delay", type=float, default=0.0, help="Seconds between messages.")
    parser.add_argument("files", nargs="+", help="One or more JSON transaction files to produce.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    topic = args.topic or require_env("KAFKA_TOPIC")
    files: List[str] = args.files

    producer = build_producer()
    value_serializer: AvroSerializer = build_value_serializer()
    key_serializer = StringSerializer("utf_8")

    base_time = datetime.now(timezone.utc)
    total = len(files)

    LOGGER.info("\n%s\nTopic : %s\nFiles : %d\n%s", format_banner("JSON EVENT PRODUCER"), topic, total, "=" * 72)

    for index, file_path in enumerate(files, start=1):
        event_time = base_time + timedelta(minutes=_EVENT_GAP_MINUTES * (index - 1))
        event_time_ms = int(event_time.timestamp() * 1000)

        raw = load_json_file(file_path)
        payload = coerce_transaction(raw, event_time_ms)

        filename = Path(file_path).name
        LOGGER.info(
            "[%d/%d] %s → card: %s | %s, %s",
            index,
            total,
            filename,
            payload["cardHash"],
            payload["merchantCity"],
            payload["merchantCountry"],
        )

        producer.produce(
            topic=topic,
            key=key_serializer(payload["cardHash"]),
            value=value_serializer(payload, SerializationContext(topic, MessageField.VALUE)),
            callback=delivery_report,
        )
        producer.poll(0)

        if args.send_delay > 0 and index < total:
            time.sleep(args.send_delay)

    producer.flush()
    LOGGER.info("\n%s\nDone. %d message(s) sent to %s.\n%s", format_banner("DONE"), total, topic, "=" * 72)


if __name__ == "__main__":
    main()

# Made with Bob
