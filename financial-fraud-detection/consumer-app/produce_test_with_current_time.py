#!/usr/bin/env python3
"""
Producer script that automatically adjusts CSV timestamps to current time.
This ensures test data works with Flink watermarks that filter out late events.

Usage:
    python3 produce_test_with_current_time.py --csv-file ../confluent-setup/data/test_global_impossible_travel.csv
"""

import argparse
import csv
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, cast

from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)

LOGGER = logging.getLogger("test-producer-current-time")


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


def load_transaction_json_schema() -> str:
    schema_path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "../confluent-setup/schemas/card-present-transaction.schema.json")
    )
    with open(schema_path, encoding="utf-8") as schema_file:
        return schema_file.read()


def transaction_to_dict(transaction: object, ctx: SerializationContext) -> Dict[str, Any]:
    return cast(Dict[str, Any], transaction)


def build_value_serializer() -> JSONSerializer:
    return JSONSerializer(
        load_transaction_json_schema(),
        build_schema_registry_client(),
        transaction_to_dict,
        conf={
            "auto.register.schemas": False,
            "use.latest.version": True,
        },
    )


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"true", "1", "yes", "y"}


def parse_timestamp(timestamp_str: str) -> datetime:
    """Parse ISO 8601 timestamp string to datetime object."""
    # Remove 'Z' and parse
    if timestamp_str.endswith('Z'):
        timestamp_str = timestamp_str[:-1] + '+00:00'
    return datetime.fromisoformat(timestamp_str)


def adjust_timestamp_to_now(original_timestamp: str, base_time: datetime, time_offset_seconds: int) -> int:
    """
    Adjust timestamp to current time while preserving relative time differences.
    
    Args:
        original_timestamp: Original timestamp from CSV
        base_time: Current time to use as base
        time_offset_seconds: Seconds to add to base time for this transaction
    
    Returns:
        Timestamp as milliseconds since epoch (BIGINT for Flink)
    """
    adjusted_time = base_time + timedelta(seconds=time_offset_seconds)
    # Convert to milliseconds since epoch for Flink TO_TIMESTAMP_LTZ
    return int(adjusted_time.timestamp() * 1000)


def csv_row_to_transaction(row: Dict[str, str], row_number: int, base_time: datetime, time_offset: int, run_id: str) -> Dict[str, Any]:
    """Convert CSV row to transaction dict with adjusted timestamp and unique card hash."""
    
    # Adjust the timestamp to current time
    adjusted_event_time = adjust_timestamp_to_now(row.get("eventTime", ""), base_time, time_offset)
    
    # Create unique card hash by appending run_id to original card hash
    original_card_hash = row["cardHash"]
    unique_card_hash = f"{original_card_hash}_{run_id}"

    return {
        "transactionId": row["transactionId"],
        "cardHash": unique_card_hash,
        "accountId": row["accountId"],
        "eventTime": adjusted_event_time,
        "amount": float(row["amount"]),
        "currency": row["currency"],
        "merchantId": row["merchantId"],
        "merchantName": row.get("merchantName", ""),
        "merchantCity": row["merchantCity"],
        "merchantState": row.get("merchantState", ""),
        "merchantCountry": row["merchantCountry"],
        "merchantLat": float(row["merchantLat"]),
        "merchantLon": float(row["merchantLon"]),
        "terminalId": row["terminalId"],
        "posEntryMode": row["posEntryMode"],
        "cardPresent": parse_bool(row.get("cardPresent", "true")),
        "authResult": row["authResult"],
        "mcc": row.get("mcc", ""),
        "channel": row.get("channel", "POS"),
    }


def read_transactions_from_csv(csv_path: str) -> List[Dict[str, Any]]:
    """Read transactions from CSV and adjust timestamps to current time."""
    transactions: List[Dict[str, Any]] = []
    base_time = datetime.now(timezone.utc)
    
    # Generate unique run ID for this batch (short UUID)
    run_id = str(uuid.uuid4())[:8]
    
    # Track card hashes and their transaction times to preserve relative timing
    card_timings: Dict[str, List[tuple[int, Dict[str, str], datetime]]] = {}

    with open(csv_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        required_columns = {
            "transactionId",
            "cardHash",
            "accountId",
            "amount",
            "currency",
            "merchantId",
            "merchantCity",
            "merchantCountry",
            "merchantLat",
            "merchantLon",
            "terminalId",
            "posEntryMode",
            "authResult",
        }

        if reader.fieldnames is None:
            raise ValueError("CSV file has no header row")

        missing_columns = sorted(required_columns - set(reader.fieldnames))
        if missing_columns:
            raise ValueError(f"CSV file is missing required columns: {', '.join(missing_columns)}")

        # First pass: group transactions by card and parse original timestamps
        for index, row in enumerate(reader, start=2):
            card_hash = row["cardHash"]
            if card_hash not in card_timings:
                card_timings[card_hash] = []
            
            original_time = parse_timestamp(row.get("eventTime", ""))
            card_timings[card_hash].append((index, row, original_time))

    # Second pass: adjust timestamps while preserving relative timing per card
    time_offset = 0
    for card_hash, card_transactions in card_timings.items():
        # Sort by original timestamp
        card_transactions.sort(key=lambda x: x[2])
        
        # Calculate time differences between transactions for this card
        for i, (index, row, original_time) in enumerate(card_transactions):
            if i == 0:
                # First transaction for this card starts at current offset
                transaction = csv_row_to_transaction(row, index, base_time, time_offset, run_id)
                transactions.append(transaction)
            else:
                # Subsequent transactions preserve the time difference from previous
                prev_original_time = card_transactions[i-1][2]
                time_diff_seconds = int((original_time - prev_original_time).total_seconds())
                time_offset += time_diff_seconds
                transaction = csv_row_to_transaction(row, index, base_time, time_offset, run_id)
                transactions.append(transaction)
        
        # Add gap between different cards
        time_offset += 60  # 1 minute gap between card groups

    return transactions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Produce Kafka messages from CSV with timestamps adjusted to current time."
    )
    parser.add_argument(
        "--csv-file",
        dest="csv_file",
        required=True,
        help="Path to a CSV file containing one transaction per row.",
    )
    parser.add_argument(
        "--topic",
        dest="topic",
        default=None,
        help="Kafka topic to produce to (overrides KAFKA_TOPIC env var).",
    )
    parser.add_argument(
        "--send-delay",
        dest="send_delay",
        type=float,
        default=float(os.getenv("SEND_DELAY_SECONDS", "0")),
        help="Optional delay in seconds between produced messages.",
    )
    return parser.parse_args()


def delivery_report(err, msg) -> None:
    if err is not None:
        LOGGER.error("❌ Delivery failed: %s", err)
        return
    # Silent success - only log errors


def log_startup(topic: str, total_messages: int, csv_path: str, send_delay_seconds: float) -> None:
    LOGGER.info(
        "\n%s\nProducing %s transactions to topic: %s\n%s",
        format_banner("TRANSACTION PRODUCER"),
        total_messages,
        topic,
        "=" * 72,
    )


def log_message_preview(index: int, total_messages: int, payload: Dict[str, Any]) -> None:
    location = f"{payload.get('merchantCity')}, {payload.get('merchantCountry')}"
    LOGGER.info(
        "[%d/%d] Card: %s | Location: %s | Coords: (%.4f, %.4f) | Amount: $%.2f",
        index,
        total_messages,
        payload.get("cardHash"),
        location,
        payload.get("merchantLat"),
        payload.get("merchantLon"),
        payload.get("amount"),
    )


def main() -> None:
    args = parse_args()
    topic = args.topic if args.topic else require_env("KAFKA_TOPIC")
    csv_path = args.csv_file
    send_delay_seconds = args.send_delay

    producer = build_producer()
    value_serializer = build_value_serializer()
    key_serializer = StringSerializer("utf_8")
    transactions = read_transactions_from_csv(csv_path)

    log_startup(topic, len(transactions), csv_path, send_delay_seconds)

    # Track unique card hashes for fraud detection
    unique_card_hashes = set()
    
    for index, payload in enumerate(transactions, start=1):
        log_message_preview(index, len(transactions), payload)
        unique_card_hashes.add(payload["cardHash"])
        producer.produce(
            topic=topic,
            key=key_serializer(payload["cardHash"]),
            value=value_serializer(payload, SerializationContext(topic, MessageField.VALUE)),
            callback=delivery_report,
        )
        producer.poll(0)

        if send_delay_seconds > 0 and index < len(transactions):
            time.sleep(send_delay_seconds)

    producer.flush()
    
    # Log completion with card hashes to search for
    LOGGER.info("\n%s\nMessages sent: %s\n%s", format_banner("PRODUCER COMPLETE"), len(transactions), "=" * 72)
    LOGGER.info("\n%s", format_banner("FRAUD DETECTION - SEARCH FOR THESE CARD HASHES", fill="*"))
    LOGGER.info("To check for fraud alerts, run this query in Flink SQL:\n")
    LOGGER.info("SELECT * FROM `bank.fraud.transactions`")
    LOGGER.info("WHERE cardHash IN (")
    for card_hash in sorted(unique_card_hashes):
        LOGGER.info("  '%s',", card_hash)
    LOGGER.info(");")
    LOGGER.info("\nOr use the check script:")
    LOGGER.info("python3 check_fraud_results.py\n")
    LOGGER.info("%s\n", "*" * 72)


if __name__ == "__main__":
    main()

# Made with Bob