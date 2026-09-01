#!/usr/bin/env python3
"""
Consumer for fraud alert messages from the results topic.
Uses the fraud-alert-agent schema for proper deserialization.

Usage:
    python consume_fraud_alerts.py
"""

import logging
import os
import sys
from typing import Any, Dict, cast

from confluent_kafka.serialization import MessageField, SerializationContext, StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)

LOGGER = logging.getLogger("fraud-alert-consumer")


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def build_kafka_common_config() -> Dict[str, Any]:
    return {
        "bootstrap.servers": require_env("KAFKA_BOOTSTRAP_SERVERS"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": require_env("KAFKA_API_KEY"),
        "sasl.password": require_env("KAFKA_API_SECRET"),
    }


def build_consumer() -> Consumer:
    config = build_kafka_common_config()
    config.update(
        {
            "group.id": "fraud-alert-consumer-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    return Consumer(config)


def build_schema_registry_client() -> SchemaRegistryClient:
    return SchemaRegistryClient(
        {
            "url": require_env("SCHEMA_REGISTRY_URL"),
            "basic.auth.user.info": (
                f"{require_env('SCHEMA_REGISTRY_API_KEY')}:{require_env('SCHEMA_REGISTRY_API_SECRET')}"
            ),
        }
    )


def load_fraud_alert_schema() -> str:
    schema_path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "../confluent-setup/schemas/fraud-alert-input.schema.json")
    )
    with open(schema_path, encoding="utf-8") as schema_file:
        return schema_file.read()


def fraud_alert_from_dict(data: object, ctx: SerializationContext) -> Dict[str, Any]:
    return cast(Dict[str, Any], data)


def build_value_deserializer() -> JSONDeserializer:
    return JSONDeserializer(
        load_fraud_alert_schema(),
        from_dict=fraud_alert_from_dict,
    )


def format_banner(title: str, width: int = 72, fill: str = "=") -> str:
    return f" {title} ".center(width, fill)


def log_startup(topic: str) -> None:
    LOGGER.info(
        "\n%s\nMonitoring topic: %s\n%s",
        format_banner("FRAUD ALERT MONITOR"),
        topic,
        "=" * 72,
    )


def log_fraud_alert(message, payload: Dict[str, Any]) -> None:
    LOGGER.info(
        "\n🚨 FRAUD DETECTED | Card: %s | %s → %s | Distance: %.0f km in %.0f min (%.0f km/h)",
        payload.get("cardHash"),
        payload.get("firstLocation"),
        payload.get("secondLocation"),
        payload.get("distanceKm", 0),
        payload.get("timeMinutes", 0),
        payload.get("speedKmh", 0),
    )


def main() -> None:
    topic = os.getenv("RESULT_TOPIC", "bank.fraud.transactions")
    consumer = build_consumer()
    value_deserializer = build_value_deserializer()

    consumer.subscribe([topic])
    log_startup(topic)

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            raw_value = message.value()
            if raw_value is None:
                LOGGER.warning("Skipping empty message at offset=%s", message.offset())
                consumer.commit(message=message)
                continue

            try:
                payload = value_deserializer(raw_value, SerializationContext(topic, MessageField.VALUE))
                if payload is None:
                    LOGGER.warning("Skipping null-deserialized message at offset=%s", message.offset())
                    consumer.commit(message=message)
                    continue

                typed_payload = cast(Dict[str, Any], payload)
                log_fraud_alert(message, typed_payload)
                consumer.commit(message=message)
                
            except Exception as e:
                LOGGER.error("❌ Error processing message at offset %s: %s", message.offset(), str(e))
                consumer.commit(message=message)
    except KeyboardInterrupt:
        LOGGER.info("\n%s", format_banner("SHUTDOWN", fill="-"))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

# Made with Bob