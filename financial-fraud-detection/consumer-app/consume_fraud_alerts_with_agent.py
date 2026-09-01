#!/usr/bin/env python3
"""
Dedicated fraud alert consumer that synchronously calls a watsonx Orchestrate agent,
validates the response, publishes the enriched response, and then commits offsets.

Usage:
    python consume_fraud_alerts_with_agent.py
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any, Dict, cast

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringDeserializer
from dotenv import load_dotenv

from agent_payload_builder import build_agent_request
from agent_response_producer import build_response_producer, get_agent_response_topic, publish_agent_response
from orchestrate_client import call_agent_sync
from response_validator import validate_agent_response

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)

LOGGER = logging.getLogger("fraud-alert-agent-consumer")


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
            "group.id": os.getenv("AGENT_CONSUMER_GROUP_ID", "fraud-alert-agent-consumer-group"),
            "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
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


def load_fraud_alert_input_schema() -> str:
    schema_path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "../confluent-setup/schemas/fraud-alert-input.schema.json")
    )
    with open(schema_path, encoding="utf-8") as schema_file:
        return schema_file.read()


def fraud_alert_from_dict(data: object, ctx: SerializationContext) -> Dict[str, Any]:
    return cast(Dict[str, Any], data)


def build_value_deserializer() -> JSONDeserializer:
    return JSONDeserializer(
        load_fraud_alert_input_schema(),
        from_dict=fraud_alert_from_dict,
    )


def format_banner(title: str, width: int = 72, fill: str = "=") -> str:
    return f" {title} ".center(width, fill)


def summarize_route(payload: Dict[str, Any]) -> str:
    travel = payload.get("travelAnalysis", {}) or {}
    return f"{travel.get('firstLocation')} → {travel.get('secondLocation')}"


def summarize_time_window(payload: Dict[str, Any]) -> str:
    travel = payload.get("travelAnalysis", {}) or {}

    def as_number(value: Any) -> float | None:
        try:
            if value in (None, ""):
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    time_minutes = as_number(travel.get("timeMinutes"))
    distance_km = as_number(travel.get("distanceKm"))
    speed_kmh = as_number(travel.get("speedKmh"))

    time_text = f"{time_minutes:.1f} min" if time_minutes is not None else "n/a"
    distance_text = f"{distance_km:,.0f} km" if distance_km is not None else "n/a"
    speed_text = f"{speed_kmh:,.0f} km/h" if speed_kmh is not None else "n/a"

    return f"{time_text} | {distance_text} | {speed_text}"


def summarize_agent_reasoning(payload: Dict[str, Any]) -> str:
    for key in ("reason", "rationale", "summary", "explanation", "decisionExplanation"):
        value = payload.get(key)
        if value:
            return str(value)

    decision = payload.get("agentDecision", {}) or {}
    for key in ("analystSummary", "actionRationale", "reason", "rationale", "summary", "explanation"):
        value = decision.get(key)
        if value:
            return str(value)

    reasoning = decision.get("reasoning")
    if isinstance(reasoning, list) and reasoning:
        return "; ".join(str(item) for item in reasoning if item)

    return "No business explanation returned"


def log_startup(input_topic: str, output_topic: str) -> None:
    LOGGER.info(
        "\n%s\nInput topic : %s\nOutput topic: %s\n%s",
        format_banner("FRAUD ALERT AGENT CONSUMER"),
        input_topic,
        output_topic,
        "=" * 72,
    )


def log_processing_context(message: Any, payload: Dict[str, Any], agent_request: Dict[str, Any]) -> None:
    LOGGER.info(
        "\n%s\nAlert         : %s\nCard          : %s\nRoute         : %s\nTravel        : %s\nKafka         : partition=%s offset=%s\n%s",
        format_banner("1. FRAUD ALERT RECEIVED", fill="-"),
        payload.get("alertMessage"),
        payload.get("cardHash"),
        summarize_route(agent_request),
        summarize_time_window(agent_request),
        message.partition(),
        message.offset(),
        "-" * 72,
    )


def log_agent_pitstop(agent_name: str) -> None:
    LOGGER.info(
        "\n%s\nAgent         : %s\nStatus        : Invoking watsonx Orchestrate and waiting for response\n%s",
        format_banner("2. AGENT REVIEWING CASE", fill="-"),
        agent_name,
        "-" * 72,
    )


def log_agent_response_received() -> None:
    LOGGER.info(
        "\n%s\nStatus        : Agent response received and schema validated\n%s",
        format_banner("3. AGENT DECISION READY", fill="-"),
        "-" * 72,
    )


def log_agent_result(payload: Dict[str, Any]) -> None:
    decision = payload.get("agentDecision", {}) or {}
    LOGGER.info(
        "Decision       : %s\nConfidence     : %s (%s)\nSummary        : %s",
        decision.get("suggestedAction"),
        decision.get("confidenceLevel"),
        decision.get("confidenceScore"),
        summarize_agent_reasoning(payload),
    )


def log_publish_success(payload: Dict[str, Any], topic: str) -> None:
    LOGGER.info(
        "\n%s\nCard          : %s\nOutput topic   : %s\nStatus        : Published successfully\n%s",
        format_banner("4. RESULT PUBLISHED", fill="-"),
        payload.get("cardHash"),
        topic,
        "-" * 72,
    )


def main() -> None:
    input_topic = os.getenv("RESULT_TOPIC", "bank.fraud.transactions")
    output_topic = get_agent_response_topic()
    agent_name = os.getenv("WXO_AGENT_ID_OR_NAME", "unknown-agent")

    consumer = build_consumer()
    producer = build_response_producer()
    _schema_registry_client = build_schema_registry_client()
    value_deserializer = build_value_deserializer()
    key_deserializer = StringDeserializer("utf_8")

    consumer.subscribe([input_topic])
    log_startup(input_topic, output_topic)

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
                _message_key = key_deserializer(
                    message.key(),
                    SerializationContext(input_topic, MessageField.KEY),
                )

                payload = value_deserializer(
                    raw_value,
                    SerializationContext(input_topic, MessageField.VALUE),
                )

                if payload is None:
                    LOGGER.warning("Skipping null-deserialized message at offset=%s", message.offset())
                    consumer.commit(message=message)
                    continue

                typed_payload = cast(Dict[str, Any], payload)
                agent_request = build_agent_request(typed_payload)
                log_processing_context(message, typed_payload, agent_request)

                log_agent_pitstop(agent_name)
                agent_response = call_agent_sync(agent_request)
                validated_response = validate_agent_response(agent_response)
                log_agent_response_received()

                log_agent_result(validated_response)
                publish_agent_response(producer, validated_response)
                log_publish_success(validated_response, output_topic)

                consumer.commit(message=message)
            except Exception as exc:
                LOGGER.error(
                    "\n%s\nKafka         : partition=%s offset=%s\nStatus        : Processing failed\nReason        : %s\n%s",
                    format_banner("PROCESSING ERROR", fill="!"),
                    message.partition(),
                    message.offset(),
                    str(exc),
                    "!" * 72,
                )
    except KeyboardInterrupt:
        LOGGER.info("\n%s", format_banner("SHUTDOWN", fill="-"))
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()

# Made with Bob
