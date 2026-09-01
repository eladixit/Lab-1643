#!/usr/bin/env python3
"""
Helpers to convert inbound fraud alert events into a watsonx Orchestrate agent request.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict


def epoch_millis_to_iso8601(value: Any) -> str:
    """
    Convert epoch milliseconds or passthrough ISO timestamps into ISO 8601 UTC format.
    """
    if value is None or value == "":
        return ""

    if isinstance(value, str):
        return value

    if isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")

    return str(value)


def shift_iso8601_minutes(timestamp_text: str, minutes: float) -> str:
    if not timestamp_text:
        return ""
    normalized = timestamp_text.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    shifted = dt + timedelta(minutes=minutes)
    return shifted.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def coalesce(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def string_or_empty(*values: Any) -> str:
    value = coalesce(*values)
    return "" if value is None else str(value)


def number_or_zero(*values: Any) -> float:
    value = coalesce(*values)
    if value is None:
        return 0.0
    return float(value)


def compact_transaction_view(transaction: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep only the fields the agent needs and normalize the timestamp format.
    """
    return {
        "transactionId": string_or_empty(transaction.get("transactionId")),
        "eventTime": epoch_millis_to_iso8601(transaction.get("eventTime")),
        "amount": number_or_zero(transaction.get("amount")),
        "currency": string_or_empty(transaction.get("currency")),
        "merchantName": string_or_empty(transaction.get("merchantName")),
        "merchantCity": string_or_empty(transaction.get("merchantCity")),
        "merchantState": string_or_empty(transaction.get("merchantState")),
        "merchantCountry": string_or_empty(transaction.get("merchantCountry")),
        "mcc": string_or_empty(transaction.get("mcc")),
    }


def derive_location(transaction: Dict[str, Any]) -> str:
    city = transaction.get("merchantCity")
    country = transaction.get("merchantCountry")

    if city and country:
        return f"{city}, {country}"
    if city:
        return str(city)
    if country:
        return str(country)
    return "Unknown"


def build_flat_transaction(alert: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    merchant_city = string_or_empty(
        alert.get(f"{prefix}Location"),
        alert.get(f"{prefix}_city"),
    )
    merchant_country = string_or_empty(alert.get(f"{prefix}Country"))
    merchant_name = string_or_empty(
        alert.get(f"{prefix}MerchantName"),
        f"{merchant_city or prefix.title()} Merchant",
    )

    detected_at = epoch_millis_to_iso8601(coalesce(alert.get("detectedAt"), alert.get("first_event_ts")))
    elapsed_minutes = build_elapsed_minutes(alert) or 0.0

    direct_event_time = epoch_millis_to_iso8601(
        coalesce(
            alert.get(f"{prefix}TransactionTime"),
            alert.get(f"{prefix}_event_ts"),
        )
    )
    fallback_event_time = (
        shift_iso8601_minutes(detected_at, -elapsed_minutes)
        if prefix == "first"
        else detected_at
    )

    return {
        "transactionId": string_or_empty(
            alert.get(f"{prefix}TransactionId"),
            alert.get(f"{prefix}_txn_id"),
        ),
        "eventTime": string_or_empty(direct_event_time, fallback_event_time),
        "amount": number_or_zero(alert.get(f"{prefix}Amount")),
        "currency": string_or_empty(alert.get(f"{prefix}Currency"), "USD"),
        "merchantName": merchant_name,
        "merchantCity": merchant_city,
        "merchantState": string_or_empty(alert.get(f"{prefix}MerchantState")),
        "merchantCountry": merchant_country,
        "mcc": string_or_empty(alert.get(f"{prefix}Mcc"), "unknown"),
    }


def build_elapsed_minutes(alert: Dict[str, Any]) -> float | None:
    elapsed_seconds = alert.get("elapsed_sec")
    if elapsed_seconds in (None, ""):
        return None
    return float(elapsed_seconds) / 60.0


def build_travel_analysis(alert: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the normalized travel analysis block expected by the agent and response schema.
    """
    first_transaction = alert.get("firstTransaction", {}) or build_flat_transaction(alert, "first")
    second_transaction = alert.get("secondTransaction", {}) or build_flat_transaction(alert, "second")

    return {
        "firstLocation": string_or_empty(
            alert.get("firstLocation"),
            alert.get("first_city"),
            derive_location(first_transaction),
        ),
        "secondLocation": string_or_empty(
            alert.get("secondLocation"),
            alert.get("second_city"),
            derive_location(second_transaction),
        ),
        "distanceKm": number_or_zero(alert.get("distanceKm"), alert.get("distance_km")),
        "timeMinutes": number_or_zero(alert.get("timeMinutes"), build_elapsed_minutes(alert)),
        "speedKmh": number_or_zero(alert.get("speedKmh"), alert.get("speed_kmh")),
    }


def build_agent_request(alert: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the structured fraud payload to send to the watsonx Orchestrate agent.

    The consumer sends this payload as JSON text inside the agent message for a
    synchronous request/response pattern.
    """
    first_transaction = alert.get("firstTransaction", {}) or build_flat_transaction(alert, "first")
    second_transaction = alert.get("secondTransaction", {}) or build_flat_transaction(alert, "second")

    return {
        "cardHash": string_or_empty(alert.get("cardHash")),
        "accountId": string_or_empty(alert.get("accountId")),
        "alertMessage": string_or_empty(alert.get("alertMessage")),
        "detectedAt": epoch_millis_to_iso8601(coalesce(alert.get("detectedAt"), alert.get("first_event_ts"))),
        "firstTransaction": compact_transaction_view(first_transaction),
        "secondTransaction": compact_transaction_view(second_transaction),
        "travelAnalysis": build_travel_analysis(alert),
    }

# Made with Bob
