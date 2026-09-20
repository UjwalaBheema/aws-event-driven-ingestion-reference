"""Ingest Lambda: validates an inbound event and publishes it to Kinesis.

API Gateway (HTTP API) -> this function -> Kinesis stream.
Synthetic, employer-independent reference code.
"""
import json
import os
import uuid
from datetime import datetime, timezone

import boto3

STREAM_NAME = os.environ.get("STREAM_NAME", "events-stream")
REQUIRED_FIELDS = ("tenant_id", "event_type", "payload")

_kinesis = None


def _client():
    global _kinesis
    if _kinesis is None:
        _kinesis = boto3.client("kinesis")
    return _kinesis


def _response(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def build_envelope(body):
    """Wrap the caller's payload with an id and timestamp for idempotency and tracing."""
    return {
        "event_id": body.get("event_id") or str(uuid.uuid4()),
        "tenant_id": body["tenant_id"],
        "event_type": body["event_type"],
        "received_at": datetime.now(timezone.utc).isoformat(),
        "payload": body["payload"],
    }


def handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "body must be valid JSON"})

    missing = [f for f in REQUIRED_FIELDS if f not in body]
    if missing:
        return _response(400, {"error": "missing fields", "fields": missing})

    envelope = build_envelope(body)

    # Partitioning by tenant keeps each tenant's events ordered within a shard.
    _client().put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(envelope).encode("utf-8"),
        PartitionKey=envelope["tenant_id"],
    )
    return _response(202, {"event_id": envelope["event_id"], "status": "accepted"})
