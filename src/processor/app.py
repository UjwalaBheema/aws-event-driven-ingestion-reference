"""Processor Lambda: consumes Kinesis records and writes them idempotently.

Uses partial batch responses so one bad record does not block the rest of the
batch, and a conditional write so retries and replays never create duplicates.
Synthetic, employer-independent reference code.
"""
import base64
import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.environ.get("TABLE_NAME", "events")
SENSITIVE_FIELDS = {"name", "email", "phone", "patient_id", "ssn"}

_table = None


def _get_table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(TABLE_NAME)
    return _table


def redact(value):
    """Mask sensitive fields before anything is logged."""
    if isinstance(value, dict):
        return {
            k: "***" if k.lower() in SENSITIVE_FIELDS else redact(v)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [redact(v) for v in value]
    return value


def decode(record):
    return json.loads(base64.b64decode(record["kinesis"]["data"]))


def write_event(table, envelope):
    """Insert once per event_id. Returns False if it was already processed."""
    try:
        table.put_item(
            Item=envelope,
            ConditionExpression="attribute_not_exists(event_id)",
        )
        return True
    except ClientError as err:
        if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def handler(event, context):
    table = _get_table()
    failures = []

    for record in event.get("Records", []):
        sequence = record["kinesis"]["sequenceNumber"]
        try:
            envelope = decode(record)
            created = write_event(table, envelope)
            logger.info(
                "processed event_id=%s created=%s payload=%s",
                envelope["event_id"], created, redact(envelope["payload"]),
            )
        except Exception:
            logger.exception("failed record sequence=%s", sequence)
            failures.append({"itemIdentifier": sequence})

    return {"batchItemFailures": failures}
