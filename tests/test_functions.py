import base64
import importlib.util
import json
import pathlib
import sys
import types
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent


def load(name, relative):
    """Load a handler module by path, with boto3 stubbed so no AWS is needed."""
    boto3 = types.ModuleType("boto3")
    boto3.client = lambda *a, **k: None
    boto3.resource = lambda *a, **k: None
    botocore = types.ModuleType("botocore")
    exceptions = types.ModuleType("botocore.exceptions")

    class ClientError(Exception):
        def __init__(self, response, op):
            super().__init__(str(response))
            self.response = response

    exceptions.ClientError = ClientError
    sys.modules.update({"boto3": boto3, "botocore": botocore,
                        "botocore.exceptions": exceptions})
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, ClientError


class FakeKinesis:
    def __init__(self):
        self.records = []

    def put_record(self, **kwargs):
        self.records.append(kwargs)


class FakeTable:
    def __init__(self, ClientError):
        self.items = {}
        self.ClientError = ClientError

    def put_item(self, Item, ConditionExpression=None):
        if Item["event_id"] in self.items:
            raise self.ClientError(
                {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
        self.items[Item["event_id"]] = Item


class IngestTests(unittest.TestCase):
    def setUp(self):
        self.mod, _ = load("ingest_app", "src/ingest/app.py")
        self.kinesis = FakeKinesis()
        self.mod._kinesis = self.kinesis

    def test_accepts_valid_event(self):
        body = {"tenant_id": "t1", "event_type": "demo", "payload": {"a": 1}}
        res = self.mod.handler({"body": json.dumps(body)}, None)
        self.assertEqual(res["statusCode"], 202)
        self.assertEqual(self.kinesis.records[0]["PartitionKey"], "t1")

    def test_rejects_missing_fields(self):
        res = self.mod.handler({"body": json.dumps({"tenant_id": "t1"})}, None)
        self.assertEqual(res["statusCode"], 400)
        self.assertEqual(self.kinesis.records, [])

    def test_rejects_bad_json(self):
        res = self.mod.handler({"body": "{not json"}, None)
        self.assertEqual(res["statusCode"], 400)


class ProcessorTests(unittest.TestCase):
    def setUp(self):
        self.mod, self.ClientError = load("processor_app", "src/processor/app.py")
        self.table = FakeTable(self.ClientError)
        self.mod._table = self.table

    @staticmethod
    def record(seq, envelope):
        data = base64.b64encode(json.dumps(envelope).encode()).decode()
        return {"kinesis": {"sequenceNumber": seq, "data": data}}

    def test_writes_once_and_ignores_replays(self):
        env = {"event_id": "e1", "payload": {"email": "x@example.com"}}
        event = {"Records": [self.record("1", env), self.record("2", env)]}
        result = self.mod.handler(event, None)
        self.assertEqual(result, {"batchItemFailures": []})
        self.assertEqual(len(self.table.items), 1)

    def test_bad_record_is_reported_not_fatal(self):
        good = self.record("2", {"event_id": "e2", "payload": {}})
        bad = {"kinesis": {"sequenceNumber": "1", "data": "###"}}
        result = self.mod.handler({"Records": [bad, good]}, None)
        self.assertEqual(result["batchItemFailures"], [{"itemIdentifier": "1"}])
        self.assertIn("e2", self.table.items)

    def test_redacts_sensitive_fields(self):
        out = self.mod.redact({"email": "a@b.c", "nested": {"phone": "1"}, "ok": 2})
        self.assertEqual(out, {"email": "***", "nested": {"phone": "***"}, "ok": 2})


if __name__ == "__main__":
    unittest.main()

