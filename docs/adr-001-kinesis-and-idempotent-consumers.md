# ADR 001: Kinesis with idempotent consumers

Status: accepted

## Context

The system accepts events from many tenants over HTTP and must process them reliably, in order per tenant, without losing or duplicating results. Consumers can fail and retry, and streams can be replayed.

## Decision

Use API Gateway and a thin ingest Lambda to publish to Kinesis, partitioned by `tenant_id`. Consumers are written to be idempotent: every event carries an `event_id`, and the write to the store is conditional on that id not existing.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| SQS standard queue | No ordering guarantees per tenant; replay is not possible once messages are consumed |
| SQS FIFO | Ordering per group is available, but throughput limits and no multi-consumer replay |
| Direct synchronous writes to the database | Couples the API's availability and latency to the database and offers no buffering for spikes |

## Consequences

The main benefits are per-tenant ordering within a shard and the ability to replay from the stream for backfills or bug fixes. At-least-once delivery is safe because consumers are idempotent. Partial batch responses (`ReportBatchItemFailures`) and batch bisecting isolate poison records, and exhausted records go to a dead-letter queue for inspection.

The costs: a hot tenant can create a hot shard (mitigate by adding a suffix to the partition key for very large tenants, or by splitting the stream), and shard count is a capacity decision that must be monitored with iterator age and write-throttling alarms.
