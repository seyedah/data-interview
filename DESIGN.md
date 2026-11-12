# DESIGN.md — Architecture Decisions (1 page)

## 1. Scaling to billions of records

The current design (file → SQLite, single process) would be replaced:

- **Storage engine**: a distributed, columnar warehouse — ClickHouse /
  BigQuery / Snowflake — with partitioning by `event_date` and clustering by
  `src_ip`; `ORDER BY (event_date, src_ip)` keeps daily aggregations and
  per-IP lookups on a few disk ranges. Compression (ZSTD/LZ4) makes raw
  network logs ~10× smaller.
- **Ingest**: replace single-file ingest with chunked, parallel loads
  (Spark / Polars / DuckDB on object storage, or Kafka Connect bulk loaders).
  The `batch_id` concept stays — it becomes a manifest/partition column.
- **Compute**: the `clean` step becomes an idempotent
  `INSERT OVERWRITE ... PARTITION (event_date, batch_id)` job, so each date
  partition is rebuilt independently and failures retry per partition.
- **Mart**: pre-aggregated daily table materialized incrementally per date
  partition (exactly what `mart_daily_network_stats` does, but partition-wise);
  heavy ad-hoc analytics served from the warehouse, dashboards from the mart.
- **Validation**: push DQ checks into the platform (dbt tests / Great
  Expectations) with the same quarantine pattern, but the checks run
  distributed and results land in a metrics store for alerting.
- The data model and idempotency keys would not change — only the engine.

## 2. Streaming data

For continuous logs (e.g., Kafka topic of connection events):

- **Ingest**: Kafka replaces the CSV; consumers (Flink / Spark Structured
  Streaming) write micro-batches to the lake every 1–5 min.
- **Cleaning**: the same validation rules run per event in the stream job;
  bad events go to a dead-letter topic (the streaming equivalent of the
  quarantine table) — nothing is silently dropped.
- **Deduplication**: composite-key dedup moves into stateful stream
  processing (keyed state with TTL) or, more simply, stays as an
  idempotent upsert keyed by `(timestamp, src_ip, dst_ip, port)`.
- **Aggregation**: the daily mart becomes either an incrementally updated
  table (micro-batch upsert per event date) or a windowed aggregation with
  **watermarks** for the streaming case.
- **Idempotency**: identical guarantees — deterministic keys
  (`event_date+src_ip`, `batch_id` per micro-batch) so replays are safe.
- Late/out-of-order events handled via watermarks + allowed lateness; data
  older than that is routed to the backfill path (below).

## 3. Late-arriving data

- **Upserts, not appends**: every layer already keys rows
  (`batch_id`, composite key, `event_date+src_ip`) and uses
  `ON CONFLICT DO UPDATE` / DELETE+INSERT, so late records *overwrite* the
  same keys instead of duplicating them — late data is safe by construction.
- **Re-aggregation windows**: after each load, recompute the mart only for
  the `event_date`s touched by the new batch (in this design, the upsert
  recomputes from the clean layer, which already includes late rows).
- **Backfill procedure**: a late historical file is just another batch —
  `ingest → validate → clean` runs normally; `aggregate → load` merges its
  rows into the existing mart dates.
- **Corrections / amendments**: re-ingesting a corrected file creates a new
  batch; a periodic "as-of" reconciliation compares per-day totals across
  batches and flags drift; quarantine keeps the *reason* history so auditors
  can see what was rejected and why, when.
- If records can arrive arbitrarily late, keep a
  "restated partitions" report rather than silently mutating old aggregates,
  so downstream consumers can decide when to re-read a date.
