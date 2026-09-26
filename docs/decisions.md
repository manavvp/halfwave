# Design decisions

Format: Context / Decision / Alternatives / Trade-off / Interview angle.
Numbered in order; superseded entries stay and are marked, never deleted.

---

## D001: Kafka provider is Confluent Cloud
*2026-09-27*

- **Context:** Kafka has to be hosted and reachable from Databricks serverless, so localhost is out.
  The riskiest platform unknown is `from_avro` + Schema Registry on serverless.
- **Decision:** Confluent Cloud, with Confluent Schema Registry.
- **Alternatives:** Redpanda Serverless. It's simpler to operate and has a Confluent-compatible SR,
  but it uses SCRAM auth, and Databricks' Kafka/SR docs and examples are written against Confluent.
  Its "easy to run locally" advantage doesn't apply because we can't use a local broker.
- **Trade-off:** The signup credits are time-limited and the project will probably outlive them.
  To keep costs down, bound the firehose runs, delete the firehose topic afterwards, and keep volume
  small. Switching providers later only changes the bootstrap servers, credentials and SR URL,
  because the Kafka protocol is the same.
- **Interview angle:** "I picked the provider that cut down the unknowns in my riskiest integration,
  and kept the switching cost to configuration only."

## D002: Bronze is append-only with idempotent batch writes (no MERGE)
*2026-09-27. Supersedes the original spec's "MERGE on (source, source_event_id)".*

- **Context:** `foreachBatch` is at-least-once. If the job dies after writing a micro-batch but
  before the checkpoint records it as committed, Spark re-runs that same batch (same `batchId`,
  same offsets) on restart. Without protection, the rows land twice.
- **Decision:** Bronze appends raw rows plus Kafka metadata. Every write inside `foreachBatch` sets
  `txnAppId = <stable per-table id>` and `txnVersion = batchId`. Delta records the
  `(appId, version)` pair in its transaction log and skips any later write whose version has already
  been committed, so a replayed batch is a no-op. Bronze does not dedup at all.
- **Alternatives:** MERGE on `(source, source_event_id)`. It's idempotent too, but every batch has
  to join against the whole target table to find matches, so the cost grows as the table grows. The
  firehose table is exactly where we want clean throughput numbers, and we're on a daily compute
  quota. It would also dedup a second time, repeating what silver does (D003).
- **Trade-off:** Bronze contains source duplicates, which is on purpose: it's a faithful log and
  the evidence for the duplicate drill. **Gotcha:** if a checkpoint is deleted or reset, `batchId`
  restarts at 0 and every write would be silently skipped as "already done." A checkpoint reset
  therefore needs a new `txnAppId`. This is recorded in the runbook.
- **Interview angle:** "The sink is idempotent per batch, not per row. Retries are absorbed by
  batch identity, so bronze never needs a MERGE."

## D003: Dedup happens exactly once, in silver
*2026-09-27*

- **Context:** There are two different ways a duplicate can appear. (1) *Pipeline retries*: the
  same Kafka offsets are processed twice. D002 handles those. (2) *Source duplicates*: the same
  real-world event is produced twice (a replay, a re-poll, a producer retry) and lands at
  *different* offsets. Offset-based protection can't see those.
- **Decision:** Silver runs `dropDuplicatesWithinWatermark` on `(source, source_event_id)` with an
  event-time watermark. That is the only dedup. Gold `unique` tests act as a backstop that
  *detects* anything that slips through; they don't fix it.
- **Alternatives:** Dedup in bronze (MERGE) as well as silver. Rejected because it's redundant and
  you'd have to answer "why both?". Plain `dropDuplicates` without a watermark was also rejected,
  because its state grows forever.
- **Trade-off:** Streaming dedup state is bounded by the watermark. Two copies further apart than
  the watermark delay are not both kept in state. Rows older than the watermark are dropped as late,
  so re-running an old backfill is silently ignored. That's correct for duplicates, but it means
  real reprocessing needs a full refresh (D006).
- **Interview angle:** "Each kind of duplicate has exactly one owner. Offsets are handled at the
  sink, and business identity is handled in silver."

## D004: Deterministic `source_event_id` per source
*2026-09-27*

- **Context:** D003 dedups on `source_event_id`. It has to be identical every time the same
  real-world fact is seen, so it can't use poll timestamps or random UUIDs.
- **Decision:**

  | Source | `source_event_id` |
  |---|---|
  | GH Archive | GitHub event `id` |
  | HN (Algolia) | `objectID` |
  | PyPI (pypistats) | `{package}:{date}:{category}` |
  | Docker Hub | `{namespace}/{image}:{snapshot_date_utc}`. One snapshot per UTC day, and the first poll of the day wins |

- **Alternatives:** Hash the whole payload. Rejected because the same fact re-fetched with a changed
  field (such as an HN points count) would get a new ID and sneak past dedup.
- **Trade-off:** Docker's rule throws away intra-day polls. That's fine, because the signal is a
  daily delta.
- **Interview angle:** "Idempotency is only as good as the key. I derived the key from the source's
  identity for the fact, never from when I happened to see it."

## D005: Streaming daily aggregation only for GH Archive and HN
*2026-09-27*

- **Context:** The spec's single streaming aggregation is daily buckets per entity per signal.
  PyPI and Docker Hub already arrive as one row per day.
- **Decision:** The watermarked daily aggregation runs on GH Archive and HN events. PyPI and Docker
  rows are normalized into the event model and go straight on. Rolling windows and Docker
  snapshot deltas stay in dbt.
- **Alternatives:** Aggregate every source uniformly. Rejected because it's an identity operation
  on daily data, it adds state, and it delays output until the watermark passes, for no benefit.
- **Trade-off:** Two paths through silver instead of one. The common event model keeps downstream
  code uniform.
- **Interview angle:** "I put stateful streaming only where events are finer-grained than the
  output. Anything already at the target grain doesn't need state."

## D006: Backfill is 90 days, streamed, in event-time order; reprocessing = full refresh
*2026-09-27*

- **Context:** I measured one GH Archive day (Sunday 2026-09-20) with HEAD requests: 24 hourly
  files, ~403 MB compressed. Weekdays are probably higher, so 90 days is roughly 36–55 GB of
  download. The earlier worry about "hundreds of GB" was wrong. Watermarks drop rows that arrive
  out of order by more than the delay.
- **Decision:** The backfill producer streams each hourly file, filters to registry repos, and
  discards it without writing to disk. It produces hours in ascending order. It's validated on
  1 day before the full 90. To reprocess silver (for example after changing resolution rules),
  run an SDP full refresh from bronze.
- **Alternatives:** Download files and keep them locally. Rejected because it costs disk for no
  benefit. The GitHub stargazers API was also rejected: it covers stars only, and it would be a
  different source from the firehose.
- **Trade-off:** Re-downloading is needed if the filter changes. That's acceptable because it's
  bounded and cheap.
- **Interview angle:** "A backfill fights the watermark unless it's ordered. I ordered the replay
  and made reprocessing an explicit full refresh from an immutable bronze."

## D007: Delivery is a vertical slice first
*2026-09-27*

- **Context:** Applications go out ~Oct 7. It's 10 days to learn six technologies and build four
  sources.
- **Decision:** By Oct 7, GH Archive + PyPI run end to end: Kafka → bronze → silver → dbt, with
  drills and a README finding. HN, Docker deltas, SCD2, Mongo and FastAPI follow before onsites.
  Timeline in SPEC.md.
- **Alternatives:** Build every layer for all four sources at once. Rejected because it risks four
  sources stuck in bronze with nothing to show.
- **Trade-off:** The quarantine table stays thin until HN lands, because exact-alias sources rarely
  quarantine.
- **Interview angle:** "I shipped a thin, complete slice and widened it, rather than a wide pipeline
  with no output."
