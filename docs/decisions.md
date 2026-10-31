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

## D008: Daily pollers run as a scheduled serverless Databricks Job
*2026-10-02*

- **Context:** Docker Hub, PyPI and HN are polled daily, and SPEC.md didn't say where producers
  run. Docker Hub only exposes the current pull count, so every missed day is a permanent gap.
  Verified 2026-10-02: serverless can reach hub.docker.com, pypistats.org and hn.algolia.com.
- **Decision:** Each poller is a plain Python module (`python -m producers.sources.<source>`)
  that reads only env vars. A thin notebook in `databricks/jobs/` copies secrets from scope
  `radar` into env vars and calls the module's `main()`. A Databricks Job runs it daily. The
  same code runs locally for development.
- **Alternatives:** A laptop cron or launchd job, rejected because it misses days whenever the
  laptop is asleep. Calling `dbutils.secrets` from inside the producer, rejected because the
  code would then only run on Databricks and would need mocks locally.
- **Trade-off:** It uses a little daily compute quota (seconds of serverless time per run). The
  backfill and firehose replays still run locally, because they're bounded, one-off and
  download-heavy.
- **Interview angle:** "The scheduler lives on the platform that's always on. The code doesn't
  know where it runs, because config comes from env vars and the wrapper adapts the platform's
  secret store."

## D009: `radar.dockerhub.snapshots.v1`: 1 partition, unlimited retention, delete policy, keyed by image
*2026-10-02*

- **Context:** About 13 messages a day. Bronze won't exist for days, and Confluent's default
  retention (7 days) would delete snapshots before bronze reads them. Kafka is our durable
  history until then.
- **Decision:** 1 partition, `retention.ms = -1` (unlimited), `cleanup.policy = delete`. The
  message key is the image name.
- **Alternatives:** **Compaction** (`cleanup.policy = compact`) looks natural for "latest value
  per key", but it would keep only the newest snapshot per image and destroy exactly the
  history we need. More partitions would add parallelism we don't need at 13 messages a day.
- **Trade-off:** Storage grows forever, by roughly a few KB a day, which is negligible. Adding
  partitions later would remap keys to partitions and break per-key ordering, so the
  partition count is fixed at creation.
- **Interview angle:** "Compaction is a table, retention is a log. I needed a log, because the
  value of a snapshot is in its history."

## D010: Entity list v1: 25 entities; Ray Serve excluded; controls and attention-only entities labelled
*2026-10-02*

- **Context:** Handles were verified against the GitHub, pypistats and Docker Hub APIs on
  2026-10-02. Coverage is uneven: many projects ship images on GHCR, NVIDIA NGC or a vendor
  registry, none of which publish pull counts.
- **Decision:** 25 entities across 4 types, listed in `config/entities.yaml`.
  - 13 have Docker Hub images, 19 have PyPI packages, and 3 are attention-only (MLC LLM,
    llamafile, Higress).
  - TGI and TorchServe (archived) and GPT4All (inactive) are kept as **controls**. Their
    attention should decay, which sanity-checks the pipeline.
  - Ray Serve is excluded: its repo and image cover all of Ray, so both signals would be
    polluted.
  - Every entity carries `adoption_proxy_notes` explaining what its numbers really measure.
  - Bare common words ("triton", "bifrost") are deliberately not aliases.
  - `producers/registry.py` rejects any alias or handle claimed by two entities, which keeps
    exact-match resolution unambiguous.
- **Alternatives:** Only entities with full coverage. Rejected because the coverage gap *is* a
  finding, and the attention-only entities still feed attention marts.
- **Trade-off:** Adoption comparisons are only valid where proxies exist and mean something. The
  marts must carry the per-entity caveat. **Follow-up for the GH Archive producer:** two repos
  were renamed (llamafile, Higress), and GH Archive records the name *at event time*. Matching
  must use the stable GitHub repo id, not the name.
- **Interview angle:** "I measured coverage before trusting the metric. Half the projects have
  no usable Docker signal, and the registry says so per entity."

## D011: Dates dropped; Bronze is built next, on the Docker Hub topic
*2026-10-21. Supersedes D007's dated timeline; the vertical-slice principle stays.*

- **Context:** The Oct 7 deadline has passed, and the goal is now depth of understanding rather
  than a date. The spec's order built the GH Archive producer before Bronze.
- **Decision:** No dates. Build order: (0) run the Docker poller live and schedule it,
  (1) Bronze on `radar.dockerhub.snapshots.v1`, (2) GH Archive replay producer feeding the same
  Bronze job, (3) PyPI producer, (4) Silver, (5) Gold, (6) end-to-end job + README,
  (7) HN, SCD2, Mongo + FastAPI.
- **Alternatives:** Keep the spec's order (GH Archive first). Rejected because 90 days of GitHub
  events is too much to inspect by eye while learning checkpoints and crash recovery.
- **Trade-off:** Bronze is first tested on a tiny, slow topic, so throughput behaviour only shows
  up in step 2. That's acceptable, because correctness is what needs to be learned first.
- **Interview angle:** "I proved the streaming mechanics on a topic small enough to verify every
  offset by hand, then pointed the same job at a high-volume topic."

## D012: Bronze is raw only: Kafka bytes plus metadata, never decoded
*2026-10-25. Decided by Manav.*

- **Context:** Bronze's invariant is "raw payload plus Kafka metadata". The open question was
  whether to also store a decoded copy of the Avro payload.
- **Decision:** Every bronze table has the same columns: `kafka_topic`, `kafka_partition`,
  `kafka_offset`, `kafka_timestamp`, `kafka_key`, `value_raw` (exact bytes, Confluent header
  included), `schema_id` (parsed from bytes 1–4, null if unframed), `ingest_ts`, and
  `bronze_batch_id` (the micro-batch, equal to the Delta `txnVersion`). Decoding with Schema
  Registry moves to silver.
- **Alternatives:** Decoded only, rejected: no re-decode path, and a single undecodable message
  would stop the stream for good (a poison pill). Raw plus decoded, rejected because it couples
  bronze to Schema Registry and duplicates storage, for convenience only.
- **Trade-off:** Bronze isn't human-readable without `from_avro`, and the poison-pill decision
  moves to silver's design. In exchange, bronze can never be blocked by a decode failure, one
  DDL serves every topic, and bronze doesn't talk to Schema Registry at all. `bronze_batch_id`
  lets a bad deploy be undone surgically (delete batches 120–135, replay) and joins rows to
  `stream_progress`.
- **Interview angle:** "Bronze is a byte-for-byte copy of the topic, so nothing downstream can
  break ingestion, and I can always re-decode."

## D013: `txnAppId` is derived from the checkpoint's query id
*2026-10-25. Resolves the gotcha recorded in D002. Decided by Manav.*

- **Context:** Delta skips a write whose `txnVersion` is ≤ the last version recorded for its
  `txnAppId`. Batch ids restart at 0 when a checkpoint is deleted or replaced. With a fixed
  `txnAppId`, every batch after a reset would be skipped as "already committed", new data
  included, and the job would still report success.
- **Decision:** `txnAppId = "<table>:<query_id>"`, where `query_id` comes from
  `<checkpoint>/metadata`. Spark writes that file when a checkpoint is created and reuses it on
  every restart. The `foreachBatch` function reads it on each batch. It never falls back to a
  fixed id.
- **Alternatives:** A fixed per-table id (the D002 design) plus a runbook warning. Rejected
  because a warning doesn't prevent silent data loss.
- **Trade-off:** After a checkpoint reset, bronze re-ingests everything from the earliest offset,
  which creates duplicates. Silver's dedup on `source_event_id` absorbs them, or the table can be
  truncated first.
- **Interview angle:** "When the checkpoint and the sink disagree, I fail toward visible
  duplicates, never toward invisible gaps."

## D014: One bronze notebook, run once per topic, each with its own checkpoint
*2026-10-25*

- **Context:** Four topics, each needing one bronze table (an invariant).
- **Decision:** `databricks/bronze/ingest_topic` takes `topic` as a parameter. The table is
  `radar.bronze.<source>_<type>_v<n>` and the checkpoint is
  `/Volumes/radar/bronze/checkpoints/<table>`.
  - `startingOffsets=earliest` applies only on first run; after that the checkpoint decides.
  - `failOnDataLoss=true`, so retention-deleted, unread offsets fail loudly.
  - `maxOffsetsPerTrigger=10000` bounds each micro-batch.
  - Trigger `AvailableNow`.
  - Each run's `recentProgress` is appended to `radar.bronze.stream_progress`.
- **Alternatives:** One query subscribed to every `radar.*` topic, splitting into tables inside
  `foreachBatch`. Rejected because one shared checkpoint couples the topics: a problem in one
  stalls all of them, and they can't be reset independently.
- **Trade-off:** One job task per topic. With four topics, that's fine.
- **Interview angle:** "One topic, one table, one checkpoint: failures and resets stay
  isolated."

## D015: Signals come from the GitHub API, not GH Archive; GH Archive was audited and retired
*2026-10-30. Decided by Manav. Supersedes the GH Archive rows of SPEC.md's sources, D004's
GH Archive id, and D006's hourly-file backfill. D006's event-time ordering still holds.*

- **Context:** Before building the GH Archive producer, I audited the source. I compared it with
  OpenDigger, an independent collector that has published files in GH Archive's format since
  2026-09-06, and checked file sizes for every hour from May to October 2026. I also counted
  events at 15:00 UTC on every other day, plus spot checks back to 2025. All sampled 2026-10-30.
  - **Coverage collapsed in stages.** 15:00 UTC on 2025-04-15 had 268k events and 9.9k stars. By
    2026-10-28 that hour had 3k events, 225 stars and no pushes. OpenDigger's file for the same
    hour has 692k events and 22.9k stars. Since 2026-09-06, GH Archive has held 0.9–37% of
    OpenDigger's bytes per day (median about 5%). vLLM gains about 100 stars a day, and GH
    Archive recorded about 1.
  - **Volume hid it.** From May to August 2026, daily file sizes looked normal (500–700 MB),
    but 85–97% of the events were pushes. Stars and PRs were at 1–5%. A volume health check
    would have called those months healthy.
  - **Some losses are upstream, at GitHub.** Tag `CreateEvent`s are absent from both archives
    since October 2025. In the same month, push, create and delete events moved to a separate
    id range (about 25 billion, against about 18 billion for everything else).
  - **Even the complete-looking mirror has caveats.** It's run by one volunteer and had a
    bandwidth crisis on Oct 19–24. It's about 10 GB a day, and its history starts on
    2026-09-06. Its files are grouped by when an event was observed, not when it happened.
    Pushes arrive with a p99 lateness of 32.5 h; stars, forks and releases arrive in under 1 h.
  - **The per-repo events API can't be ground truth.** vLLM's last 300 events span 27 days.
  - Public reports agree: igrigorik/gharchive.org issues #310, #320 and #323.
- **What I considered first:** keep GH Archive and flag "recording errors", leaving flagged
  hours out of the gold rolling windows. That fails here for three reasons:
  1. Degradation is the normal state, not the exception. No hour sampled since about March 2026
     had more than a few percent of the mirror's stars, so every window would be empty.
  2. The loss is per event type. A volume-based flag misses it, as May–August 2026 shows.
  3. You can't scale it back up. At 1% capture, 100 real stars show up as about 1, and
     1 × 100 = 100 ± 100, because counting noise swamps the signal.

  The principle survives: a collection gap is **global** (every repo dips at once), while a real
  dip is **local**. Completeness has to be measured against a reference the collector doesn't
  control: a second collector, the platform's own counters, or the sequence of event ids.
- **Decision (Direction 1):**
  - Attention and activity signals come from the **GitHub GraphQL API** with a read-only token:
    stars, forks, releases, opened and merged PRs, and opened issues, each timestamped by
    GitHub. Topic `radar.github.events.v1`.
  - A **daily repo snapshot** (star, fork, watcher and open issue/PR totals), topic
    `radar.github.snapshots.v1`. Gold reconciles counted star events against the change in
    GitHub's star counter between snapshots. That's the completeness check against an
    independent reference, for free.
  - **Firehose load test, parked:** 1–3 hours of OpenDigger (about 700k events an hour),
    downloaded once as a fixed test input. Own topic, own bronze table, short retention. It
    never feeds silver: only `stream_progress` and the README's throughput table read it.
    Taking a one-time download means the volunteer risk doesn't matter.
  - The official GH Archive host is no longer used.
- **Alternatives:**
  - **Direction 2:** OpenDigger as the signal source, with per-hour × per-event-type health flags
    plus snapshot reconciliation. More depth (real late data, completeness classification), but
    it means 10 GB a day, a single-volunteer dependency on the critical path, and history only
    from September. It's moved to phase2.md.
  - **GitHub REST:** its stargazer listing stops at 40k, and vLLM and Ollama are past that.
    GraphQL lists newest first.
- **Trade-off:** We lose the "real firehose" as the signal source. The API is polled per repo, so
  its *load* is tiny, and the load test covers scale instead. Stars that were later removed
  disappear from the API, which reconciliation makes visible. A token becomes a required
  secret.
- **Interview angle:** "I audited my source before trusting it, found it captured about 1% of
  stars, moved signals to an exact source, kept the archive only for load testing, and
  reconcile my counts against the platform's own counter every day."

## D016: GitHub producer: stateless "yesterday plus overlap" window, two topics, GitHub-id keys
*2026-10-30. Window rule decided by Manav (Q7); topic separation agreed (Q8).*

- **Context:** The producer needs to know which events to fetch on each run, and each fact needs
  an id that's identical every time it's re-fetched (D004).
- **Decision:**
  - **Window:** `--days` whole UTC days ending at `--date` (default yesterday), plus
    `--overlap-hours` (default 6) before them. It's a pure function of the arguments, so there's
    no cursor or state file. A crash is fixed by rerunning, a missed day by `--date`, and the
    backfill is `--days 90`. **Rule: the overlap must stay below silver's watermark delay**,
    otherwise re-fetched events that are genuinely new land behind the watermark and are
    silently dropped.
  - **Paging:** each list (stargazers, forks, releases, PRs, issues) is paged newest first and
    stops at the first page older than the window. PRs are ordered by `updatedAt`, so an old PR
    merged inside the window is still seen.
  - **Order:** all events for a run are sorted by `(event_ts, source_event_id)` before they're
    produced, so the backfill is in event-time order (D006).
  - **`source_event_id`:** `{event_type}:{repo_id}:{github_object_id}`, or for stars
    `star:{repo_id}:{user_id}:{starred_at}` (an unstar and re-star is a new fact). The snapshot
    uses `{repo}:{snapshot_date}`, the same as Docker Hub. `repo_id` is stable across renames,
    which settles D010's rename follow-up.
  - **Topics:** both have 1 partition, unlimited retention and the delete policy, keyed by
    `repo` (the D009 reasoning). The volume is a few thousand events a day, and one partition
    keeps the global event-time order the producer creates. Snapshots can't be re-fetched, so
    Kafka is their history until bronze reads them.
  - **Placement:** the daily run goes in the scheduled job (`databricks/jobs/poll_github`,
    D008). The 90-day backfill runs locally, once, **before** the first daily run feeds silver,
    because otherwise it arrives behind the watermark (D003).
- **Alternatives:**
  - **A high-water-mark cursor** (store the last time produced). Rejected: if you save it after
    producing, a crash in between gives duplicates; if you save it before, a crash loses data.
    Since dedup is needed either way, the cursor adds state without adding safety.
  - **One topic for events and snapshots:** rejected because they're different shapes going
    down two different silver paths (D005), just as Docker snapshots have their own topic.
  - **An Avro enum for `event_type`:** rejected, because adding a symbol later breaks readers
    that have the old schema. It's a string with documented values.
- **Trade-off:** Every daily run re-fetches 6 hours that were already produced, so bronze holds
  those duplicates (expected, and absorbed in silver). One partition caps consumer parallelism
  at 1, which doesn't matter at this volume.
- **Interview angle:** "The producer is stateless and at-least-once. The consumer is idempotent,
  so a cursor would have added state without adding any safety."

## D017: Producers apply backpressure when librdkafka's local queue is full
*2026-10-31. Found when the first 90-day GitHub backfill crashed.*

- **Context:** `produce()` only puts a message in librdkafka's local queue (100k messages by
  default); a background thread sends it. The backfill queued about 109k events in under a
  second, far faster than one partition could drain from a laptop to us-east-1. At message
  100,001, `produce()` raised `BufferError: Queue full`. The exception skipped `flush()`, and the
  client dropped all 100k queued messages on exit, so the events topic got nothing. The 25
  snapshots had already been delivered.
- **Decision:** `produce_records` catches `BufferError`, calls `poll(1)` to let deliveries free
  space, and retries the same message. It logs, per topic, how many messages were delivered, the
  time taken, messages per second, and how often it waited on a full queue. Per-message delivery
  logs move to DEBUG.
- **Alternatives:**
  - **Raise `queue.buffering.max.messages`.** Rejected: it absorbs bursts but only postpones the
    crash when the producer is continuously faster than the network, and the backfill grows with
    every entity.
  - **Tune `linger.ms` or the batch size.** These trade latency for throughput. Under a backlog,
    batches fill immediately, so linger has no effect. Neither tuning prevents the crash.
- **Trade-off:** If the broker is unreachable, the loop waits until queued messages time out
  (`message.timeout.ms`, 5 minutes by default) and come back as delivery errors. Recovery was
  just a rerun: the producer is stateless (D016), and silver's dedup absorbs the 25
  re-produced snapshots.
- **Interview angle:** "Produce isn't send. When the app is faster than the network, the
  buffer fills, and something has to push back. Java's producer blocks, and librdkafka
  raises, so I added the wait-and-retry myself."

## D018: Stars come only from the daily snapshot; fork events are the reconciled signal
*2026-10-31. Decided by Manav. Amends D015 (star events and star reconciliation) and D016 (the
star `source_event_id`).*

- **Context:** The first live run of the GitHub producer returned forks, PRs, issues and
  releases, but zero stars. A probe showed why: GraphQL `stargazers` returns `totalCount: 0`
  with no edges, and REST `/repos/{repo}/stargazers` returns 404, for vLLM and KServe alike.
  Since mid-2026, GitHub lists a repo's stargazers only to its admins and collaborators. The
  repo's `stargazerCount` is still public. The failure was silent: an empty list, not an error.
- **Decision:**
  - The producer no longer queries `stargazers`, so it can't silently return nothing again.
    A test asserts that the connection is absent.
  - The star signal is the **daily change in `stars`** between GitHub snapshots, computed in dbt
    like Docker Hub pull deltas. Its history starts with the first snapshot (2026-10-31).
  - D015's completeness check moves to forks: gold compares counted fork events per day with
    the daily change in the snapshot's `forks` counter.
  - Fork events (90-day history) become the event-level attention signal.
- **Alternatives:**
  - **Rebuild star history from OpenDigger WatchEvents** (back to 2026-09-06). It's the only
    public source left for star timestamps, but it means scanning about 10 GB a day. Moved to
    phase2.md.
  - **Keep the stargazers query, guarded** (warn when `totalCount` is 0 while `stargazerCount`
    is not). Rejected: it's code for a signal we know we can't get.
- **Trade-off:** Stars lose their 90-day history and per-event evidence. A daily delta is a net
  number (new stars minus unstars), so it can't be traced back to individual stargazers.
  Attention windows that need history lean on forks, PRs and issues until the snapshots
  accumulate.
- **Interview angle:** "The platform closed the endpoint and returned an empty list, not an
  error. I caught it because zero stars on vLLM was impossible. I moved stars to a counter and
  moved reconciliation to forks."
