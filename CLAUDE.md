# AI-Infra Adoption Radar

Event-driven data platform on Databricks that tracks ~25 AI-infrastructure projects
(inference engines, serving frameworks, local runtimes, LLM gateways) and answers one question:
**does rising attention translate into observable adoption?** Every derived signal must be
traceable back to its source records.

Full V1 scope, sources, success criteria and timeline: @docs/SPEC.md
Design decisions log: @docs/decisions.md

---

## How we work (read this first)

I'm Manav, the architect and reviewer. You implement. I'm new to this stack (Kafka, Spark
Structured Streaming, SDP, Databricks, dbt, MongoDB) and I'm learning it while we build, so
optimize for me *understanding* the system, not just for it working.

1. **Plan before code.** For anything beyond a trivial edit, propose a short plan first: files
   touched, approach, one alternative you considered and why you rejected it. Wait for approval.
2. **No silent design decisions.** Any non-trivial choice (keys, idempotency strategy, watermark
   duration, schema shape, partitioning) gets an entry in `docs/decisions.md` using this format:
   Context / Decision / Alternatives / Trade-off / Interview angle (one line on how I'd explain it).
3. **Teach in your summaries.** End each task with 2–4 sentences on the key concept it relied on
   (e.g. why MERGE on a natural key makes replays idempotent). Short, concrete, no lecturing.
4. **One layer at a time.** Each component must run and be tested before we move to the next.
   One commit per logical unit, conventional commit messages.
5. **Scope guard.** Do not add tools, services, or libraries that aren't listed under Stack.
   If something seems necessary, stop and ask. New ideas go to `docs/phase2.md` by default.
6. **Don't thrash on environment problems.** For networking, auth, quota or platform errors,
   stop after two failed attempts and summarize what you tried, what you observed, and your
   best hypothesis.
7. **Failure drills.** After each layer lands, propose 2–3 drills (kill mid-batch, replay
   duplicates, inject late events, change a schema) plus a script in `drills/` to run them.
   Record expected vs observed behaviour in `docs/runbook.md`.

---

## Stack (fixed for V1)

- **Kafka:** hosted (Confluent Cloud or Redpanda free tier), Avro + Schema Registry
- **Producers:** Python (`confluent-kafka`, `requests`)
- **Bronze:** hand-written Spark Structured Streaming on Databricks (`foreachBatch` + Delta MERGE)
- **Silver:** Lakeflow Spark Declarative Pipelines (SDP) with expectations
- **Storage/governance:** Delta Lake, Unity Catalog (catalog `radar`, schemas `bronze`/`silver`/`gold`)
- **Gold:** dbt (`dbt-databricks`)
- **Orchestration:** Databricks Jobs
- **Serving:** MongoDB Atlas free tier (`pymongo`) + FastAPI
- **Tooling:** Python 3.11, ruff, pytest

---

## Hard platform constraints (Databricks Free Edition)

- **Serverless only.** Structured Streaming supports only `Trigger.AvailableNow` (and `Once`).
  `processingTime` and continuous triggers fail. For near-continuous runs, use an `AvailableNow`
  job scheduled as a **continuous job**.
- **No custom Maven JARs on serverless.** Use the built-in Kafka source. Talk to external systems
  (MongoDB, HTTP APIs) with Python clients, not Spark connectors.
- **Verify before relying:** `from_avro` with Schema Registry on serverless. If unsupported, stop
  and raise it. Don't silently switch formats.
- **Daily compute quota.** Exceeding it shuts compute down for the day. Keep replay runs bounded
  and never leave a continuous job running unattended.
- **Kafka and MongoDB must be reachable from Databricks.** Hosted services only, no localhost.
  Serverless egress IPs aren't fixed, so Atlas network access may need to be open with strong
  credentials. Note this in the README.
- Checkpoints and landed files live in **Unity Catalog volumes**.
- **Secrets:** Databricks secret scopes on the platform, `.env` locally (gitignored). Never commit
  credentials.

**First task (day 1 spike):** prove connectivity from a Free Edition notebook to hosted Kafka
(read one message) and to MongoDB Atlas (write one document) before building anything else.

---

## Architecture

```
sources → Python producers → Kafka topics → BRONZE (Structured Streaming) → SILVER (SDP)
        → GOLD (dbt) → Mongo sync job → FastAPI
```

### Layer invariants (do not violate without a decisions.md entry)

- **Registry:** `config/entities.yaml` is the single source of truth for entities. Adding an
  entity must require only a YAML change. The registry is loaded into `dim_entity`.
- **Bronze:**
  - Raw payload plus Kafka metadata (topic, partition, offset, ingest_ts). No business logic.
  - Idempotent writes via MERGE on `(source, source_event_id)`.
  - One table per topic.
- **Silver:**
  - Normalize into the common event model (below).
  - Resolve entities against `dim_entity`. Unresolved or ambiguous records go to a quarantine table.
  - Dedup with `dropDuplicatesWithinWatermark` on event time.
  - The only streaming aggregation is daily buckets per entity per signal.
  - Expectations for data quality.
- **Rolling windows (7/14/30-day) live in dbt, never in streaming state.**
- **Gold:** dbt marts plus `signal_evidence`, which maps every signal row → silver event_ids →
  bronze offsets → source URLs. No composite "hype score"; any thresholds are documented in the
  model YAML.
- **Serving:** one denormalized document per entity, upserted by `entity_id`.

### Common event model (silver)

```
event_id, entity_id, source, event_type, signal_class (attention|activity|adoption),
event_ts (UTC, source time), ingest_ts, value, source_url, bronze_ref (topic/partition/offset)
```

---

## Repo layout

```
config/entities.yaml
schemas/                  # Avro schemas, one per topic
producers/
  sources/                # gharchive.py, hn.py, pypi.py, dockerhub.py
  replay.py               # modes: firehose (bounded window) | backfill (registry-filtered); speed 1x–1000x
databricks/
  bronze/                 # Structured Streaming jobs (thin wrappers)
  silver/                 # SDP pipeline definitions
transforms/               # pure PySpark functions: DataFrame in -> DataFrame out, unit-tested locally
dbt/                      # models/staging, models/marts, tests
serving/
  sync_mongo.py
  api/                    # FastAPI: GET /entity/{id}, GET /entities?sort=
drills/                   # failure-drill scripts
tests/
docs/                     # SPEC.md, decisions.md, phase2.md, runbook.md
```

---

## Conventions

- Type hints everywhere. Code must pass `ruff check .` and `pytest`.
- Business logic goes in `transforms/` as pure functions so it can be tested with a local
  SparkSession. Databricks notebooks and jobs are thin wrappers around them.
- Topic names: `radar.<source>.<type>.v1`. Partition keys: repo name, package, or entity_id.
- All timestamps UTC. `event_ts` is the source's event time; `ingest_ts` is when we saw it.
- Configuration comes from env vars and secret scopes. Nothing hardcoded.
- dbt: staging models map 1:1 to silver tables, marts live in `models/marts/`, and every model
  has `unique`/`not_null` tests on its keys.
- Use the current Lakeflow SDP Python API. Check the docs if unsure; don't default to legacy
  `dlt` syntax without saying so.

## Commands (keep this section updated as they become real)

```
ruff check . && pytest                                            # lint + unit tests
python producers/replay.py --mode backfill --days 90              # build history
python producers/replay.py --mode firehose --hours 24 --speed 100 # throughput run
cd dbt && dbt build                                               # models + tests
uvicorn serving.api.main:app --reload                             # local API
```

## Out of scope for V1

Widening beyond AI-infra, RAG/vector stores/LLMs, composite scores, any frontend beyond the
API, CDC, Kubernetes, Flink, Iceberg, Snowflake, text entity resolution over the full firehose.
Stretch goal only if V1 lands early: Databricks Asset Bundles + GitHub Actions.

## Definition of done (per task)

The code runs, tests pass, ruff is clean, decisions are logged, the runbook/README is updated if
behaviour changed, and the summary includes the concept explanation.
