# V1 Spec: AI-Infra Adoption Radar

This spec is frozen. Anything not listed here is Phase 2 by default (log it in `docs/phase2.md`).
Changes after the freeze are made only through an entry in `docs/decisions.md` (D-numbers cited inline).

## Objective

Track ~25 AI-infrastructure projects and measure whether rising **attention** translates into
observable **adoption**, with every signal traceable to source evidence. There is no composite
score; the methodology is explicit and lives in dbt.

## Entities

- ~25 projects defined in `config/entities.yaml`. Each entry has: id, type, aliases, GitHub repo,
  PyPI package, Docker Hub image, and `adoption_proxy_notes` (what the PyPI/Docker numbers actually
  measure for this project, e.g. "PyPI package is the client SDK, not the server").
- Types: `inference_engine`, `serving_framework`, `local_runtime`, `llm_gateway`.
- Examples: vLLM, SGLang, TGI, TensorRT-LLM, llama.cpp, Ollama, LMDeploy, MLC-LLM, Triton
  Inference Server, Ray Serve, KServe, LiteLLM.

## Sources (all flow through Kafka)

| Source | Signal class | Cadence | History |
|---|---|---|---|
| GH Archive (stars, forks, releases, PRs, contributors) | attention + activity | hourly files, replayed | full |
| Hacker News via Algolia API (text mentions) | attention | polled | full |
| PyPI downloads (pypistats) | adoption | daily | ~180 days |
| Docker Hub pull counts (cumulative snapshots) | adoption | daily | from first poll |

Known limitation: adoption history is shorter than attention history. State this in the README.

## Kafka

- Hosted cluster. One topic per source. Avro + Schema Registry. Keys are repo, package, or entity.
- The replay producer has two modes:
  - **firehose**: a bounded 1–3 day window of the full GH Archive stream, at 1x / 10x / 100x / 1000x. Used for throughput experiments.
  - **backfill**: registry repos only, over ~90 days, produced in event-time order. Hourly files
    are streamed and filtered, never stored (~400 MB/day compressed, measured; D006). Validate on
    1 day before running 90. Used to build the signal history.

## Bronze: hand-written Structured Streaming

- Kafka → one Delta table per topic, storing the raw payload plus Kafka metadata.
- `foreachBatch` + append-only Delta writes with `txnAppId`/`txnVersion`, so a retried micro-batch
  is a no-op (D002). Bronze keeps source duplicates; dedup happens once, in silver (D003).
- `AvailableNow` trigger. Scheduled job for normal operation; continuous job during replay demos.
- Checkpoints stored in a UC volume.
- Streaming query progress (input/processed rows per sec, batch duration) is written to a metrics
  table.

## Silver: SDP

- Normalize everything into the common event model.
- Entity resolution:
  - exact alias match for GitHub, PyPI and Docker Hub
  - alias match plus word-boundary and context rules for HN text
  - unresolved or ambiguous records go to a quarantine table
- `dropDuplicatesWithinWatermark` on `(source, source_event_id)` with event-time watermarks. This is
  the only dedup in the pipeline (D003, D004).
- Daily buckets per entity per signal for GH Archive and HN. This is the only streaming aggregation.
  PyPI and Docker Hub rows are already daily, so they're only normalized (D005).
- Reprocessing (e.g. after changing resolution rules) is an SDP full refresh from bronze (D006).
- Expectations for data quality.

## Gold: dbt

- `dim_entity`, `dim_source`
- `fct_entity_signal_daily`
- `entity_attention_daily`, `entity_adoption_daily`: rolling 7/14/30-day windows; Docker pull
  deltas computed from successive snapshots
- `entity_attention_vs_adoption`: growth comparison plus divergence flags, with documented thresholds
- `signal_evidence`: signal → silver event_ids → bronze offsets → source URLs
- dbt tests + source freshness checks

## Serving

- `serving/sync_mongo.py` builds one denormalized document per entity from gold and upserts it
  into MongoDB Atlas.
- FastAPI endpoints: `GET /entity/{id}`, `GET /entities?sort=`.

## Orchestration

One Databricks job: bronze stream → SDP pipeline → dbt → Mongo sync.

## Out of scope

Widening beyond AI-infra, RAG/vector/LLM, composite scores, frontend, CDC, Kubernetes, Flink,
Iceberg, Snowflake, text entity resolution over the full firehose.

## Done when

1. The registry drives everything; adding an entity requires no code changes.
2. All four sources land in bronze via Kafka.
3. A duplicate event is absorbed idempotently, and a late event is handled per the watermark. Both demonstrated.
4. Silver is resolved to entities, and the quarantine table is populated.
5. dbt marts produce attention/adoption signals, and all tests pass.
6. Any signal traces back to its source URL.
7. `GET /entity/vllm` returns the full document.
8. Firehose replay at multiple speeds has recorded throughput numbers.
9. The README covers the Kafka justification and known limitations.

## Open decisions

- ~~Hosted Kafka provider~~ → Confluent Cloud (D001)
- MongoDB (default) vs DynamoDB
- Final entity list

## Timeline (revised, D007)

Build vertically: one thin slice end to end before widening. Applications go out with the slice;
widening continues before onsites.

| When | Build | Read |
|---|---|---|
| Sep 27–28 | Repo + decision log; connectivity spike (Kafka, Mongo, `from_avro` + SR on serverless); start Docker Hub poller; ~~measure GH Archive download cost~~ (done: ~400 MB/day) | Kafka: The Definitive Guide ch 1–4; SR compatibility modes |
| Sep 29–30 | `entities.yaml`, Avro schemas, GH Archive backfill producer (1d, then 90d), PyPI producer | Delivery semantics; schema evolution |
| Oct 1–2 | Bronze streaming + metrics table; drills: kill mid-batch, duplicate replay | Spark SS guide (fault tolerance, checkpoints, foreachBatch); Delta Lake paper |
| Oct 3–4 | Silver SDP: normalize, exact-alias resolution, quarantine, dedup, GH daily agg, expectations; drill: late event | Streaming 101/102; SS watermarks and state; SDP docs |
| Oct 5–6 | dbt gold: dims, fact, rolling windows, attention vs adoption, `signal_evidence`, tests | Kimball DW Toolkit ch 1–3; dbt incremental + snapshots |
| Oct 7 | One Databricks Job end to end; README with one finding + chart; firehose at 2 speeds. **Apply.** | Rehearse the project walkthrough |
| After Oct 7 | HN + context-rule resolution; Docker deltas; SCD2 snapshot on `dim_entity`; Mongo sync + FastAPI; schema-change drill | DDIA stream processing; mock design defenses |

Cut line if we slip: HN and Mongo/FastAPI move later first. Drills, `signal_evidence` and the README
are never cut.
