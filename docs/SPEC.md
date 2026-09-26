# V1 Spec: AI-Infra Adoption Radar

This spec is frozen. Anything not listed here is Phase 2 by default (log it in `docs/phase2.md`).

## Objective

Track ~25 AI-infrastructure projects and measure whether rising **attention** translates into
observable **adoption**, with every signal traceable to source evidence. There is no composite
score; the methodology is explicit and lives in dbt.

## Entities

- ~25 projects defined in `config/entities.yaml`. Each entry has: id, type, aliases, GitHub repo,
  PyPI package, Docker Hub image.
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
  - **backfill**: registry repos only, over ~90 days. Used to build the signal history.

## Bronze: hand-written Structured Streaming

- Kafka → one Delta table per topic, storing the raw payload plus Kafka metadata.
- `foreachBatch` + MERGE on `(source, source_event_id)` for idempotent writes.
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
- `dropDuplicatesWithinWatermark` with event-time watermarks.
- Daily buckets per entity per signal. This is the only streaming aggregation.
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

- Hosted Kafka provider (pick by free tier at signup)
- MongoDB (default) vs DynamoDB
- Final entity list

## Timeline

| When | What |
|---|---|
| Days 1–2 | Concepts + decision log; connectivity spike |
| Days 3–10 | Build layer by layer, with failure drills as each layer lands |
| Days 11–12 | Own-the-project review |
| ~Oct 7 | Applications go out |
