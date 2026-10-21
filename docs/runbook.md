# Runbook

Failure drills and operational notes. Each drill records expected vs observed behaviour.

## Platform verifications (day-1 spike)

Run by Manav on Databricks Free Edition, 2026-10-01/02. Spike notebook export pending
(`databricks/spike/connectivity.py`).

| Check | Expected | Observed |
|---|---|---|
| Serverless → Confluent Kafka, decode with `from_avro` + Schema Registry | Decoded rows | ✅ Works |
| Serverless → MongoDB Atlas (`pymongo`) write | One document written | ✅ Works (Atlas network access `0.0.0.0/0`) |
| `Trigger.AvailableNow` with checkpoint in a UC volume | Stream drains and stops | ✅ Works |
| `CREATE CATALOG radar` on Free Edition | Allowed | ✅ Allowed |
| Serverless egress to hub.docker.com, pypistats.org, hn.algolia.com | HTTP 200 | ✅ All 200 |

## Operational notes

- **Resetting a bronze checkpoint requires a new `txnAppId`.** Bronze writes use
  `txnVersion = batchId` (D002). A fresh checkpoint restarts `batchId` at 0, and Delta would skip
  every write as already committed.
- **Docker Hub rate-limits bursts.** Back-to-back unauthenticated requests started failing
  during setup. The poller paces 1 request/s and retries 429/5xx with exponential backoff,
  honouring `Retry-After`.
- **pypistats rate-limits hard.** On 2026-10-02 it returned 429 at 1 req/s and intermittently
  even at 1 req per 6 s. The PyPI producer needs slow pacing and backoff from the start.
- **Kafka `SASL authentication error` while Schema Registry calls succeed means a bad cluster
  key.** Seen 2026-10-21. The SR key is separate and was fine. The `.env` values were clean, but
  the cluster rejected the key ID + secret pair. Fixed by creating a new key from inside the
  cluster (Cluster → API Keys → Add key → My account) and replacing the ID and secret together.
  Remember to update `kafka-api-key` and `kafka-api-secret` in secret scope `radar` as well.
- **Docker Hub history starts 2026-10-21**, with the first live poll (offsets 0–12).
- **Re-running the Docker poll on the same UTC day is safe.** It produces snapshots with the same
  `source_event_id` (D004), which silver drops as duplicates. The first poll of the day wins.

## Drills

_None yet. Drills are added as each layer lands._
