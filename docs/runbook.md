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

- **Resetting a bronze checkpoint re-ingests the whole topic.** `txnAppId` includes the
  checkpoint's query id (D013), so a new checkpoint writes everything again. That means
  duplicates, never silently skipped batches. For a clean rebuild, drop or truncate the table
  *and* delete the checkpoint together.
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

### Bronze: `drills/bronze_replay` (scratch table `dockerhub_snapshots_v1_drill`)

Run in Databricks: open the notebook, Connect → Serverless → Run all. It cleans up after itself.
Result on 2026-10-27: **13/13 checks passed.**

A first attempt scored 11/13. The two failing checks were "run succeeds" in drills 1 and 3. Both
runs had done their streaming work correctly, but the metrics step then raised `TypeError: Object
of type UUID is not JSON serializable`: PySpark 4's `StreamingQueryProgress` is a `dict` subclass
holding `uuid.UUID` values. Fixed by reading `progress.json` instead, with a regression test. The
lesson: the unit-test fixture was a plain dict with string ids, unlike the real object.

| # | Drill | Expected | Observed |
|---|---|---|---|
| 1 | Crash after the batch-0 write, before the checkpoint commit; then rerun | Crashed run fails. `offsets/` = [0], `commits/` = []. Rerun succeeds, row count unchanged, `commits/` = [0], same txnAppId, one WRITE commit in `DESCRIBE HISTORY` | ✅ 2026-10-27. Crash run failed. 91 rows written before the crash, `offsets/ [0]`, `commits/ []`. Rerun succeeded: 91 → 91, `commits/ [0]`, same txnAppId `dockerhub_snapshots_v1_drill:705b59cb-…` |
| 2 | Source duplicates (same-day re-polls) | One row per Kafka offset. Some (image, day) groups have more than one row | ✅ 2026-10-27. 91 rows = 91 distinct offsets (7 polls × 13 images). Repeated (image, day) groups present |
| 3 | Delete the checkpoint, keep the table; rerun | New txnAppId. Rows exactly double (duplicates, no silent loss) | ✅ 2026-10-27. New txnAppId. Rows 91 → 182 |
