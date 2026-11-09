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
| Silver spike (2026-11-09, Spark 4.1 pipeline): `import transforms` with `root_path` = repo root | Imports | ✅ Works, no `sys.path` code |
| Same: secrets in pipeline source | Readable | ✅ `dbutils.secrets` works. ❌ `{{secrets/...}}` in pipeline configuration didn't resolve |
| Same: `from_avro` + SR inside a pipeline | Decoded rows | ✅ 247/247 real Docker Hub rows |
| Same: PERMISSIVE on truncated bytes, unknown schema id, unframed bytes | Null record | ⚠️ No crash, but a **struct of nulls**, not null. Decode failure = `rec.source_event_id IS NULL` (D021) |
| Same: watermark + `dropDuplicatesWithinWatermark` in a streaming table | Dedup | ✅ 247 → 234 (13 same-day re-polls) |

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
- **PyPI downloads drop sharply on 2026-08-25 for almost every package.** That's PyPI's
  counting change (2026-08-24), not adoption. Never compare across it (D019).
- **A PyPI package logging `0 days`** means pypistats hasn't published those days yet. The
  3-day lookback picks them up on the next run.
- **Kafka `SASL authentication error` while Schema Registry calls succeed means a bad cluster
  key.** Seen 2026-10-21. The SR key is separate and was fine. The `.env` values were clean, but
  the cluster rejected the key ID + secret pair. Fixed by creating a new key from inside the
  cluster (Cluster → API Keys → Add key → My account) and replacing the ID and secret together.
  Remember to update `kafka-api-key` and `kafka-api-secret` in secret scope `radar` as well.
- **Docker Hub history starts 2026-10-21**, with the first live poll (offsets 0–12).
- **Re-running the Docker poll on the same UTC day is safe.** It produces snapshots with the same
  `source_event_id` (D004), which silver drops as duplicates. The first poll of the day wins.
- **Free Edition stops firing schedules after a few days without a human login.** Found
  2026-11-08. `radar-daily` ran on Nov 3–6 with no browser login since Nov 2, then had no run at
  all on Nov 7 and 8. The audit log showed no `runTriggered` event and no edit, pause or skip, and
  the schedule stayed unpaused. After a login on Nov 8, the Nov 9 run fired. Job runs (even as
  Manav) don't count as activity. Databricks doesn't document this. **Rule: open the workspace
  at least every 3 days.** Lost for good: the Docker Hub and GitHub snapshots for 2026-11-07.
  Nov 8 was saved by running both producers locally (`github --date 2026-11-07 --days 2`).
  Diagnose with `databricks jobs list-runs --job-id <id>`, then `system.access.audit` where
  `service_name = 'jobs' AND action_name = 'runTriggered'`.
- **Topic retention wasn't unlimited** (found 2026-11-08), despite D009, D016 and D019. The
  GitHub and PyPI topics had 7 days and Docker Hub had 30. The GitHub and PyPI backfills had
  already expired from Kafka, and only Bronze held them (counts checked 2026-11-08). Fixed
  2026-11-09: `retention.ms=-1` on all four topics (AdminClient `incremental_alter_configs`,
  verified with `describe_configs`). Offsets that had already expired stay gone, so a Bronze
  checkpoint reset still can't rebuild GitHub or PyPI history before those offsets from Kafka.
  For that history, Bronze is the only copy.

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

### Silver: `drills/silver_drills.py` (scratch pipeline `radar-silver-drill`, schema `silver_drill`)

Run locally: `set -a; source .env; set +a`, then `python drills/silver_drills.py`. It writes
synthetic forks into empty `radar.bronze.*_drill` tables, encoded by the producers' own Avro
serializer (byte-identical to real bronze rows), runs one pipeline update per step and checks
silver. It drops the drill bronze tables at the end (`--keep` to inspect them). About 12 minutes,
five pipeline updates. Result on 2026-11-09 (against the dev copy and dev registry):
**12/12 checks passed.**

| # | Drill | Expected | Observed |
|---|---|---|---|
| 1 | Duplicate replay: run 2 re-fetches fork B (the 6 h overlap) plus C | Bronze keeps both copies of B (4 rows); silver has A, B, C once each | ✅ Bronze 4 rows; silver `ABC` |
| 2 | Late event: run 3 brings D dated Nov 6 15:00 (watermark is Nov 7 22:00) plus E at Nov 8 23:00 | D dropped as late, E kept. Nov 7 closes with 2; no Nov 6 row | ✅ Silver `ABCE`; daily `[2026-11-07, 2]` only |
| 3 | Poison pill: valid header + corrupt Avro body, plus a fork of a repo not in the registry | Update completes; silver unchanged; quarantine has `decode_failed` (offset 6) and `unresolved_entity` (offset 7) | ✅ Exactly that |
