# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze drills: replay, source duplicates, checkpoint reset
# MAGIC
# MAGIC Runs `databricks/bronze/ingest_topic` against a **scratch table** (`<table>_drill`, own
# MAGIC checkpoint), so the real bronze table is never touched. Each drill prints expected vs
# MAGIC observed; copy the summary at the bottom into `docs/runbook.md`.
# MAGIC
# MAGIC 1. **Crash after write, before commit.** Batch 0 is written, then the run crashes before the
# MAGIC    checkpoint records it. On rerun Spark replays batch 0; Delta must skip it (D002).
# MAGIC 2. **Source duplicates.** Same-day re-polls are separate messages; bronze keeps them all.
# MAGIC 3. **Checkpoint reset.** Delete the checkpoint, keep the table. A new query id means a new
# MAGIC    txnAppId, so everything is written again: duplicates, not silent loss (D013).

# COMMAND ----------

# MAGIC %run ../databricks/_bootstrap

# COMMAND ----------

import os

from pyspark.sql import functions as F

from transforms import bronze

TOPIC = "radar.dockerhub.snapshots.v1"
SUFFIX = "_drill"
TABLE = bronze.bronze_table_name(TOPIC) + SUFFIX
FULL_TABLE = f"{bronze.CATALOG}.{bronze.SCHEMA}.{TABLE}"
CHECKPOINT = f"{bronze.CHECKPOINT_VOLUME}/{TABLE}"
INGEST_NOTEBOOK = "../databricks/bronze/ingest_topic"

results: list[tuple[str, str, str, str]] = []  # (drill, check, expected, observed)


def check(drill: str, what: str, expected: object, observed: object) -> None:
    status = "PASS" if expected == observed else "FAIL"
    results.append((drill, what, str(expected), f"{observed} [{status}]"))
    print(f"[{status}] {drill}: {what}: expected {expected}, observed {observed}")


def run_ingest(fail_after_batch: int | None = None) -> bool:
    """Run the real ingest notebook against the drill table. Returns False if the run failed."""
    args = {
        "topic": TOPIC,
        "table_suffix": SUFFIX,
        "max_offsets_per_trigger": "10000",
        "drill_fail_after_batch": "" if fail_after_batch is None else str(fail_after_batch),
    }
    try:
        dbutils.notebook.run(INGEST_NOTEBOOK, 900, args)
        return True
    except Exception as exc:
        print(f"ingest run failed: {str(exc).splitlines()[0][:300]}")
        return False


def checkpoint_batches(sub: str) -> list[int]:
    path = os.path.join(CHECKPOINT, sub)
    return sorted(int(n) for n in os.listdir(path) if n.isdigit()) if os.path.isdir(path) else []


def current_app_id() -> str:
    with open(os.path.join(CHECKPOINT, "metadata")) as f:
        return bronze.txn_app_id(TABLE, bronze.read_query_id(f.read()))


def row_count() -> int:
    return spark.table(FULL_TABLE).count()


def reset_drill_target() -> None:
    spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE}")
    dbutils.fs.rm(CHECKPOINT, True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill 1: crash after the write, before the checkpoint commit

# COMMAND ----------

reset_drill_target()

crashed_run_ok = run_ingest(fail_after_batch=0)
check("1 replay", "crashed run reports failure", False, crashed_run_ok)
rows_after_crash = row_count()
print(f"rows after crash: {rows_after_crash}")
check("1 replay", "batch 0 rows were written before the crash", True, rows_after_crash > 0)
check("1 replay", "offsets/ has batch 0 (planned)", [0], checkpoint_batches("offsets"))
check("1 replay", "commits/ is empty (never confirmed)", [], checkpoint_batches("commits"))
app_id_before_replay = current_app_id()

replay_run_ok = run_ingest()
check("1 replay", "rerun succeeds", True, replay_run_ok)
check("1 replay", "row count unchanged (replay skipped by Delta)", rows_after_crash, row_count())
check("1 replay", "commits/ now has batch 0", [0], checkpoint_batches("commits")[:1])
check("1 replay", "same txnAppId (same checkpoint)", app_id_before_replay, current_app_id())

# COMMAND ----------

# Delta's own record: the replay should NOT have added a second WRITE commit for batch 0.
display(
    spark.sql(f"DESCRIBE HISTORY {FULL_TABLE}")
    .select("version", "timestamp", "operation", "operationMetrics.numOutputRows")
    .orderBy("version")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill 2: source duplicates are kept
# MAGIC Every same-day poll after the first is a duplicate *fact* at a new *offset*. Bronze must
# MAGIC keep one row per offset (no pipeline duplicates) and still show the repeats.

# COMMAND ----------

df = spark.table(FULL_TABLE)
total = df.count()
distinct_offsets = df.select("kafka_topic", "kafka_partition", "kafka_offset").distinct().count()
check("2 source dups", "one row per Kafka offset", total, distinct_offsets)

per_image_day = df.groupBy("kafka_key", F.to_date("kafka_timestamp").alias("day")).count()
repeated = per_image_day.filter("count > 1").count()
check("2 source dups", "some (image, day) groups have >1 row", True, repeated > 0)
display(per_image_day.groupBy("day").agg(F.max("count").alias("polls_that_day")).orderBy("day"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill 3: checkpoint reset → duplicates, never silent loss

# COMMAND ----------

rows_before_reset = row_count()
app_id_old = current_app_id()
dbutils.fs.rm(CHECKPOINT, True)  # keep the table, lose the checkpoint

reset_run_ok = run_ingest()
check("3 reset", "run after reset succeeds", True, reset_run_ok)
check("3 reset", "new txnAppId after reset", True, current_app_id() != app_id_old)
check("3 reset", "everything re-ingested (rows doubled)", 2 * rows_before_reset, row_count())
print(f"old txnAppId: {app_id_old}\nnew txnAppId: {current_app_id()}")
print(
    "With a fixed txnAppId the count would have stayed at "
    f"{rows_before_reset}: every batch skipped as 'already committed', new data included."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and cleanup

# COMMAND ----------

display(
    spark.createDataFrame(results, "drill string, check string, expected string, observed string")
)
failed = [r for r in results if r[3].endswith("[FAIL]")]
print(f"{len(results) - len(failed)}/{len(results)} checks passed")

reset_drill_target()  # drop the scratch table and its checkpoint
print("drill table and checkpoint removed")
