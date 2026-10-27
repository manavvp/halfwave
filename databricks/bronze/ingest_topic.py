# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: one Kafka topic → one Delta table (raw copy)
# MAGIC
# MAGIC Reads everything that arrived since the last run (`AvailableNow`), appends it exactly once,
# MAGIC records the run's progress, and stops. Parameterized by `topic` (D014).
# MAGIC
# MAGIC | Concern | Mechanism |
# MAGIC |---|---|
# MAGIC | Where did I stop? | Checkpoint in `/Volumes/radar/bronze/checkpoints/<table>` |
# MAGIC | Batch replayed after a crash | `txnAppId` + `txnVersion = batchId`: a no-op (D002) |
# MAGIC | Checkpoint reset | `txnAppId` has the query id: duplicates, never loss (D013) |
# MAGIC | Undecodable message | Not bronze's problem: bronze never decodes (D012) |

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("topic", "radar.dockerhub.snapshots.v1")
dbutils.widgets.text("table_suffix", "")  # drills write to <table>_drill
dbutils.widgets.text("max_offsets_per_trigger", "10000")
dbutils.widgets.text("drill_fail_after_batch", "")  # drills only: crash after this batch's write

import json
import os

from transforms import bronze

TOPIC = dbutils.widgets.get("topic").strip()
TABLE = bronze.bronze_table_name(TOPIC) + dbutils.widgets.get("table_suffix").strip()
FULL_TABLE = f"{bronze.CATALOG}.{bronze.SCHEMA}.{TABLE}"
CHECKPOINT = f"{bronze.CHECKPOINT_VOLUME}/{TABLE}"
QUERY_NAME = f"bronze_{TABLE}"
MAX_OFFSETS = int(dbutils.widgets.get("max_offsets_per_trigger"))
_fail = dbutils.widgets.get("drill_fail_after_batch").strip()
DRILL_FAIL_AFTER_BATCH = int(_fail) if _fail else None

print(f"topic       {TOPIC}\ntable       {FULL_TABLE}\ncheckpoint  {CHECKPOINT}")
if DRILL_FAIL_AFTER_BATCH is not None:
    print(f"DRILL MODE: will crash after writing batch {DRILL_FAIL_AFTER_BATCH}")

# COMMAND ----------

# Idempotent setup: safe to run every time.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {bronze.CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze.CATALOG}.{bronze.SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {bronze.CATALOG}.{bronze.SCHEMA}.checkpoints")
spark.sql(bronze.bronze_table_ddl(FULL_TABLE))
spark.sql(bronze.progress_table_ddl())

# COMMAND ----------

SECRET_SCOPE = "radar"
api_key = dbutils.secrets.get(SECRET_SCOPE, "kafka-api-key")
api_secret = dbutils.secrets.get(SECRET_SCOPE, "kafka-api-secret")
jaas = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{api_key}" password="{api_secret}";'
)

kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", dbutils.secrets.get(SECRET_SCOPE, "kafka-bootstrap-servers"))
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas)
    .option("subscribe", TOPIC)
    # Only used the very first time: after that the checkpoint says where to resume.
    .option("startingOffsets", "earliest")
    # If Kafka deleted messages we never read (retention), fail loudly instead of skipping.
    .option("failOnDataLoss", "true")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    .load()
)
bronze_stream = bronze.kafka_to_bronze(kafka_stream)

# COMMAND ----------


def write_batch(batch_df, batch_id: int) -> None:  # noqa: ANN001
    """Append one micro-batch exactly once.

    Runs in a separate server-side Python process, not this notebook's, so it can't import repo
    modules: it uses only the stdlib, PySpark and the constants captured from the cells above.
    The txnAppId logic mirrors transforms.bronze.read_query_id / txn_app_id (unit-tested there).
    """
    import json
    import os

    from pyspark.sql import functions as F

    # The query id is the checkpoint's identity (written by Spark when the checkpoint is created).
    try:
        with open(os.path.join(CHECKPOINT, "metadata")) as f:
            query_id = json.loads(f.readline())["id"]
    except OSError:
        active = [q for q in batch_df.sparkSession.streams.active if q.name == QUERY_NAME]
        if not active:
            # Never fall back to a fixed id: that is the silent-data-loss mode (D013).
            raise RuntimeError(f"can't determine query id for {QUERY_NAME}") from None
        query_id = str(active[0].id)

    (
        batch_df.withColumn("bronze_batch_id", F.lit(batch_id).cast("bigint"))
        .write.format("delta")
        .mode("append")
        .option("txnAppId", f"{TABLE}:{query_id}")
        .option("txnVersion", batch_id)
        .saveAsTable(FULL_TABLE)
    )

    if DRILL_FAIL_AFTER_BATCH is not None and batch_id == DRILL_FAIL_AFTER_BATCH:
        raise RuntimeError(
            f"DRILL: simulated crash after writing batch {batch_id}, before the checkpoint "
            "recorded it as committed"
        )


# COMMAND ----------

rows_before = spark.table(FULL_TABLE).count()

query = (
    bronze_stream.writeStream.queryName(QUERY_NAME)
    .foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)

try:
    query.awaitTermination()
finally:
    # Record whatever batches completed, even if the run failed.
    # PySpark 4's StreamingQueryProgress subclasses dict but holds uuid.UUID values; its .json is
    # Spark's own serialization, so prefer it. Plain dicts (older PySpark) have no .json.
    progress = [json.loads(p.json) if hasattr(p, "json") else p for p in query.recentProgress]
    progress_rows = bronze.progress_to_rows(progress)
    if progress_rows:
        schema = ", ".join(f"{n} {t}" for n, t in bronze.PROGRESS_COLUMNS)
        spark.createDataFrame(progress_rows, schema).write.mode("append").saveAsTable(
            bronze.PROGRESS_TABLE
        )

# COMMAND ----------

# What happened, in terms of the checkpoint and the table.
rows_after = spark.table(FULL_TABLE).count()
with open(os.path.join(CHECKPOINT, "metadata")) as f:
    query_id = bronze.read_query_id(f.read())


def checkpoint_batches(sub: str) -> list[int]:
    path = os.path.join(CHECKPOINT, sub)
    return sorted(int(n) for n in os.listdir(path) if n.isdigit()) if os.path.isdir(path) else []


offsets_log, commits_log = checkpoint_batches("offsets"), checkpoint_batches("commits")
print(f"txnAppId        {bronze.txn_app_id(TABLE, query_id)}")
print(f"batches run     {[p.get('batchId') for p in progress]}")
print(f"rows            {rows_before} -> {rows_after}  (+{rows_after - rows_before})")
print(f"checkpoint      offsets/ {offsets_log[-5:]}   commits/ {commits_log[-5:]}")
if offsets_log:
    with open(os.path.join(CHECKPOINT, "offsets", str(offsets_log[-1]))) as f:
        print(f"last range      {f.read().strip().splitlines()[-1]}")
