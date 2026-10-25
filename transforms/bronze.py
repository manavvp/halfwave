"""Bronze: a raw, faithful copy of each Kafka topic plus where every message came from.

Bronze never decodes (D012). Every bronze table therefore has the same schema, whatever the
topic carries, and one DDL serves them all. Decoding with Schema Registry happens in silver.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

CATALOG = "radar"
SCHEMA = "bronze"
CHECKPOINT_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"
PROGRESS_TABLE = f"{CATALOG}.{SCHEMA}.stream_progress"

_TOPIC_RE = re.compile(r"^radar\.([a-z0-9]+)\.([a-z0-9]+)\.(v[0-9]+)$")

# (name, Spark type, comment). The single source of truth for the bronze row shape: the DDL is
# built from it, and a test checks that kafka_to_bronze() produces exactly these columns.
BRONZE_COLUMNS: list[tuple[str, str, str]] = [
    ("kafka_topic", "string", "Topic the message was read from."),
    ("kafka_partition", "int", "Partition within the topic. Offsets are per partition."),
    (
        "kafka_offset",
        "bigint",
        "Offset within the partition. (topic, partition, offset) is unique.",
    ),
    ("kafka_timestamp", "timestamp", "Message timestamp from Kafka (producer create time)."),
    ("kafka_key", "string", "Message key as UTF-8, e.g. vllm/vllm-openai."),
    ("value_raw", "binary", "Exact message bytes: Confluent 5-byte header + Avro body."),
    ("schema_id", "int", "Schema Registry id from bytes 1-4; null if not Confluent-framed."),
    ("ingest_ts", "timestamp", "When bronze processed the micro-batch."),
    ("bronze_batch_id", "bigint", "Micro-batch that wrote the row (= Delta txnVersion)."),
]
_NOT_NULL = {"kafka_topic", "kafka_partition", "kafka_offset", "ingest_ts", "bronze_batch_id"}

# One row per streaming progress report (one per micro-batch).
PROGRESS_COLUMNS: list[tuple[str, str]] = [
    ("query_name", "string"),
    ("query_id", "string"),
    ("run_id", "string"),
    ("batch_id", "bigint"),
    ("progress_ts", "timestamp"),
    ("num_input_rows", "bigint"),
    ("input_rows_per_sec", "double"),
    ("processed_rows_per_sec", "double"),
    ("trigger_execution_ms", "bigint"),
    ("add_batch_ms", "bigint"),
    ("start_offset", "string"),
    ("end_offset", "string"),
    ("raw_progress", "string"),
]


def bronze_table_name(topic: str) -> str:
    """radar.dockerhub.snapshots.v1 -> dockerhub_snapshots_v1 (one table per topic)."""
    match = _TOPIC_RE.match(topic)
    if not match:
        raise ValueError(f"topic {topic!r} doesn't follow radar.<source>.<type>.v<n>")
    return "_".join(match.groups())


def bronze_table_ddl(full_table_name: str) -> str:
    columns = ",\n  ".join(
        f"{name} {spark_type.upper()}{' NOT NULL' if name in _NOT_NULL else ''} COMMENT '{comment}'"
        for name, spark_type, comment in BRONZE_COLUMNS
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {full_table_name} (\n  {columns}\n)\n"
        "COMMENT 'Raw copy of a Kafka topic (D012). Unique on (kafka_topic, kafka_partition, "
        "kafka_offset). Contains source duplicates by design; silver dedups.'"
    )


def progress_table_ddl() -> str:
    columns = ",\n  ".join(f"{name} {t.upper()}" for name, t in PROGRESS_COLUMNS)
    return f"CREATE TABLE IF NOT EXISTS {PROGRESS_TABLE} (\n  {columns}\n)"


def _schema_id(value: Column) -> Column:
    """Bytes 1-4 of the Confluent wire format, as an int. Null unless byte 0 is the magic 0x00."""
    framed = (F.length(value) >= 5) & (F.hex(F.substring(value, 1, 1)) == "00")
    return F.when(framed, F.conv(F.hex(F.substring(value, 2, 4)), 16, 10).cast("int"))


def kafka_to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Map Spark's Kafka source columns to the bronze row (everything except bronze_batch_id).

    No business logic and no decoding: the value is kept byte for byte.
    """
    return kafka_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").alias("value_raw"),
        _schema_id(F.col("value")).alias("schema_id"),
        F.current_timestamp().alias("ingest_ts"),
    )


def read_query_id(checkpoint_metadata: str) -> str:
    """The query id from a checkpoint's `metadata` file, e.g. '{"id":"8a3c..."}'."""
    first_line = checkpoint_metadata.strip().splitlines()[0]
    return str(json.loads(first_line)["id"])


def txn_app_id(table_name: str, query_id: str) -> str:
    """Delta txnAppId for one checkpoint's writes into one table (D013).

    Tied to the checkpoint's query id, so a reset checkpoint gets a fresh id: the worst case is
    visible duplicates, never batches silently skipped as "already committed".
    """
    return f"{table_name}:{query_id}"


def _offsets_json(value: object) -> str | None:
    if value is None:
        return None
    return value if isinstance(value, str) else json.dumps(value, sort_keys=True)


def progress_to_rows(progress: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    """Flatten StreamingQueryProgress dicts into PROGRESS_COLUMNS-ordered tuples."""
    rows = []
    for p in progress:
        durations = p.get("durationMs") or {}
        sources = p.get("sources") or [{}]
        source = sources[0]
        rows.append(
            (
                p.get("name"),
                p.get("id"),
                p.get("runId"),
                p.get("batchId"),
                datetime.fromisoformat(p["timestamp"]) if p.get("timestamp") else None,
                p.get("numInputRows"),
                _as_float(p.get("inputRowsPerSecond")),
                _as_float(p.get("processedRowsPerSecond")),
                durations.get("triggerExecution"),
                durations.get("addBatch"),
                _offsets_json(source.get("startOffset")),
                _offsets_json(source.get("endOffset")),
                json.dumps(p, sort_keys=True),
            )
        )
    return rows


def _as_float(value: object) -> float | None:
    # Spark reports NaN as the string "NaN" in JSON progress when a duration is zero.
    if value is None or value == "NaN":
        return None
    return float(value)
