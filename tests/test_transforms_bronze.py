import json
from datetime import UTC, datetime

import pytest
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transforms import bronze

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("timestampType", IntegerType()),
    ]
)
TS = datetime(2026, 10, 21, 13, 6, 2, tzinfo=UTC)
FRAMED = bytes([0]) + (100007).to_bytes(4, "big") + b"\x02avro-body"


def kafka_row(value, key=b"vllm/vllm-openai", offset=0):
    return (key, value, "radar.dockerhub.snapshots.v1", 0, offset, TS, 0)


# --- naming and DDL (no Spark needed) ---------------------------------------------------------


def test_table_name_from_topic():
    assert bronze.bronze_table_name("radar.dockerhub.snapshots.v1") == "dockerhub_snapshots_v1"
    assert bronze.bronze_table_name("radar.gharchive.events.v1") == "gharchive_events_v1"


@pytest.mark.parametrize("topic", ["dockerhub.snapshots.v1", "radar.dockerhub.v1", "radar.A.b.v1"])
def test_table_name_rejects_unconventional_topics(topic):
    with pytest.raises(ValueError, match="radar.<source>.<type>.v<n>"):
        bronze.bronze_table_name(topic)


def test_ddl_declares_every_column_and_keys_not_null():
    ddl = bronze.bronze_table_ddl("radar.bronze.t")
    for name, spark_type, _ in bronze.BRONZE_COLUMNS:
        assert f"{name} {spark_type.upper()}" in ddl
    assert "kafka_offset BIGINT NOT NULL" in ddl
    assert "kafka_key STRING COMMENT" in ddl  # keys may legally be null in Kafka


# --- txnAppId ---------------------------------------------------------------------------------


def test_query_id_read_from_checkpoint_metadata():
    assert bronze.read_query_id('{"id":"8a3c-42"}\n') == "8a3c-42"


def test_txn_app_id_changes_with_the_checkpoint():
    first = bronze.txn_app_id("dockerhub_snapshots_v1", "query-a")
    after_reset = bronze.txn_app_id("dockerhub_snapshots_v1", "query-b")
    assert first != after_reset
    assert first == "dockerhub_snapshots_v1:query-a"


# --- kafka_to_bronze (local Spark) ------------------------------------------------------------


def test_bronze_columns_match_the_declared_shape(spark):
    out = bronze.kafka_to_bronze(spark.createDataFrame([kafka_row(FRAMED)], KAFKA_SCHEMA))
    declared = [(n, t) for n, t, _ in bronze.BRONZE_COLUMNS if n != "bronze_batch_id"]
    actual = [(f.name, f.dataType.simpleString()) for f in out.schema.fields]
    assert actual == declared


def test_value_is_kept_byte_for_byte_and_schema_id_parsed(spark):
    [row] = bronze.kafka_to_bronze(
        spark.createDataFrame([kafka_row(FRAMED, offset=26)], KAFKA_SCHEMA)
    ).collect()
    assert bytes(row.value_raw) == FRAMED
    assert row.schema_id == 100007
    assert row.kafka_key == "vllm/vllm-openai"
    assert (row.kafka_topic, row.kafka_partition, row.kafka_offset) == (
        "radar.dockerhub.snapshots.v1",
        0,
        26,
    )
    assert row.ingest_ts is not None


@pytest.mark.parametrize(
    "value",
    [
        b'{"not": "avro"}',  # plain JSON written to the topic by mistake
        b"\x00\x01",  # too short to hold a schema id
        None,  # tombstone
    ],
)
def test_unframed_values_get_null_schema_id_but_are_still_kept(spark, value):
    [row] = bronze.kafka_to_bronze(
        spark.createDataFrame([kafka_row(value)], KAFKA_SCHEMA)
    ).collect()
    assert row.schema_id is None
    assert (bytes(row.value_raw) if row.value_raw is not None else None) == value


def test_null_key_is_allowed(spark):
    [row] = bronze.kafka_to_bronze(
        spark.createDataFrame([kafka_row(FRAMED, key=None)], KAFKA_SCHEMA)
    ).collect()
    assert row.kafka_key is None


# --- progress_to_rows -------------------------------------------------------------------------

PROGRESS = {
    "id": "8a3c-42",
    "runId": "run-1",
    "name": "bronze_dockerhub_snapshots_v1",
    "timestamp": "2026-10-25T06:00:03.123Z",
    "batchId": 0,
    "numInputRows": 39,
    "inputRowsPerSecond": "NaN",
    "processedRowsPerSecond": 12.5,
    "durationMs": {"addBatch": 2900, "triggerExecution": 3120},
    "sources": [
        {
            "description": "KafkaV2[Subscribe[radar.dockerhub.snapshots.v1]]",
            "startOffset": None,
            "endOffset": {"radar.dockerhub.snapshots.v1": {"0": 39}},
        }
    ],
}


def test_progress_is_flattened_in_column_order():
    [row] = bronze.progress_to_rows([PROGRESS])
    record = dict(zip([c for c, _ in bronze.PROGRESS_COLUMNS], row, strict=True))
    assert record["batch_id"] == 0
    assert record["num_input_rows"] == 39
    assert record["input_rows_per_sec"] is None  # Spark's "NaN"
    assert record["processed_rows_per_sec"] == 12.5
    assert record["trigger_execution_ms"] == 3120
    assert record["start_offset"] is None
    assert json.loads(record["end_offset"]) == {"radar.dockerhub.snapshots.v1": {"0": 39}}
    assert record["progress_ts"] == datetime(2026, 10, 25, 6, 0, 3, 123000, tzinfo=UTC)
    assert json.loads(record["raw_progress"])["runId"] == "run-1"


def test_progress_rows_load_into_the_declared_schema(spark):
    schema = ", ".join(f"{n} {t}" for n, t in bronze.PROGRESS_COLUMNS)
    df = spark.createDataFrame(bronze.progress_to_rows([PROGRESS]), schema)
    assert df.count() == 1


def test_run_with_no_batches_gives_no_rows():
    assert bronze.progress_to_rows([]) == []
