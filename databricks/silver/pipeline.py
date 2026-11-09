"""Silver pipeline (Lakeflow SDP): bronze tables -> events, quarantine, github_events_daily.

A thin wrapper. Decoding with from_avro + Schema Registry is Databricks-only, so it lives here;
everything after decoding is in transforms/silver.py, tested locally. The pipeline's root_path
is the repo root, which SDP puts on sys.path, so `transforms` imports directly (verified
2026-11-09).

One flow per bronze table into `events` and one into `quarantine`, so each source keeps its own
watermark and dedup state (D020), and a new source (HN) is a new flow, not a full refresh.

Pipeline configuration (settings: databricks/silver/pipeline_settings.json):
    radar.registry_table   table to resolve against (default radar.silver.entity_registry)
    radar.bronze_suffix    read bronze tables with this suffix; drills use "_drill"
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from transforms import silver

SECRET_SCOPE = "radar"
# dbutils works in pipeline source; {{secrets/...}} references in the pipeline configuration
# did not resolve (spike, 2026-11-09).
SR_URL = dbutils.secrets.get(SECRET_SCOPE, "schema-registry-url")
SR_OPTIONS = {
    "confluent.schema.registry.basic.auth.credentials.source": "USER_INFO",
    "confluent.schema.registry.basic.auth.user.info": (
        f"{dbutils.secrets.get(SECRET_SCOPE, 'schema-registry-api-key')}:"
        f"{dbutils.secrets.get(SECRET_SCOPE, 'schema-registry-api-secret')}"
    ),
    # A bad message becomes a struct of nulls instead of failing the pipeline (D021).
    "mode": "PERMISSIVE",
}
REGISTRY_TABLE = spark.conf.get("radar.registry_table", silver.REGISTRY_TABLE)
BRONZE_SUFFIX = spark.conf.get("radar.bronze_suffix", "")


def decoded(src: silver.Source) -> DataFrame:
    """New bronze rows for one topic, with the Avro payload decoded into `rec`."""
    rec = from_avro(
        F.col("value_raw"),
        options=SR_OPTIONS,
        subject=f"{src.topic}-value",
        schemaRegistryAddress=SR_URL,
    )
    return spark.readStream.table(src.bronze_table + BRONZE_SUFFIX).select(
        rec.alias("rec"), "value_raw", "kafka_topic", "kafka_partition", "kafka_offset",
        "ingest_ts",
    )


def registry() -> DataFrame:
    return spark.read.table(REGISTRY_TABLE)


dp.create_streaming_table(
    name="events",
    comment="Every fact once, in the common event model, resolved to an entity (D020-D024).",
    schema=silver.EVENTS_SCHEMA,
    expect_all=silver.EVENT_EXPECTATIONS,
)
dp.create_streaming_table(
    name="quarantine",
    comment="Rows silver couldn't use: undecodable or unresolved, with a pointer to bronze (D021).",
    schema=silver.QUARANTINE_SCHEMA,
)


def add_flows(src: silver.Source) -> None:
    @dp.append_flow(target="events", name=f"events_{src.name}")
    def events() -> DataFrame:
        return silver.events_flow(decoded(src), registry(), src)

    @dp.append_flow(target="quarantine", name=f"quarantine_{src.name}")
    def quarantine() -> DataFrame:
        return silver.quarantine_flow(decoded(src), registry(), src)


for _src in silver.SOURCES:
    add_flows(_src)


@dp.table(
    name="github_events_daily",
    comment="GitHub events per entity, UTC day and type. A day is written once, when final (D023).",
)
def github_events_daily() -> DataFrame:
    return silver.github_daily_counts(spark.readStream.table("events"))
