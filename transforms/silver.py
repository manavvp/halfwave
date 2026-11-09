"""Silver: decoded, deduplicated, entity-resolved events in one shape (the common event model).

The pipeline decodes bronze's `value_raw` with `from_avro` + Schema Registry (Databricks-only,
so it stays in the thin wrapper, databricks/silver/pipeline.py). Everything after decoding is
here and tested locally:

    decoded bronze row ─┬─ decode failed ──────────────────────────────► quarantine
                        └─ keyed ─► watermark + dedup ─► resolve ─┬─ no entity ─► quarantine
                                                                  └─► events ─► github_events_daily

`decoded` DataFrames carry the bronze columns plus `rec`, the decoded record struct.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StructType

from transforms.bronze import bronze_table_name

CATALOG = "radar"
SCHEMA = "silver"
REGISTRY_TABLE = f"{CATALOG}.{SCHEMA}.entity_registry"

ATTENTION, ACTIVITY, ADOPTION = "attention", "activity", "adoption"
# What each event type measures (D024). Attention: people looking. Activity: people building.
# Adoption: people using.
SIGNAL_CLASS: dict[str, str] = {
    "fork": ATTENTION,
    "stars_total": ATTENTION,
    "forks_total": ATTENTION,
    "release": ACTIVITY,
    "pr_opened": ACTIVITY,
    "pr_merged": ACTIVITY,
    "issue_opened": ACTIVITY,
    "downloads": ADOPTION,
    "pulls_total": ADOPTION,
}
GITHUB_EVENT_TYPES = ("fork", "release", "pr_opened", "pr_merged", "issue_opened")
# The daily GitHub aggregation's watermark: the same delay as the events flow (D020, D023).
GITHUB_DAILY_WATERMARK = "12 hours"


@dataclass(frozen=True)
class Source:
    """One bronze table and how it becomes events."""

    name: str  # flow name suffix
    source: str  # value of the `source` column; dedup key part
    topic: str
    handle_field: str  # record field naming the thing tracked: repo, package or image
    registry_column: str  # entity_registry column holding the same handle
    event_time_field: str  # record field that is the event time (timestamp or UTC date)
    # Max out-of-orderness: the producer's re-read window plus a margin (D020).
    watermark: str
    # Snapshot-style records become one event per metric: (event_type, record field). None
    # means the record is itself one event, with its own event_type and url, and value 1.
    metrics: tuple[tuple[str, str], ...] | None = None
    url_prefix: str = ""  # with `metrics`: source_url = url_prefix + handle

    @property
    def bronze_table(self) -> str:
        return f"{CATALOG}.bronze.{bronze_table_name(self.topic)}"


SOURCES: tuple[Source, ...] = (
    Source(
        "github_events", "github", "radar.github.events.v1", "repo", "github_repo",
        "event_ts", "12 hours",
    ),
    Source(
        "github_snapshots", "github", "radar.github.snapshots.v1", "repo", "github_repo",
        "snapshot_date", "1 day",
        metrics=(("stars_total", "stars"), ("forks_total", "forks")),
        url_prefix="https://github.com/",
    ),
    Source(
        "pypi_downloads", "pypi", "radar.pypi.downloads.v1", "package", "pypi_package",
        "download_date", "3 days",
        metrics=(("downloads", "downloads"),),
        url_prefix="https://pypistats.org/packages/",
    ),
    Source(
        "dockerhub_snapshots", "dockerhub", "radar.dockerhub.snapshots.v1", "image",
        "docker_image", "snapshot_date", "1 day",
        metrics=(("pulls_total", "pull_count"),),
        url_prefix="https://hub.docker.com/r/",
    ),
)

# The common event model. Every flow into `events` produces exactly these columns.
EVENTS_SCHEMA = (
    "event_id STRING, source_event_id STRING, entity_id STRING, source STRING, "
    "event_type STRING, signal_class STRING, event_ts TIMESTAMP, ingest_ts TIMESTAMP, "
    "value BIGINT, source_url STRING, "
    "bronze_ref STRUCT<topic: STRING, partition: INT, offset: BIGINT>"
)
EVENT_COLUMNS = [
    "event_id", "source_event_id", "entity_id", "source", "event_type", "signal_class",
    "event_ts", "ingest_ts", "value", "source_url", "bronze_ref",
]

QUARANTINE_SCHEMA = (
    "source STRING, reason STRING, "
    "bronze_ref STRUCT<topic: STRING, partition: INT, offset: BIGINT>, "
    "payload STRING, quarantined_at TIMESTAMP"
)
DECODE_FAILED, UNRESOLVED = "decode_failed", "unresolved_entity"

# Data quality rules on `events`, recorded as warnings in the pipeline event log. Known failure
# modes (undecodable, unresolved) never reach `events`: they go to quarantine instead (D021).
EVENT_EXPECTATIONS: dict[str, str] = {
    "event_ts_present": "event_ts IS NOT NULL",
    "known_signal_class": "signal_class IN ('attention', 'activity', 'adoption')",
    "value_not_negative": "value >= 0",
    "not_in_the_future": "event_ts <= ingest_ts",
    "source_url_present": "source_url IS NOT NULL",
}

REGISTRY_SCHEMA = (
    "entity_id STRING, name STRING, type STRING, aliases ARRAY<STRING>, github_repo STRING, "
    "pypi_package STRING, docker_image STRING, adoption_proxy_notes STRING"
)


# --- decode outcome -----------------------------------------------------------------------------


def decoded_ok(rec: Column) -> Column:
    """True when `from_avro` actually decoded the row.

    In PERMISSIVE mode a failed decode is not a null record: it's a struct whose fields are all
    null (verified on serverless, 2026-11-09). `source_event_id` is required in every schema,
    so a null there can only mean the decode failed.
    """
    return rec["source_event_id"].isNotNull()


def _bronze_ref() -> Column:
    return F.struct(
        F.col("kafka_topic").alias("topic"),
        F.col("kafka_partition").alias("partition"),
        F.col("kafka_offset").alias("offset"),
    )


def _utc_midnight(day: Column) -> Column:
    """A DATE as the timestamp of its UTC midnight, whatever the session time zone."""
    return F.timestamp_seconds(F.unix_date(day).cast("bigint") * 86400)


def _utc_date(ts: Column) -> Column:
    """The UTC calendar day of a timestamp, whatever the session time zone."""
    return F.date_from_unix_date(F.floor(F.unix_seconds(ts) / 86400).cast("int"))


# --- events -------------------------------------------------------------------------------------


def keyed(decoded: DataFrame, src: Source) -> DataFrame:
    """Decoded rows with the dedup key, the handle and the event time pulled out of `rec`."""
    rec_type = decoded.schema["rec"].dataType
    assert isinstance(rec_type, StructType)
    event_time = F.col(f"rec.{src.event_time_field}")
    if isinstance(rec_type[src.event_time_field].dataType, DateType):
        event_time = _utc_midnight(event_time)
    return decoded.where(decoded_ok(F.col("rec"))).select(
        F.lit(src.source).alias("source"),
        F.col("rec.source_event_id").alias("source_event_id"),
        F.col(f"rec.{src.handle_field}").alias("handle"),
        event_time.alias("event_ts"),
        "rec",
        _bronze_ref().alias("bronze_ref"),
        "ingest_ts",
    )


def deduplicate(keyed_rows: DataFrame, src: Source) -> DataFrame:
    """The pipeline's only dedup (D003): one row per (source, source_event_id).

    State for a key is kept until the watermark passes it, so copies that arrive within the
    source's re-read window are dropped. Anything older than the watermark is dropped as late.
    """
    key = ["source", "source_event_id"]
    if not keyed_rows.isStreaming:  # batch (tests, ad-hoc reads): the whole input is one window
        return keyed_rows.dropDuplicates(key)
    return keyed_rows.withWatermark("event_ts", src.watermark).dropDuplicatesWithinWatermark(key)


def _registry_handles(registry: DataFrame, src: Source) -> DataFrame:
    return F.broadcast(
        registry.where(F.col(src.registry_column).isNotNull()).select(
            "entity_id", F.lower(F.col(src.registry_column)).alias("_handle")
        )
    )


def resolve(rows: DataFrame, registry: DataFrame, src: Source) -> DataFrame:
    """Attach entity_id by exact handle match, ignoring case (the registry rejects collisions).

    A stream-static join: the registry is re-read for every micro-batch, so a registry change
    applies to new rows only. Old rows need a full refresh (D006).
    """
    handles = _registry_handles(registry, src)
    return rows.join(handles, F.lower(rows["handle"]) == handles["_handle"], "inner").drop(
        "_handle"
    )


def unresolved(rows: DataFrame, registry: DataFrame, src: Source) -> DataFrame:
    handles = _registry_handles(registry, src)
    return rows.join(handles, F.lower(rows["handle"]) == handles["_handle"], "left_anti")


def _signal_class(event_type: Column) -> Column:
    mapping = F.create_map(*[F.lit(x) for pair in SIGNAL_CLASS.items() for x in pair])
    return mapping[event_type]


def to_events(resolved: DataFrame, src: Source) -> DataFrame:
    """Resolved rows in the common event model. Snapshot-style records explode to one row per
    metric; their value is the running total (dbt computes the daily change)."""
    if src.metrics is None:
        rows = resolved.select(
            "*",
            F.col("rec.event_type").alias("event_type"),
            F.lit(1).cast("bigint").alias("value"),
            F.col("rec.url").alias("source_url"),
            F.concat_ws(":", "source", "source_event_id").alias("event_id"),
        )
    else:
        metric = F.explode(
            F.array(
                *[
                    F.struct(
                        F.lit(event_type).alias("event_type"),
                        F.col(f"rec.{field}").cast("bigint").alias("value"),
                    )
                    for event_type, field in src.metrics
                ]
            )
        )
        rows = resolved.select("*", metric.alias("m")).select(
            "*",
            F.col("m.event_type").alias("event_type"),
            F.col("m.value").alias("value"),
            F.concat(F.lit(src.url_prefix), F.col("handle")).alias("source_url"),
            F.concat_ws(":", "source", "source_event_id", "m.event_type").alias("event_id"),
        )
    return rows.withColumn("signal_class", _signal_class(F.col("event_type"))).select(
        *EVENT_COLUMNS
    )


def events_flow(decoded: DataFrame, registry: DataFrame, src: Source) -> DataFrame:
    """Decoded bronze rows → deduplicated, resolved rows of the event model."""
    return to_events(resolve(deduplicate(keyed(decoded, src), src), registry, src), src)


def quarantine_flow(decoded: DataFrame, registry: DataFrame, src: Source) -> DataFrame:
    """Rows silver can't use, kept with the reason and a pointer back to bronze (D021).

    Undecodable rows keep their raw bytes (base64), and unresolved rows their decoded record (JSON).
    Not deduplicated: quarantine is evidence, and its volume is the alarm.
    """
    failed = decoded.where(~decoded_ok(F.col("rec"))).select(
        F.lit(src.source).alias("source"),
        F.lit(DECODE_FAILED).alias("reason"),
        _bronze_ref().alias("bronze_ref"),
        F.base64("value_raw").alias("payload"),
        F.current_timestamp().alias("quarantined_at"),
    )
    lost = unresolved(keyed(decoded, src), registry, src).select(
        "source",
        F.lit(UNRESOLVED).alias("reason"),
        "bronze_ref",
        F.to_json("rec").alias("payload"),
        F.current_timestamp().alias("quarantined_at"),
    )
    return failed.unionByName(lost)


# --- daily GitHub aggregation -------------------------------------------------------------------


def github_daily_counts(events: DataFrame, watermark: str = GITHUB_DAILY_WATERMARK) -> DataFrame:
    """Events per entity, UTC day and event type, for GitHub events (D005, D023).

    Append mode: a day's row is written once, when the watermark passes the end of that day,
    and never changes. An event for a day that's already closed is dropped as late.
    """
    github = events.where(
        (F.col("source") == "github") & F.col("event_type").isin(*GITHUB_EVENT_TYPES)
    )
    return (
        github.withWatermark("event_ts", watermark)
        .groupBy(F.window("event_ts", "1 day"), "entity_id", "event_type", "signal_class")
        .agg(F.count(F.lit(1)).alias("event_count"))
        .select(
            _utc_date(F.col("window.start")).alias("day"),
            "entity_id",
            "event_type",
            "signal_class",
            "event_count",
        )
    )


# --- registry -----------------------------------------------------------------------------------


class _Entity(Protocol):
    id: str
    name: str
    type: str
    aliases: tuple[str, ...]
    github_repo: str | None
    pypi_package: str | None
    docker_image: str | None
    adoption_proxy_notes: str


def registry_rows(entities: Iterable[_Entity]) -> list[tuple]:
    """producers.registry entities as rows of REGISTRY_SCHEMA (loaded by a job task, D022)."""
    return [
        (
            e.id,
            e.name,
            e.type,
            list(e.aliases),
            e.github_repo,
            e.pypi_package,
            e.docker_image,
            e.adoption_proxy_notes,
        )
        for e in entities
    ]
