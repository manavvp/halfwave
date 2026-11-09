import base64
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from producers.registry import load_registry
from transforms import silver

SCHEMAS = Path(__file__).resolve().parents[1] / "schemas"
INGEST = datetime(2026, 11, 9, 6, 5, tzinfo=UTC)
GH_EVENTS, GH_SNAPSHOTS, PYPI, DOCKER = silver.SOURCES


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=UTC)


# --- decoded bronze rows, typed like from_avro output -----------------------------------------


def _spark_type(avro) -> str:
    if isinstance(avro, list):  # ["null", X]
        [inner] = [t for t in avro if t != "null"]
        return _spark_type(inner)
    if isinstance(avro, dict):
        logical = avro.get("logicalType")
        if logical == "timestamp-millis":
            return "TIMESTAMP"
        if logical == "date":
            return "DATE"
        return _spark_type(avro["type"])
    return {"string": "STRING", "long": "BIGINT", "int": "INT", "boolean": "BOOLEAN"}[avro]


def rec_ddl(topic: str) -> str:
    fields = json.loads((SCHEMAS / f"{topic}.avsc").read_text())["fields"]
    return ", ".join(f"{f['name']}: {_spark_type(f['type'])}" for f in fields)


def decoded_df(spark, src, recs, offsets=None):
    """Rows as the pipeline sees them after from_avro: bronze columns + `rec`."""
    names = [f.split(":")[0] for f in rec_ddl(src.topic).split(", ")]
    schema = (
        f"rec STRUCT<{rec_ddl(src.topic)}>, value_raw BINARY, kafka_topic STRING, "
        "kafka_partition INT, kafka_offset BIGINT, ingest_ts TIMESTAMP"
    )
    rows = []
    for i, r in enumerate(recs):
        rec = None if r is None else tuple(r.get(n) for n in names)
        rows.append((rec, b"\x00raw", src.topic, 0, (offsets or range(100))[i], INGEST))
    df = spark.createDataFrame(rows, schema)
    # PERMISSIVE from_avro returns a struct of nulls, not a null struct, for a failed decode.
    return df.selectExpr(
        f"coalesce(rec, named_struct({', '.join(f'{chr(39)}{n}{chr(39)}, null' for n in names)})) "
        f"AS rec",
        "value_raw", "kafka_topic", "kafka_partition", "kafka_offset", "ingest_ts",
    )


def gh_event(oid=11, event_type="fork", at="2026-11-07T15:00:00", repo="vllm-project/vllm"):
    return {
        "source_event_id": f"{event_type}:599547518:{oid}",
        "event_type": event_type,
        "repo": repo,
        "repo_id": 599547518,
        "event_ts": ts(at),
        "actor": "ada",
        "object_id": oid,
        "url": f"https://github.com/ada/vllm/{oid}",
        "fetched_at": INGEST,
    }


def gh_snapshot(repo="vllm-project/vllm", day=date(2026, 11, 8), stars=96000, forks=16100):
    return {
        "source_event_id": f"{repo}:{day}",
        "repo": repo,
        "repo_id": 599547518,
        "stars": stars,
        "forks": forks,
        "watchers": 600,
        "open_issues": 2200,
        "open_pull_requests": 1100,
        "is_archived": False,
        "snapshot_date": day,
        "polled_at": INGEST,
    }


@pytest.fixture(scope="module")
def registry(spark):
    return spark.createDataFrame(silver.registry_rows(load_registry()), silver.REGISTRY_SCHEMA)


# --- config ---------------------------------------------------------------------------------------


def test_every_source_topic_has_a_schema_and_a_distinct_name():
    assert {s.topic for s in silver.SOURCES} == {p.stem for p in SCHEMAS.glob("*.avsc")}
    assert len({s.name for s in silver.SOURCES}) == len(silver.SOURCES)


def test_every_event_type_has_a_signal_class():
    metric_types = {t for s in silver.SOURCES for t, _ in (s.metrics or ())}
    assert set(silver.GITHUB_EVENT_TYPES) | metric_types == set(silver.SIGNAL_CLASS)


def test_registry_rows_cover_every_entity():
    rows = silver.registry_rows(load_registry())
    assert len(rows) == 25 and len({r[0] for r in rows}) == 25
    assert ("vllm", "vLLM") == rows[0][:2]


# --- events ---------------------------------------------------------------------------------------


def test_github_event_becomes_one_event(spark, registry):
    [e] = silver.events_flow(decoded_df(spark, GH_EVENTS, [gh_event()], [7]), registry, GH_EVENTS
                             ).collect()
    assert e.event_id == "github:fork:599547518:11"
    assert (e.entity_id, e.source, e.event_type, e.signal_class, e.value) == (
        "vllm", "github", "fork", "attention", 1
    )
    assert e.source_url == "https://github.com/ada/vllm/11"
    assert e.bronze_ref.asDict() == {"topic": GH_EVENTS.topic, "partition": 0, "offset": 7}
    assert e.event_ts == ts("2026-11-07T15:00:00").replace(tzinfo=None)


def test_snapshot_becomes_one_event_per_metric_at_utc_midnight(spark, registry):
    rows = silver.events_flow(decoded_df(spark, GH_SNAPSHOTS, [gh_snapshot()]), registry,
                              GH_SNAPSHOTS).collect()
    by_type = {r.event_type: r for r in rows}
    assert set(by_type) == {"stars_total", "forks_total"}
    assert (by_type["stars_total"].value, by_type["forks_total"].value) == (96000, 16100)
    stars = by_type["stars_total"]
    assert stars.event_id == "github:vllm-project/vllm:2026-11-08:stars_total"
    assert stars.event_ts == datetime(2026, 11, 8)  # UTC midnight (session time zone is UTC)
    assert stars.source_url == "https://github.com/vllm-project/vllm"


def test_pypi_and_docker_rows_are_adoption(spark, registry):
    pypi = {"source_event_id": "vllm:2026-11-07:without_mirrors", "package": "vllm",
            "download_date": date(2026, 11, 7), "category": "without_mirrors",
            "downloads": 500000, "polled_at": INGEST}
    docker = {"source_event_id": "vllm/vllm-openai:2026-11-08", "image": "vllm/vllm-openai",
              "pull_count": 9_000_000, "star_count": 300, "snapshot_date": date(2026, 11, 8),
              "polled_at": INGEST}
    [p] = silver.events_flow(decoded_df(spark, PYPI, [pypi]), registry, PYPI).collect()
    [d] = silver.events_flow(decoded_df(spark, DOCKER, [docker]), registry, DOCKER).collect()
    assert (p.event_type, p.signal_class, p.value, p.entity_id) == (
        "downloads", "adoption", 500000, "vllm"
    )
    assert p.source_url == "https://pypistats.org/packages/vllm"
    assert (d.event_type, d.signal_class, d.value) == ("pulls_total", "adoption", 9_000_000)
    assert d.source_url == "https://hub.docker.com/r/vllm/vllm-openai"


def test_every_source_produces_exactly_the_event_model(spark, registry):
    """All flows append to one table, so their output schemas must be identical."""
    expected = spark.createDataFrame([], silver.EVENTS_SCHEMA).schema
    for src in silver.SOURCES:
        out = silver.events_flow(decoded_df(spark, src, []), registry, src)
        assert out.schema.simpleString() == expected.simpleString(), src.name


def test_handle_match_ignores_case(spark, registry):
    row = gh_event(repo="BerriAI/LiteLLM")
    [e] = silver.events_flow(decoded_df(spark, GH_EVENTS, [row]), registry, GH_EVENTS).collect()
    assert e.entity_id == "litellm"


def test_duplicates_are_dropped(spark, registry):
    rows = [gh_event(oid=1), gh_event(oid=1), gh_event(oid=2)]
    out = silver.events_flow(decoded_df(spark, GH_EVENTS, rows), registry, GH_EVENTS)
    assert sorted(r.event_id for r in out.collect()) == [
        "github:fork:599547518:1", "github:fork:599547518:2"
    ]


# --- quarantine -----------------------------------------------------------------------------------


def test_undecodable_row_goes_to_quarantine_not_events(spark, registry):
    decoded = decoded_df(spark, GH_EVENTS, [None, gh_event()], [3, 4])
    assert silver.events_flow(decoded, registry, GH_EVENTS).count() == 1
    [q] = silver.quarantine_flow(decoded, registry, GH_EVENTS).collect()
    assert (q.source, q.reason, q.bronze_ref.offset) == ("github", "decode_failed", 3)
    assert base64.b64decode(q.payload) == b"\x00raw"


def test_unknown_handle_goes_to_quarantine(spark, registry):
    decoded = decoded_df(spark, GH_EVENTS, [gh_event(repo="someone/unknown")], [9])
    assert silver.events_flow(decoded, registry, GH_EVENTS).count() == 0
    [q] = silver.quarantine_flow(decoded, registry, GH_EVENTS).collect()
    assert (q.reason, q.bronze_ref.offset) == ("unresolved_entity", 9)
    assert json.loads(q.payload)["repo"] == "someone/unknown"


# --- streaming: dedup across runs, and a closed day never changes ---------------------------------


def run_available_now(spark, query_df, out_dir, checkpoint):
    (
        query_df.writeStream.format("parquet")
        .option("checkpointLocation", str(checkpoint))
        .outputMode("append")
        .trigger(availableNow=True)
        .start(str(out_dir))
        .awaitTermination()
    )
    return spark.read.parquet(str(out_dir)) if any(Path(out_dir).glob("*.parquet")) else None


def test_overlap_refetch_is_dropped_across_runs(spark, registry, tmp_path):
    """Day 1's run, then day 2's run re-reads 6 h of overlap: the copy is dropped."""
    src_dir, out, chk = tmp_path / "in", tmp_path / "out", tmp_path / "chk"
    src_dir.mkdir()
    schema = decoded_df(spark, GH_EVENTS, []).schema

    def produce(run: int, recs):
        decoded_df(spark, GH_EVENTS, recs).write.mode("append").parquet(str(src_dir / str(run)))

    stream = spark.readStream.schema(schema).option("recursiveFileLookup", "true").parquet(
        str(src_dir)
    )
    query = silver.events_flow(stream, registry, GH_EVENTS)
    last_of_day_1 = gh_event(oid=2, at="2026-11-07T23:00:00")
    produce(1, [gh_event(oid=1, at="2026-11-07T20:00:00"), last_of_day_1])
    run_available_now(spark, query, out, chk)
    produce(2, [last_of_day_1, gh_event(oid=3, at="2026-11-08T10:00:00")])  # overlap re-fetch
    result = run_available_now(spark, query, out, chk)
    assert sorted(r.event_id[-1] for r in result.collect()) == ["1", "2", "3"]


def test_closed_day_is_final_and_late_events_are_dropped(spark, tmp_path):
    """The watermark decides when a day is final (D023): option A's whole point."""
    src_dir, out, chk = tmp_path / "in", tmp_path / "out", tmp_path / "chk"
    src_dir.mkdir()

    def produce(run: int, *times: str):
        rows = [
            (f"github:fork:1:{run}{i}", f"fork:1:{run}{i}", "vllm", "github", "fork", "attention",
             ts(t), INGEST, 1, "u", ("t", 0, 0))
            for i, t in enumerate(times)
        ]
        spark.createDataFrame(rows, silver.EVENTS_SCHEMA).write.parquet(str(src_dir / str(run)))

    stream = spark.readStream.schema(silver.EVENTS_SCHEMA).option(
        "recursiveFileLookup", "true"
    ).parquet(str(src_dir))
    query = silver.github_daily_counts(stream)

    def counts():
        df = run_available_now(spark, query, out, chk)
        return {} if df is None else {r.day.isoformat(): r.event_count for r in df.collect()}

    # Run on Nov 8: yesterday's events. Newest 23:00 Nov 7 -> watermark 11:00 Nov 7.
    produce(1, "2026-11-06T10:00:00", "2026-11-06T20:00:00", "2026-11-07T23:00:00")
    after_run_1 = counts()
    # Run on Nov 9: a late Nov 6 event (behind the watermark) plus Nov 8's events.
    produce(2, "2026-11-06T15:00:00", "2026-11-08T23:00:00")
    after_run_2 = counts()

    assert after_run_1 == {"2026-11-06": 2}  # Nov 6 closed within the same run; Nov 7 open
    assert after_run_2 == {"2026-11-06": 2, "2026-11-07": 1}  # late event dropped, Nov 6 final
