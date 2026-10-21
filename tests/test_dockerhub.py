import io
import json
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import fastavro
import pytest
import requests

from producers.kafka_io import load_schema
from producers.sources import dockerhub

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "dockerhub_vllm_vllm-openai.json").read_text()
)
POLLED_AT = datetime(2026, 10, 2, 9, 30, tzinfo=UTC)


class FakeResponse:
    def __init__(self, status_code, body=None, headers=None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}

    def json(self):
        return self._body


class FakeSession:
    """Returns queued responses (or raises queued exceptions) in order."""

    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.urls = []

    def get(self, url, timeout):
        self.urls.append(url)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


# --- build_snapshot ----------------------------------------------------------------------------


def test_snapshot_from_real_response():
    record = dockerhub.build_snapshot("vllm/vllm-openai", FIXTURE, POLLED_AT)
    assert record["source_event_id"] == "vllm/vllm-openai:2026-10-02"
    assert record["pull_count"] == FIXTURE["pull_count"]
    assert record["star_count"] == FIXTURE["star_count"]
    assert record["snapshot_date"] == date(2026, 10, 2)
    assert record["last_updated"] == datetime(2026, 10, 2, 7, 2, 53, 245246, tzinfo=UTC)


def test_same_utc_day_gives_same_source_event_id():
    early = dockerhub.build_snapshot("a/b", FIXTURE, datetime(2026, 10, 2, 0, 5, tzinfo=UTC))
    late = dockerhub.build_snapshot("a/b", FIXTURE, datetime(2026, 10, 2, 23, 55, tzinfo=UTC))
    assert early["source_event_id"] == late["source_event_id"]


def test_snapshot_date_is_the_utc_day_not_the_local_day():
    # 01:00 on Oct 3 in UTC+05:30 is still Oct 2 in UTC.
    ist = timezone(timedelta(hours=5, minutes=30))
    record = dockerhub.build_snapshot("a/b", FIXTURE, datetime(2026, 10, 3, 1, 0, tzinfo=ist))
    assert record["snapshot_date"] == date(2026, 10, 2)


def test_naive_poll_time_is_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        dockerhub.build_snapshot("a/b", FIXTURE, datetime(2026, 10, 2))


def test_record_matches_avro_schema():
    """Catches drift between build_snapshot and the .avsc before Schema Registry does."""
    schema = fastavro.parse_schema(json.loads(load_schema(dockerhub.TOPIC)))
    record = dockerhub.build_snapshot("vllm/vllm-openai", FIXTURE, POLLED_AT)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    buf.seek(0)
    decoded = fastavro.schemaless_reader(buf, schema)
    assert decoded["source_event_id"] == record["source_event_id"]
    assert decoded["pull_count"] == record["pull_count"]
    assert decoded["snapshot_date"] == record["snapshot_date"]


def test_missing_last_updated_is_null():
    payload = {**FIXTURE, "last_updated": None}
    assert dockerhub.build_snapshot("a/b", payload, POLLED_AT)["last_updated"] is None


# --- fetch_repository --------------------------------------------------------------------------


def test_fetch_retries_rate_limit_then_succeeds():
    session = FakeSession(FakeResponse(429), FakeResponse(503), FakeResponse(200, FIXTURE))
    sleeps = []
    body = dockerhub.fetch_repository(session, "vllm/vllm-openai", sleep=sleeps.append)
    assert body == FIXTURE
    assert sleeps == [2.0, 4.0]  # exponential backoff
    assert session.urls[0] == "https://hub.docker.com/v2/repositories/vllm/vllm-openai/"


def test_fetch_honours_retry_after():
    session = FakeSession(FakeResponse(429, headers={"Retry-After": "30"}), FakeResponse(200, {}))
    sleeps = []
    dockerhub.fetch_repository(session, "a/b", sleep=sleeps.append)
    assert sleeps == [30.0]


def test_fetch_retries_network_errors():
    session = FakeSession(requests.ConnectionError("reset"), FakeResponse(200, FIXTURE))
    assert dockerhub.fetch_repository(session, "a/b", sleep=lambda _: None) == FIXTURE


def test_fetch_does_not_retry_404():
    session = FakeSession(FakeResponse(404))
    with pytest.raises(dockerhub.FetchError, match="HTTP 404 .not retryable"):
        dockerhub.fetch_repository(session, "a/b", sleep=lambda _: None)
    assert len(session.urls) == 1


def test_fetch_gives_up_after_max_attempts():
    session = FakeSession(*[FakeResponse(429)] * 3)
    with pytest.raises(dockerhub.FetchError, match="gave up after 3 attempts"):
        dockerhub.fetch_repository(session, "a/b", max_attempts=3, sleep=lambda _: None)


# --- poll --------------------------------------------------------------------------------------


def test_one_failing_image_does_not_stop_the_rest():
    session = FakeSession(FakeResponse(404), FakeResponse(200, FIXTURE))
    records, failures = dockerhub.poll(
        ["gone/image", "a/b"], session, POLLED_AT, sleep=lambda _: None
    )
    assert [r["image"] for r in records] == ["a/b"]
    assert len(failures) == 1 and failures[0].startswith("gone/image")
