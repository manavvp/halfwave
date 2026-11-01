import io
import json
from datetime import UTC, date, datetime
from pathlib import Path

import fastavro
import pytest
import requests

from producers.kafka_io import load_schema
from producers.sources import pypi

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "pypistats_ctranslate2_overall.json").read_text()
)  # real response, trimmed to 2026-10-26..30
POLLED_AT = datetime(2026, 10, 31, 6, 0, tzinfo=UTC)
FIRST, LAST = pypi.date_window(date(2026, 10, 30))  # 2026-10-28..30


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


def payload(package, *days):
    rows = [{"category": "without_mirrors", "date": d, "downloads": 10} for d in days]
    return {"data": rows, "package": package, "type": "overall_downloads"}


# --- window and records -----------------------------------------------------------------------


def test_default_window_is_the_last_three_days():
    assert (FIRST, LAST) == (date(2026, 10, 28), date(2026, 10, 30))


def test_window_rejects_zero_days():
    with pytest.raises(ValueError):
        pypi.date_window(date(2026, 10, 30), 0)


def test_records_from_real_response_keep_only_the_window():
    records = pypi.to_records("ctranslate2", FIXTURE, FIRST, LAST, POLLED_AT)
    assert [r["download_date"] for r in records] == [
        date(2026, 10, 28),
        date(2026, 10, 29),
        date(2026, 10, 30),
    ]
    first = records[0]
    assert first["source_event_id"] == "ctranslate2:2026-10-28:without_mirrors"
    assert (first["downloads"], first["category"]) == (910837, "without_mirrors")


def test_refetch_gives_identical_ids():
    """The dedup key never depends on when we fetched (D004)."""
    early = pypi.to_records("ctranslate2", FIXTURE, FIRST, LAST, POLLED_AT)
    later = pypi.to_records("ctranslate2", FIXTURE, FIRST, LAST, datetime(2026, 11, 2, tzinfo=UTC))
    assert [r["source_event_id"] for r in early] == [r["source_event_id"] for r in later]


def test_naive_poll_time_is_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        pypi.to_records("ctranslate2", FIXTURE, FIRST, LAST, datetime(2026, 10, 31))


def test_record_matches_avro_schema():
    [record, *_] = pypi.to_records("ctranslate2", FIXTURE, FIRST, LAST, POLLED_AT)
    schema = fastavro.parse_schema(json.loads(load_schema(pypi.TOPIC)))
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    buf.seek(0)
    decoded = fastavro.schemaless_reader(buf, schema)
    assert decoded["download_date"] == date(2026, 10, 28)
    assert decoded["source_event_id"] == record["source_event_id"]


# --- fetch_overall ----------------------------------------------------------------------------


def test_retries_rate_limit_honouring_retry_after():
    session = FakeSession(
        FakeResponse(429, headers={"Retry-After": "30"}),
        requests.ConnectionError("reset"),
        FakeResponse(200, FIXTURE),
    )
    sleeps = []
    assert pypi.fetch_overall(session, "ctranslate2", sleep=sleeps.append) == FIXTURE
    assert sleeps == [30.0, 10.0]  # Retry-After beats the 5s backoff; then 10s backoff
    assert session.urls[0].endswith("/ctranslate2/overall?mirrors=false")


def test_unknown_package_fails_immediately():
    session = FakeSession(FakeResponse(404))
    with pytest.raises(pypi.FetchError, match="HTTP 404"):
        pypi.fetch_overall(session, "nope", sleep=lambda _: None)
    assert len(session.urls) == 1


# --- poll -------------------------------------------------------------------------------------


def test_one_failing_package_does_not_stop_the_rest_and_rows_are_date_ordered():
    session = FakeSession(
        FakeResponse(200, payload("b-pkg", "2026-10-29", "2026-10-30")),
        FakeResponse(404),
        FakeResponse(200, payload("a-pkg", "2026-10-28", "2026-10-30")),
    )
    sleeps = []
    records, failures = pypi.poll(
        ["b-pkg", "gone", "a-pkg"], session, FIRST, LAST, POLLED_AT, sleep=sleeps.append
    )
    assert len(failures) == 1 and failures[0].startswith("gone")
    assert [(r["download_date"].day, r["package"]) for r in records] == [
        (28, "a-pkg"),
        (29, "b-pkg"),
        (30, "a-pkg"),
        (30, "b-pkg"),
    ]
    assert sleeps == [pypi.DEFAULT_PACE_S, pypi.DEFAULT_PACE_S]  # paced between packages
