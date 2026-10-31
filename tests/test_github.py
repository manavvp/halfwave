import io
import json
from datetime import UTC, date, datetime, timedelta

import fastavro
import pytest
import requests

from producers.kafka_io import load_schema
from producers.sources import github

POLLED_AT = datetime(2026, 10, 30, 6, 0, tzinfo=UTC)
WINDOW = github.window_for(date(2026, 10, 29))  # 2026-10-28 18:00 -> 2026-10-30 00:00
REPO = "vllm-project/vllm"
REPO_ID = 599547518

SNAPSHOT = {
    "databaseId": REPO_ID,
    "nameWithOwner": REPO,
    "stargazerCount": 95856,
    "forkCount": 16012,
    "pushedAt": "2026-10-30T05:41:10Z",
    "isArchived": False,
    "watchers": {"totalCount": 612},
    "openIssues": {"totalCount": 2210},
    "openPullRequests": {"totalCount": 1180},
}


class FakeResponse:
    def __init__(self, status_code, body=None, headers=None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}

    def json(self):
        return self._body


def ok(data, remaining=4999):
    rate = {"remaining": remaining, "resetAt": "2026-10-30T07:00:00Z"}
    return FakeResponse(200, {"data": {"rateLimit": rate, **data}})


def page(name, items, has_next=False, cursor="c1"):
    info = {"hasNextPage": has_next, "endCursor": cursor}
    return ok({"repository": {name: {"pageInfo": info, "nodes": items}}})


class FakeSession:
    """Returns queued responses (or raises queued exceptions) in order; records each request."""

    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.requests = []

    def post(self, url, json, timeout):
        self.requests.append(json)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def fork(ts, fid=11, login="ada"):
    url = f"https://github.com/{login}/vllm"
    return {"databaseId": fid, "url": url, "createdAt": ts, "owner": {"login": login}}


# --- window -----------------------------------------------------------------------------------


def test_daily_window_is_yesterday_plus_overlap():
    assert WINDOW.since == datetime(2026, 10, 28, 18, 0, tzinfo=UTC)
    assert WINDOW.until == datetime(2026, 10, 30, 0, 0, tzinfo=UTC)


def test_backfill_window_covers_whole_days():
    w = github.window_for(date(2026, 10, 29), days=90, overlap_hours=0)
    assert w.until - w.since == timedelta(days=90)
    assert w.since == datetime(2026, 8, 1, tzinfo=UTC)


def test_window_is_half_open():
    assert WINDOW.since in WINDOW
    assert WINDOW.until not in WINDOW
    assert None not in WINDOW


@pytest.mark.parametrize(("days", "overlap"), [(0, 6), (1, -1)])
def test_window_rejects_nonsense(days, overlap):
    with pytest.raises(ValueError):
        github.window_for(date(2026, 10, 29), days, overlap)


# --- to_events --------------------------------------------------------------------------------


def events(name, items):
    return list(github.to_events(name, items, REPO, REPO_ID, WINDOW, POLLED_AT))


def test_fork_inside_window_becomes_an_event():
    [e] = events("forks", [fork("2026-10-29T15:04:05Z", fid=42, login="ada")])
    assert e["event_type"] == "fork"
    assert e["event_ts"] == datetime(2026, 10, 29, 15, 4, 5, tzinfo=UTC)
    assert e["source_event_id"] == f"fork:{REPO_ID}:42"
    assert (e["actor"], e["object_id"], e["url"]) == ("ada", 42, "https://github.com/ada/vllm")


def test_events_outside_the_window_are_dropped():
    items = [fork("2026-10-30T00:00:00Z"), fork("2026-10-28T17:59:59Z")]  # until, before since
    assert events("forks", items) == []


def test_refetch_gives_identical_ids():
    """The dedup key never depends on when we fetched (D004)."""
    item = [fork("2026-10-29T10:00:00Z")]
    first = github.to_events("forks", item, REPO, REPO_ID, WINDOW, POLLED_AT)
    later = github.to_events("forks", item, REPO, REPO_ID, WINDOW, POLLED_AT + timedelta(1))
    assert next(first)["source_event_id"] == next(later)["source_event_id"]


def test_fork_by_deleted_account_keeps_a_null_actor():
    item = {"databaseId": 13, "url": "f", "createdAt": "2026-10-29T10:00:00Z", "owner": None}
    [e] = events("forks", [item])
    assert e["actor"] is None


def test_stars_are_not_fetched_as_events():
    """GitHub lists stargazers only to repo admins since mid-2026 (D018)."""
    assert "stargazers" not in github.CONNECTIONS


def test_old_pr_merged_in_window_gives_only_a_merge_event():
    pr = {
        "databaseId": 7,
        "number": 27001,
        "url": "https://github.com/vllm-project/vllm/pull/27001",
        "createdAt": "2026-09-01T08:00:00Z",
        "updatedAt": "2026-10-29T12:00:00Z",
        "mergedAt": "2026-10-29T12:00:00Z",
        "author": {"login": "bob"},
    }
    [e] = events("pullRequests", [pr])
    assert (e["event_type"], e["number"], e["actor"]) == ("pr_merged", 27001, "bob")
    assert e["source_event_id"] == f"pr_merged:{REPO_ID}:7"


def test_pr_opened_and_merged_in_window_gives_two_events():
    pr = {
        "databaseId": 8,
        "number": 27002,
        "url": "u",
        "createdAt": "2026-10-29T09:00:00Z",
        "updatedAt": "2026-10-29T12:00:00Z",
        "mergedAt": "2026-10-29T12:00:00Z",
        "author": None,
    }
    assert [e["event_type"] for e in events("pullRequests", [pr])] == ["pr_opened", "pr_merged"]


def test_draft_release_is_not_an_event():
    draft = {"databaseId": 3, "url": "u", "createdAt": "2026-10-29T09:00:00Z", "publishedAt": None}
    assert events("releases", [draft]) == []


def test_issue_event():
    issue = {"databaseId": 12, "number": 5, "url": "i", "createdAt": "2026-10-29T09:00:00Z"}
    [i] = events("issues", [issue])
    assert (i["event_type"], i["number"], i["actor"]) == ("issue_opened", 5, None)


# --- Avro -------------------------------------------------------------------------------------


def roundtrip(topic, record):
    schema = fastavro.parse_schema(json.loads(load_schema(topic)))
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    buf.seek(0)
    return fastavro.schemaless_reader(buf, schema)


def test_event_matches_avro_schema():
    [e] = events("forks", [fork("2026-10-29T15:04:05Z")])
    decoded = roundtrip(github.EVENTS_TOPIC, e)
    assert decoded["source_event_id"] == e["source_event_id"]
    assert decoded["event_ts"] == e["event_ts"]


def test_snapshot_matches_avro_schema():
    snap = github.build_snapshot(REPO, SNAPSHOT, POLLED_AT)
    decoded = roundtrip(github.SNAPSHOTS_TOPIC, snap)
    assert decoded["stars"] == 95856
    assert decoded["snapshot_date"] == date(2026, 10, 30)
    assert snap["source_event_id"] == f"{REPO}:2026-10-30"


def test_naive_poll_time_is_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        github.build_snapshot(REPO, SNAPSHOT, datetime(2026, 10, 30))


# --- pagination -------------------------------------------------------------------------------


def test_paging_stops_once_items_are_older_than_since():
    session = FakeSession(
        page("forks", [fork("2026-10-29T20:00:00Z")], has_next=True, cursor="p2"),
        page("forks", [fork("2026-10-28T10:00:00Z")], has_next=True, cursor="p3"),
    )
    items = github.fetch_connection(session, REPO, "forks", WINDOW.since)
    assert len(items) == 2  # never asked for p3
    assert [r["variables"]["after"] for r in session.requests] == [None, "p2"]


def test_paging_stops_at_the_last_page():
    session = FakeSession(page("forks", [], has_next=False))
    assert github.fetch_connection(session, REPO, "forks", WINDOW.since) == []


# --- post_graphql -----------------------------------------------------------------------------


def test_retries_server_errors_then_succeeds():
    session = FakeSession(FakeResponse(502), requests.ConnectionError("reset"), ok({"x": 1}))
    sleeps = []
    assert github.post_graphql(session, "q", {}, sleep=sleeps.append)["x"] == 1
    assert sleeps == [2.0, 4.0]


def test_secondary_rate_limit_honours_retry_after():
    session = FakeSession(FakeResponse(403, headers={"Retry-After": "45"}), ok({}))
    sleeps = []
    github.post_graphql(session, "q", {}, sleep=sleeps.append)
    assert sleeps == [45.0]


def test_bad_token_fails_immediately():
    session = FakeSession(FakeResponse(401))
    with pytest.raises(github.FetchError, match="GITHUB_TOKEN"):
        github.post_graphql(session, "q", {}, sleep=lambda _: None)
    assert len(session.requests) == 1


def test_graphql_error_is_not_retried():
    body = {"errors": [{"type": "NOT_FOUND", "message": "Could not resolve to a Repository"}]}
    session = FakeSession(FakeResponse(200, body))
    with pytest.raises(github.FetchError, match="Could not resolve"):
        github.post_graphql(session, "q", {}, sleep=lambda _: None)


def test_waits_for_reset_when_points_run_low():
    session = FakeSession(ok({}, remaining=10))
    sleeps = []
    now = lambda: datetime(2026, 10, 30, 6, 59, 0, tzinfo=UTC)  # noqa: E731
    github.post_graphql(session, "q", {}, sleep=sleeps.append, now=now)
    assert sleeps == [61.0]  # until resetAt 07:00, plus a second


# --- poll -------------------------------------------------------------------------------------


def repo_responses(fork_ts):
    return [
        ok({"repository": SNAPSHOT}),
        page("forks", [fork(fork_ts)]),
        page("releases", []),
        page("pullRequests", []),
        page("issues", []),
    ]


def test_one_failing_repo_does_not_stop_the_rest_and_events_are_time_ordered():
    not_found = FakeResponse(200, {"errors": [{"type": "NOT_FOUND", "message": "gone"}]})
    session = FakeSession(
        *repo_responses("2026-10-29T20:00:00Z"),
        not_found,
        *repo_responses("2026-10-29T08:00:00Z"),
    )
    evs, snaps, failures = github.poll(
        [REPO, "gone/repo", "other/repo"], session, WINDOW, POLLED_AT, sleep=lambda _: None
    )
    assert len(snaps) == 2
    assert len(failures) == 1 and failures[0].startswith("gone/repo")
    assert [e["event_ts"].hour for e in evs] == [8, 20]  # sorted across repos
