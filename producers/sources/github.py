"""GitHub poller: attention and activity events for registry repos, plus a daily repo snapshot.

Signals come from the GitHub GraphQL API, which timestamps every fact at the source: a fork's
created_at, a release's published_at, a PR's created_at and merged_at, an issue's created_at. So
this is event data, produced to Kafka in event-time order (D015, D016). Stars are not events here:
since mid-2026 GitHub lists a repo's stargazers only to its admins and collaborators (D018).

Each run covers whole UTC days ending at the end of --date (default: yesterday), widened
backwards by --overlap-hours. The window is a pure function of the arguments, so the producer
keeps no state: a rerun produces the same events again and silver drops them as duplicates.

The snapshot (current star/fork/issue totals) is taken at poll time and can't be backfilled. It
is the only star signal (a daily delta), and gold reconciles counted fork events against its fork
counter (D018).

    python -m producers.sources.github --dry-run                  # yesterday, print only
    python -m producers.sources.github                            # yesterday -> Kafka
    python -m producers.sources.github --days 90                  # backfill
    python -m producers.sources.github --date 2026-10-28 --repos vllm-project/vllm
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import requests

from producers.config import require_env
from producers.registry import load_registry

log = logging.getLogger(__name__)

EVENTS_TOPIC = "radar.github.events.v1"
SNAPSHOTS_TOPIC = "radar.github.snapshots.v1"
API_URL = "https://api.github.com/graphql"
USER_AGENT = "halfwave-radar/0.1 (+https://github.com/manavvp/halfwave)"
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
DEFAULT_OVERLAP_HOURS = 6
# GraphQL allows 5,000 points an hour; one page of 100 nodes costs 1. Below this many points
# left, wait for the reset instead of running into the limit mid-repo.
RATE_LIMIT_FLOOR = 50

Sleep = Callable[[float], None]
Now = Callable[[], datetime]


class FetchError(RuntimeError):
    """A repo could not be fetched."""


# --- the window ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class Window:
    """[since, until) in UTC."""

    since: datetime
    until: datetime

    def __contains__(self, ts: object) -> bool:
        return isinstance(ts, datetime) and self.since <= ts < self.until


def window_for(day: date, days: int = 1, overlap_hours: int = DEFAULT_OVERLAP_HOURS) -> Window:
    """`days` whole UTC days ending with `day`, plus `overlap_hours` before them.

    The overlap must stay below silver's watermark delay: overlap events are older than the
    previous run's newest event, and anything behind the watermark is dropped as late.
    """
    if days < 1 or overlap_hours < 0:
        raise ValueError("days must be >= 1 and overlap_hours >= 0")
    until = datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(days=1)
    return Window(until - timedelta(days=days, hours=overlap_hours), until)


def parse_ts(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None  # GitHub sends ISO 8601, UTC ("Z")


# --- GraphQL ------------------------------------------------------------------------------------


def post_graphql(
    session: requests.Session,
    query: str,
    variables: dict[str, Any],
    *,
    max_attempts: int = 4,
    base_delay_s: float = 2.0,
    sleep: Sleep = time.sleep,
    now: Now = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    """POST one query, retrying rate limits, server errors and network failures.

    Anything else (bad token, unknown repo, a malformed query) fails immediately: retrying would
    only hide a bug.
    """
    error = ""
    for attempt in range(1, max_attempts + 1):
        delay = base_delay_s * 2 ** (attempt - 1)
        try:
            resp = session.post(API_URL, json={"query": query, "variables": variables}, timeout=30)
        except requests.RequestException as exc:
            error = f"{type(exc).__name__}: {exc}"
        else:
            if resp.status_code == 200:
                body = resp.json()
                errors = body.get("errors") or []
                if not errors:
                    data = body["data"]
                    _wait_if_nearly_out_of_points(data.get("rateLimit"), sleep, now)
                    return data
                if not any(e.get("type") == "RATE_LIMITED" for e in errors):
                    first = errors[0]
                    where = ".".join(str(p) for p in first.get("path") or []) or "query"
                    raise FetchError(
                        f"GraphQL error at {where} (not retryable): {first.get('message')}"
                    )
                error, delay = "RATE_LIMITED", max(delay, 60.0)
            elif resp.status_code == 401:
                raise FetchError("HTTP 401: GITHUB_TOKEN is missing, expired or revoked")
            # 403 with Retry-After is GitHub's "secondary" (burst) rate limit.
            elif resp.status_code in RETRYABLE_STATUSES or (
                resp.status_code == 403 and "Retry-After" in resp.headers
            ):
                error = f"HTTP {resp.status_code}"
                retry_after = resp.headers.get("Retry-After", "")
                if retry_after.isdigit():
                    delay = max(delay, float(retry_after))
            else:
                raise FetchError(f"HTTP {resp.status_code} (not retryable)")
        if attempt < max_attempts:
            log.warning("GraphQL %s, retrying in %.0fs", error, delay)
            sleep(delay)
    raise FetchError(f"gave up after {max_attempts} attempts ({error})")


def _wait_if_nearly_out_of_points(rate: dict[str, Any] | None, sleep: Sleep, now: Now) -> None:
    if rate and rate["remaining"] < RATE_LIMIT_FLOOR:
        wait = (parse_ts(rate["resetAt"]) - now()).total_seconds() + 1
        log.warning("%d GraphQL points left, sleeping %.0fs until reset", rate["remaining"], wait)
        sleep(max(0.0, wait))


_QUERY = """
query($owner: String!, $name: String!, $after: String) {
  rateLimit { remaining resetAt }
  repository(owner: $owner, name: $name) { %s }
}
"""
_PAGE_INFO = "pageInfo { hasNextPage endCursor }"


@dataclass(frozen=True)
class Connection:
    """One paged list on a repository, newest first by `order_field`."""

    selection: str  # the GraphQL field, with first/after/orderBy
    items_key: str  # "edges" or "nodes"
    order_field: str  # timestamp the list is sorted by (descending): pagination stops on it

    @property
    def query(self) -> str:
        return _QUERY % self.selection


def _conn(field: str, order: str, items: str, order_field: str) -> Connection:
    selection = (
        f"{field}(first: 100, after: $after, orderBy: {{field: {order}, direction: DESC}}) "
        f"{{ {_PAGE_INFO} {items} }}"
    )
    return Connection(selection, items.split(" ", 1)[0], order_field)


CONNECTIONS: dict[str, Connection] = {
    "forks": _conn(
        "forks", "CREATED_AT", "nodes { databaseId url createdAt owner { login } }", "createdAt"
    ),
    "releases": _conn(
        "releases",
        "CREATED_AT",
        "nodes { databaseId url createdAt publishedAt author { login } }",
        "createdAt",
    ),
    # Ordered by last update, so a PR opened long ago but merged inside the window is still seen.
    "pullRequests": _conn(
        "pullRequests",
        "UPDATED_AT",
        "nodes { databaseId number url createdAt updatedAt mergedAt author { login } }",
        "updatedAt",
    ),
    "issues": _conn(
        "issues", "CREATED_AT", "nodes { databaseId number url createdAt author { login } }",
        "createdAt",
    ),
}

SNAPSHOT_QUERY = """
query($owner: String!, $name: String!) {
  rateLimit { remaining resetAt }
  repository(owner: $owner, name: $name) {
    databaseId nameWithOwner stargazerCount forkCount pushedAt isArchived
    watchers { totalCount }
    openIssues: issues(states: OPEN) { totalCount }
    openPullRequests: pullRequests(states: OPEN) { totalCount }
  }
}
"""


def fetch_connection(
    session: requests.Session, repo: str, name: str, since: datetime, *, sleep: Sleep = time.sleep
) -> list[dict[str, Any]]:
    """Page newest-first through one connection until items are older than `since`."""
    owner, repo_name = repo.split("/")
    conn = CONNECTIONS[name]
    items: list[dict[str, Any]] = []
    after = None
    while True:
        data = post_graphql(
            session, conn.query, {"owner": owner, "name": repo_name, "after": after}, sleep=sleep
        )
        page = data["repository"][name]
        batch = page[conn.items_key]
        items.extend(batch)
        oldest = parse_ts(batch[-1][conn.order_field]) if batch else None
        if not page["pageInfo"]["hasNextPage"] or oldest is None or oldest < since:
            return items
        after = page["pageInfo"]["endCursor"]


# --- records ------------------------------------------------------------------------------------


def _event(
    event_type: str,
    repo: str,
    repo_id: int,
    ts: datetime,
    source_event_id: str,
    item: dict[str, Any],
    fetched_at: datetime,
    *,
    actor: str | None,
) -> dict[str, Any]:
    return {
        # Built from GitHub's own ids, never from when we fetched it, so a re-fetch is a
        # duplicate in silver (D004).
        "source_event_id": source_event_id,
        "event_type": event_type,
        "repo": repo,
        "repo_id": repo_id,
        "event_ts": ts,
        "actor": actor,
        "object_id": item.get("databaseId"),
        "number": item.get("number"),
        "url": item.get("url"),
        "fetched_at": fetched_at,
    }


def _login(item: dict[str, Any], key: str) -> str | None:
    return (item.get(key) or {}).get("login")  # null when the account was deleted


def to_events(
    name: str,
    items: list[dict[str, Any]],
    repo: str,
    repo_id: int,
    window: Window,
    fetched_at: datetime,
) -> Iterator[dict[str, Any]]:
    """Turn one connection's items into event records, keeping only events inside the window."""
    for item in items:
        if name == "forks":
            ts = parse_ts(item["createdAt"])
            if ts in window:
                sid = f"fork:{repo_id}:{item['databaseId']}"
                actor = _login(item, "owner")
                yield _event("fork", repo, repo_id, ts, sid, item, fetched_at, actor=actor)
        elif name == "releases":
            ts = parse_ts(item.get("publishedAt"))  # null for drafts: not public yet
            if ts in window:
                sid = f"release:{repo_id}:{item['databaseId']}"
                actor = _login(item, "author")
                yield _event("release", repo, repo_id, ts, sid, item, fetched_at, actor=actor)
        elif name == "pullRequests":
            actor = _login(item, "author")
            for event_type, field in (("pr_opened", "createdAt"), ("pr_merged", "mergedAt")):
                ts = parse_ts(item.get(field))
                if ts in window:
                    sid = f"{event_type}:{repo_id}:{item['databaseId']}"
                    yield _event(event_type, repo, repo_id, ts, sid, item, fetched_at, actor=actor)
        elif name == "issues":
            ts = parse_ts(item["createdAt"])
            if ts in window:
                sid = f"issue_opened:{repo_id}:{item['databaseId']}"
                actor = _login(item, "author")
                yield _event("issue_opened", repo, repo_id, ts, sid, item, fetched_at, actor=actor)
        else:
            raise ValueError(f"unknown connection {name!r}")


def build_snapshot(repo: str, payload: dict[str, Any], polled_at: datetime) -> dict[str, Any]:
    """Map a SNAPSHOT_QUERY repository to a radar.github.snapshots.v1 record."""
    if polled_at.tzinfo is None:
        raise ValueError("polled_at must be timezone-aware")
    polled_at = polled_at.astimezone(UTC)
    snapshot_date = polled_at.date()
    return {
        # Same rule as Docker Hub: one snapshot per repo per UTC day, first poll wins (D004).
        "source_event_id": f"{repo}:{snapshot_date.isoformat()}",
        "repo": repo,
        "repo_id": int(payload["databaseId"]),
        "stars": int(payload["stargazerCount"]),
        "forks": int(payload["forkCount"]),
        "watchers": int(payload["watchers"]["totalCount"]),
        "open_issues": int(payload["openIssues"]["totalCount"]),
        "open_pull_requests": int(payload["openPullRequests"]["totalCount"]),
        "pushed_at": parse_ts(payload.get("pushedAt")),
        "is_archived": bool(payload["isArchived"]),
        "snapshot_date": snapshot_date,
        "polled_at": polled_at,
    }


# --- one run ------------------------------------------------------------------------------------


def collect_repo(
    session: requests.Session,
    repo: str,
    window: Window,
    polled_at: datetime,
    *,
    sleep: Sleep = time.sleep,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """All events in the window for one repo, plus its snapshot."""
    owner, name = repo.split("/")
    data = post_graphql(session, SNAPSHOT_QUERY, {"owner": owner, "name": name}, sleep=sleep)
    snapshot = build_snapshot(repo, data["repository"], polled_at)
    events: list[dict[str, Any]] = []
    for conn in CONNECTIONS:
        items = fetch_connection(session, repo, conn, window.since, sleep=sleep)
        events.extend(to_events(conn, items, repo, snapshot["repo_id"], window, polled_at))
    return events, snapshot


def poll(
    repos: list[str],
    session: requests.Session,
    window: Window,
    polled_at: datetime,
    *,
    sleep: Sleep = time.sleep,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """Fetch every repo. One bad repo doesn't stop the rest. Returns (events, snapshots, failures).

    A repo that fails part-way contributes nothing: rerunning the same window fills it in.
    """
    events: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []
    failures: list[str] = []
    for repo in repos:
        try:
            repo_events, snapshot = collect_repo(session, repo, window, polled_at, sleep=sleep)
        except (FetchError, KeyError, TypeError, ValueError) as exc:
            failures.append(f"{repo}: {exc}")
            continue
        events.extend(repo_events)
        snapshots.append(snapshot)
    # Event-time order across all repos, so silver's watermark advances smoothly (D006).
    events.sort(key=lambda e: (e["event_ts"], e["source_event_id"]))
    return events, snapshots, failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="print records, don't produce")
    parser.add_argument("--date", type=date.fromisoformat, help="last UTC day (default: yesterday)")
    parser.add_argument("--days", type=int, default=1, help="days to cover, ending at --date")
    parser.add_argument("--overlap-hours", type=int, default=DEFAULT_OVERLAP_HOURS)
    parser.add_argument("--repos", nargs="+", help="limit to these owner/name repos")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    polled_at = datetime.now(UTC)
    window = window_for(
        args.date or polled_at.date() - timedelta(days=1), args.days, args.overlap_hours
    )
    repos = args.repos or sorted(e.github_repo for e in load_registry() if e.github_repo)
    log.info("window %s -> %s, %d repos", window.since, window.until, len(repos))

    if not args.dry_run:
        # Imported here so --dry-run works without Kafka. Set up before fetching, so a missing
        # dependency or credential fails in seconds, not after a long backfill.
        from producers.kafka_io import make_avro_serializer, make_producer, produce_records

        producer = make_producer()

    with requests.Session() as session:
        session.headers["User-Agent"] = USER_AGENT
        session.headers["Authorization"] = f"Bearer {require_env('GITHUB_TOKEN')}"
        events, snapshots, failures = poll(repos, session, window, polled_at)

    counts = Counter((e["repo"], e["event_type"]) for e in events)
    for (repo, event_type), n in sorted(counts.items()):
        log.info("%-40s %-13s %6d", repo, event_type, n)

    if args.dry_run:
        for r in snapshots + events:
            print(json.dumps(r, default=str))
        errors: list[str] = []
    else:
        errors = []
        for topic, records in ((SNAPSHOTS_TOPIC, snapshots), (EVENTS_TOPIC, events)):
            errors += produce_records(
                producer,
                topic,
                records,
                key_field="repo",
                value_serializer=make_avro_serializer(topic),
            )

    for problem in failures + errors:
        log.error(problem)
    log.info(
        "done: %d events, %d snapshots, %d repo failures, %d delivery errors",
        len(events),
        len(snapshots),
        len(failures),
        len(errors),
    )
    return 1 if failures or errors else 0


if __name__ == "__main__":
    sys.exit(main())
