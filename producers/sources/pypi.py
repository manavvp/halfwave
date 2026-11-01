"""Daily PyPI poller: daily download counts for registry packages, from pypistats.org.

One request per package returns every day pypistats still holds (about 180 days), one row per
day. Each run keeps the rows for --days whole UTC days ending at --date (default: yesterday)
and produces them in date order. Like the GitHub producer it keeps no state: a rerun produces
the same rows again and silver drops them as duplicates (D016, D019).

Counts exclude mirrors (bandersnatch and friends re-downloading everything). PyPI stopped
counting installer metadata requests on 2026-08-24 without restating history, so counts before
and after that date are not comparable. Gold handles the break; this producer stays raw (D019).

    python -m producers.sources.pypi --dry-run           # last 3 days, print only
    python -m producers.sources.pypi                     # last 3 days -> Kafka
    python -m producers.sources.pypi --days 180          # backfill everything pypistats has
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import Counter
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any

import requests

from producers.registry import load_registry

log = logging.getLogger(__name__)

TOPIC = "radar.pypi.downloads.v1"
API_URL = "https://pypistats.org/api/packages/{package}/overall?mirrors=false"
USER_AGENT = "halfwave-radar/0.1 (+https://github.com/manavvp/halfwave)"
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
# pypistats publishes a day some hours after it ends. Re-reading the last 3 days means a day
# that wasn't out yet on one run is picked up by the next. Must stay below silver's watermark.
DEFAULT_DAYS = 3
# pypistats returned 429 even at 1 request/s during setup (runbook), so go slowly.
DEFAULT_PACE_S = 3.0

Sleep = Callable[[float], None]


class FetchError(RuntimeError):
    """A package could not be fetched."""


def date_window(day: date, days: int = DEFAULT_DAYS) -> tuple[date, date]:
    """The inclusive range of `days` UTC dates ending with `day`."""
    if days < 1:
        raise ValueError("days must be >= 1")
    return day - timedelta(days=days - 1), day


def fetch_overall(
    session: requests.Session,
    package: str,
    *,
    max_attempts: int = 5,
    base_delay_s: float = 5.0,
    sleep: Sleep = time.sleep,
) -> dict[str, Any]:
    """GET one package's daily downloads, retrying rate limits, server errors and network failures.

    Anything else (e.g. 404 for a package pypistats doesn't know) fails immediately.
    """
    url = API_URL.format(package=package)
    error = ""
    for attempt in range(1, max_attempts + 1):
        delay = base_delay_s * 2 ** (attempt - 1)
        try:
            resp = session.get(url, timeout=30)
        except requests.RequestException as exc:
            error = f"{type(exc).__name__}: {exc}"
        else:
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code not in RETRYABLE_STATUSES:
                raise FetchError(f"{package}: HTTP {resp.status_code} (not retryable)")
            error = f"HTTP {resp.status_code}"
            retry_after = resp.headers.get("Retry-After", "")
            if retry_after.isdigit():
                delay = max(delay, float(retry_after))
        if attempt < max_attempts:
            log.warning("%s: %s, retrying in %.0fs", package, error, delay)
            sleep(delay)
    raise FetchError(f"{package}: gave up after {max_attempts} attempts ({error})")


def to_records(
    package: str, payload: dict[str, Any], first: date, last: date, polled_at: datetime
) -> list[dict[str, Any]]:
    """Map a pypistats /overall response to radar.pypi.downloads.v1 records within [first, last]."""
    if polled_at.tzinfo is None:
        raise ValueError("polled_at must be timezone-aware")
    records = []
    for row in payload["data"]:
        day = date.fromisoformat(row["date"])
        if not first <= day <= last:
            continue
        category = row["category"]
        records.append(
            {
                # The package, the day and the category identify the fact. A re-fetch gives the
                # same id and silver drops it (D004).
                "source_event_id": f"{package}:{day.isoformat()}:{category}",
                "package": package,
                "download_date": day,
                "category": category,
                "downloads": int(row["downloads"]),
                "polled_at": polled_at.astimezone(UTC),
            }
        )
    return records


def poll(
    packages: list[str],
    session: requests.Session,
    first: date,
    last: date,
    polled_at: datetime,
    *,
    pace_s: float = DEFAULT_PACE_S,
    sleep: Sleep = time.sleep,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Fetch every package. One bad package doesn't stop the rest. Returns (records, failures)."""
    records: list[dict[str, Any]] = []
    failures: list[str] = []
    for i, package in enumerate(packages):
        if i:
            sleep(pace_s)
        try:
            payload = fetch_overall(session, package, sleep=sleep)
            records.extend(to_records(package, payload, first, last, polled_at))
        except (FetchError, KeyError, TypeError, ValueError) as exc:
            failures.append(f"{package}: {exc}")
    # Event-time order, so silver's watermark advances smoothly (D006).
    records.sort(key=lambda r: (r["download_date"], r["package"]))
    return records, failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="print records, don't produce")
    parser.add_argument("--date", type=date.fromisoformat, help="last UTC day (default: yesterday)")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="days ending at --date")
    parser.add_argument("--packages", nargs="+", help="limit to these PyPI packages")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    polled_at = datetime.now(UTC)
    first, last = date_window(args.date or polled_at.date() - timedelta(days=1), args.days)
    packages = args.packages or sorted(e.pypi_package for e in load_registry() if e.pypi_package)
    log.info("dates %s -> %s, %d packages", first, last, len(packages))

    if not args.dry_run:
        # Imported here so --dry-run works without Kafka. Set up before fetching, so a missing
        # dependency or credential fails before the slow, paced fetch.
        from producers.kafka_io import make_avro_serializer, make_producer, produce_records

        producer = make_producer()

    with requests.Session() as session:
        session.headers["User-Agent"] = USER_AGENT
        records, failures = poll(packages, session, first, last, polled_at)

    counts = Counter(r["package"] for r in records)
    for package in packages:
        log.info("%-20s %4d days", package, counts[package])  # 0 here means pypistats lags

    if args.dry_run:
        for r in records:
            print(json.dumps(r, default=str))
        errors: list[str] = []
    else:
        errors = produce_records(
            producer,
            TOPIC,
            records,
            key_field="package",
            value_serializer=make_avro_serializer(TOPIC),
        )

    for problem in failures + errors:
        log.error(problem)
    log.info(
        "done: %d rows, %d fetch failures, %d delivery errors",
        len(records),
        len(failures),
        len(errors),
    )
    return 1 if failures or errors else 0


if __name__ == "__main__":
    sys.exit(main())
