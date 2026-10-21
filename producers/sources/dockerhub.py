"""Daily Docker Hub poller: snapshot cumulative pull counts for registry images into Kafka.

Docker Hub only exposes the *current* all-time pull count, so history exists only from our
first poll. Each run emits one snapshot per image. Daily deltas are computed later in dbt.

    python -m producers.sources.dockerhub --dry-run   # fetch and print, don't produce
    python -m producers.sources.dockerhub             # fetch and produce to Kafka
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import requests

from producers.registry import load_registry

log = logging.getLogger(__name__)

TOPIC = "radar.dockerhub.snapshots.v1"
API_URL = "https://hub.docker.com/v2/repositories/{image}/"
USER_AGENT = "halfwave-radar/0.1 (+https://github.com/manavvp/halfwave)"
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


class FetchError(RuntimeError):
    """An image could not be fetched."""


def fetch_repository(
    session: requests.Session,
    image: str,
    *,
    max_attempts: int = 4,
    base_delay_s: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """GET one repository, retrying rate limits, server errors and network failures.

    Anything else (e.g. 404) fails immediately: that's a registry bug, not a transient blip,
    and retrying would only hide it.
    """
    url = API_URL.format(image=image)
    error = ""
    for attempt in range(1, max_attempts + 1):
        delay = base_delay_s * 2 ** (attempt - 1)
        try:
            resp = session.get(url, timeout=15)
        except requests.RequestException as exc:
            error = f"{type(exc).__name__}: {exc}"
        else:
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code not in RETRYABLE_STATUSES:
                raise FetchError(f"{image}: HTTP {resp.status_code} (not retryable)")
            error = f"HTTP {resp.status_code}"
            retry_after = resp.headers.get("Retry-After", "")
            if retry_after.isdigit():
                delay = max(delay, float(retry_after))
        if attempt < max_attempts:
            log.warning("%s: %s, retrying in %.0fs", image, error, delay)
            sleep(delay)
    raise FetchError(f"{image}: gave up after {max_attempts} attempts ({error})")


def build_snapshot(image: str, payload: dict[str, Any], polled_at: datetime) -> dict[str, Any]:
    """Map an API response to a radar.dockerhub.snapshots.v1 record."""
    if polled_at.tzinfo is None:
        raise ValueError("polled_at must be timezone-aware")
    polled_at = polled_at.astimezone(UTC)
    snapshot_date = polled_at.date()
    last_updated = payload.get("last_updated")
    return {
        # Derived from the image and the UTC day, never the poll time, so a same-day re-poll
        # produces the same id and is dropped as a duplicate in silver (D004).
        "source_event_id": f"{image}:{snapshot_date.isoformat()}",
        "image": image,
        "pull_count": int(payload["pull_count"]),
        "star_count": int(payload.get("star_count") or 0),
        "last_updated": datetime.fromisoformat(last_updated) if last_updated else None,
        "snapshot_date": snapshot_date,
        "polled_at": polled_at,
    }


def poll(
    images: list[str],
    session: requests.Session,
    polled_at: datetime,
    *,
    pace_s: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Fetch every image. One bad image doesn't stop the rest. Returns (records, failures)."""
    records: list[dict[str, Any]] = []
    failures: list[str] = []
    for i, image in enumerate(images):
        if i:
            sleep(pace_s)  # Docker Hub rate-limits bursts (seen during setup)
        try:
            records.append(build_snapshot(image, fetch_repository(session, image), polled_at))
        except (FetchError, KeyError, ValueError) as exc:
            failures.append(f"{image}: {exc}")
    return records, failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="print records, don't produce")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    images = sorted(e.docker_image for e in load_registry() if e.docker_image)
    log.info("polling %d images", len(images))
    with requests.Session() as session:
        session.headers["User-Agent"] = USER_AGENT
        records, failures = poll(images, session, datetime.now(UTC))

    if args.dry_run:
        for r in records:
            print(json.dumps(r, default=str))
        errors: list[str] = []
    else:
        # Imported here so --dry-run works without Kafka credentials.
        from producers.kafka_io import make_avro_serializer, make_producer, produce_records

        errors = produce_records(
            make_producer(),
            TOPIC,
            records,
            key_field="image",
            value_serializer=make_avro_serializer(TOPIC),
        )

    for problem in failures + errors:
        log.error(problem)
    log.info(
        "done: %d fetched, %d fetch failures, %d delivery errors",
        len(records),
        len(failures),
        len(errors),
    )
    return 1 if failures or errors else 0


if __name__ == "__main__":
    sys.exit(main())
