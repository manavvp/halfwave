"""Silver failure drills: duplicates, a late event, and a poison pill (D020-D023).

Runs locally against scratch tables, never production:
  - bronze: empty radar.bronze.<table>_drill copies of every bronze table
  - pipeline: radar-silver-drill (schema radar.silver_drill, radar.bronze_suffix = _drill)

Each "run" inserts rows into radar.bronze.github_events_v1_drill exactly as bronze stores them
(Confluent-framed Avro from the producers' own serializer), runs one pipeline update, and checks
silver. The story mirrors two daily producer runs plus trouble:

  1  run 1: forks A (Nov 7 20:00) and B (Nov 7 23:00)
  2  run 2: the 6 h overlap re-fetches B, plus C (Nov 8 10:00)        -> B once in silver
  3  run 3: D dated Nov 6 15:00, behind the watermark, plus E (Nov 8 23:00)
                                                                     -> D dropped, Nov 7 closed at 2
  4  run 4: garbage bytes, plus a fork of a repo not in the registry -> both quarantined,
                                                                        the pipeline keeps going

    set -a; source .env; set +a
    python drills/silver_drills.py                        # uses the prod registry table
    python drills/silver_drills.py --registry-table radar.silver_dev.entity_registry \\
        --repo-root /Workspace/Users/<you>/radar_dev/halfwave

Needs the databricks CLI logged in (profile DEFAULT) and Schema Registry credentials in env.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402

from producers.kafka_io import make_avro_serializer  # noqa: E402
from transforms import silver  # noqa: E402

GIT_FOLDER = "/Workspace/Users/manav.specticoid@gmail.com/halfwave/halfwave"
PIPELINE = "radar-silver-drill"
DRILL_SCHEMA = "silver_drill"
SUFFIX = "_drill"
TOPIC = "radar.github.events.v1"
EVENTS_DRILL = f"radar.bronze.github_events_v1{SUFFIX}"
REPO, REPO_ID = "vllm-project/vllm", 599547518


# --- Databricks via the CLI ----------------------------------------------------------------------


def api(method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    cmd = ["databricks", "api", method, path]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(f"{method} {path} failed: {out.stderr.strip()[-500:]}")
    return json.loads(out.stdout or "{}")


def sql(warehouse: str, statement: str) -> list[list[Any]]:
    r = api("post", "/api/2.0/sql/statements",
            {"warehouse_id": warehouse, "statement": statement, "wait_timeout": "50s"})
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = api("get", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r["status"]["state"] != "SUCCEEDED":
        error = r["status"].get("error", {}).get("message")
        raise RuntimeError(f"SQL failed: {error}\n{statement}")
    return (r.get("result") or {}).get("data_array") or []


def ensure_pipeline(repo_root: str, registry_table: str) -> str:
    settings = {
        "name": PIPELINE,
        "serverless": True,
        "catalog": "radar",
        "schema": DRILL_SCHEMA,
        "development": True,
        "continuous": False,
        "channel": "CURRENT",
        "root_path": repo_root,
        "libraries": [{"file": {"path": f"{repo_root}/databricks/silver/pipeline.py"}}],
        "configuration": {"radar.registry_table": registry_table, "radar.bronze_suffix": SUFFIX},
    }
    found = api("get", "/api/2.0/pipelines", None).get("statuses", [])
    existing = [p["pipeline_id"] for p in found if p["name"] == PIPELINE]
    if existing:
        api("put", f"/api/2.0/pipelines/{existing[0]}", {**settings, "id": existing[0]})
        return existing[0]
    return api("post", "/api/2.0/pipelines", settings)["pipeline_id"]


def run_update(pipeline_id: str, *, full_refresh: bool = False) -> str:
    update = api("post", f"/api/2.0/pipelines/{pipeline_id}/updates",
                 {"full_refresh": full_refresh})["update_id"]
    while True:
        state = api("get", f"/api/2.0/pipelines/{pipeline_id}/updates/{update}")["update"]["state"]
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            return state
        time.sleep(10)


# --- bronze rows as the bronze job writes them ---------------------------------------------------


class DrillBronze:
    def __init__(self, warehouse: str) -> None:
        self.warehouse = warehouse
        self.serialize = make_avro_serializer(TOPIC)
        self.offset = 0

    def fork(self, oid: int, at: str, repo: str = REPO) -> bytes:
        record = {
            "source_event_id": f"fork:{REPO_ID}:{oid}",
            "event_type": "fork",
            "repo": repo,
            "repo_id": REPO_ID,
            "event_ts": datetime.fromisoformat(at).replace(tzinfo=UTC),
            "actor": "drill",
            "object_id": oid,
            "number": None,
            "url": f"https://github.com/drill/fork-{oid}",
            "fetched_at": datetime.now(UTC),
        }
        return self.serialize(record, SerializationContext(TOPIC, MessageField.VALUE))

    def insert(self, run: int, values: list[bytes]) -> None:
        rows = []
        for value in values:
            schema_id = int.from_bytes(value[1:5], "big") if value[:1] == b"\x00" else "NULL"
            rows.append(
                f"('{TOPIC}', 0, {self.offset}, current_timestamp(), '{REPO}', "
                f"unhex('{value.hex()}'), {schema_id}, current_timestamp(), {run})"
            )
            self.offset += 1
        sql(self.warehouse, f"INSERT INTO {EVENTS_DRILL} VALUES {', '.join(rows)}")


# --- checks --------------------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--registry-table", default=silver.REGISTRY_TABLE)
    parser.add_argument("--repo-root", default=GIT_FOLDER)
    parser.add_argument("--warehouse-id", help="default: the first SQL warehouse")
    parser.add_argument("--keep", action="store_true", help="keep the drill bronze tables")
    args = parser.parse_args(argv)

    warehouse = args.warehouse_id or api("get", "/api/2.0/sql/warehouses")["warehouses"][0]["id"]
    results: list[tuple[str, str, str, bool]] = []

    def check(name: str, expected: object, observed: object) -> None:
        results.append((name, str(expected), str(observed), expected == observed))
        print(f"{'PASS' if expected == observed else 'FAIL'}  {name}: "
              f"expected {expected}, observed {observed}")

    def event_ids() -> list[str]:
        rows = sql(warehouse, f"SELECT event_id FROM radar.{DRILL_SCHEMA}.events ORDER BY 1")
        return [r[0].rsplit(":", 1)[-1] for r in rows]  # github:fork:<repo_id>:<object_id>

    letters = {101: "A", 102: "B", 103: "C", 104: "D", 105: "E", 106: "F"}

    def letters_in_silver() -> str:
        return "".join(sorted(letters[int(i)] for i in event_ids()))

    print("setup: empty drill bronze tables, drill pipeline, full refresh")
    for src in silver.SOURCES:
        sql(warehouse, f"DROP TABLE IF EXISTS {src.bronze_table}{SUFFIX}")
        sql(warehouse, f"CREATE TABLE {src.bronze_table}{SUFFIX} LIKE {src.bronze_table}")
    pipeline_id = ensure_pipeline(args.repo_root, args.registry_table)
    check("setup update", "COMPLETED", run_update(pipeline_id, full_refresh=True))

    bronze = DrillBronze(warehouse)
    a, b = bronze.fork(101, "2026-11-07T20:00:00"), bronze.fork(102, "2026-11-07T23:00:00")
    bronze.insert(1, [a, b])
    check("run 1 update", "COMPLETED", run_update(pipeline_id))
    check("run 1: A and B in silver", "AB", letters_in_silver())

    print("drill 1: duplicate replay (the overlap re-fetches B)")
    bronze.insert(2, [b, bronze.fork(103, "2026-11-08T10:00:00")])
    check("run 2 update", "COMPLETED", run_update(pipeline_id))
    check("run 2: bronze keeps both copies of B", 4,
          int(sql(warehouse, f"SELECT count(*) FROM {EVENTS_DRILL}")[0][0]))
    check("run 2: B once in silver", "ABC", letters_in_silver())

    print("drill 2: a late event (D, Nov 6, behind the 12 h watermark)")
    bronze.insert(3, [bronze.fork(104, "2026-11-06T15:00:00"),
                      bronze.fork(105, "2026-11-08T23:00:00")])
    check("run 3 update", "COMPLETED", run_update(pipeline_id))
    check("run 3: D dropped as late, E kept", "ABCE", letters_in_silver())
    daily = sql(warehouse, f"SELECT string(day), event_count FROM radar.{DRILL_SCHEMA}"
                           ".github_events_daily ORDER BY 1")
    check("run 3: Nov 7 closed with 2, no Nov 6 row", [["2026-11-07", "2"]], daily)

    print("drill 3: poison pill (garbage bytes) and an unknown repo")
    garbage = a[:5] + b"\xff\xfe not avro"  # valid header and schema id, corrupt body
    bronze.insert(4, [garbage, bronze.fork(106, "2026-11-08T23:30:00", repo="someone/unknown")])
    check("run 4 update (pipeline keeps going)", "COMPLETED", run_update(pipeline_id))
    check("run 4: silver unchanged", "ABCE", letters_in_silver())
    quarantine = sql(warehouse, f"SELECT reason, bronze_ref.offset FROM radar.{DRILL_SCHEMA}"
                                ".quarantine ORDER BY 2")
    check("run 4: both quarantined with their bronze offsets",
          [["decode_failed", "6"], ["unresolved_entity", "7"]], quarantine)

    if not args.keep:
        for src in silver.SOURCES:
            sql(warehouse, f"DROP TABLE IF EXISTS {src.bronze_table}{SUFFIX}")
    passed = sum(r[3] for r in results)
    print(f"\n{passed}/{len(results)} checks passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
