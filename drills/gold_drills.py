"""Gold failure drills: a duplicate past silver, a missed poll, a poison pill (D003, D021, D026).

Runs locally against scratch copies, never production:
  - silver: radar.silver_drill, a copy of the four silver tables
  - gold:   radar.gold_drill (dbt target `drill`, var silver_schema = silver_drill)

  0  baseline: the copy builds clean                    -> every model and test passes
  1  a duplicate event_id slips past silver              -> gold's unique test catches it (D003)
  2  a snapshot day goes missing (as on Nov 7 and 18-19) -> no delta for that day or the day
                                                            before, the gap test warns, evidence
                                                            still matches every row (D026)
  3  a decode failure lands in quarantine                -> the quarantine alarm fails the build

    python drills/gold_drills.py [--keep]

Needs the databricks CLI logged in (profile DEFAULT) and DBT_DATABRICKS_HOST/HTTP_PATH in env.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from drills.silver_drills import api, sql  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
DBT_DIR = REPO / "dbt"
DBT = REPO / ".venv" / "bin" / "dbt"
SILVER, DRILL_SILVER, DRILL_GOLD = "radar.silver", "radar.silver_drill", "radar.gold_drill"
TABLES = ["events", "github_events_daily", "quarantine", "entity_registry"]
MISSED_DAY = "2026-11-12"


def dbt_build(select: str | None = None) -> dict[str, str]:
    """Run dbt build against the drill copies; return {node name: status}."""
    cmd = [str(DBT), "build", "--target", "drill", "--vars", '{"silver_schema": "silver_drill"}']
    if select:
        cmd += ["--select", select]
    subprocess.run(cmd, cwd=DBT_DIR, capture_output=True, text=True)
    results = json.loads((DBT_DIR / "target" / "run_results.json").read_text())["results"]
    return {r["unique_id"].split(".")[2]: r["status"] for r in results}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--warehouse-id", help="default: the first SQL warehouse")
    parser.add_argument("--keep", action="store_true", help="keep the drill schemas")
    args = parser.parse_args(argv)

    token = json.loads(subprocess.run(
        ["databricks", "auth", "token", "--profile", "DEFAULT"],
        capture_output=True, text=True, check=True).stdout)["access_token"]
    os.environ["DBT_DATABRICKS_TOKEN"] = token
    warehouse = args.warehouse_id or api("get", "/api/2.0/sql/warehouses")["warehouses"][0]["id"]
    results: list[bool] = []

    def check(name: str, expected: object, observed: object) -> None:
        results.append(expected == observed)
        print(f"{'PASS' if expected == observed else 'FAIL'}  {name}: "
              f"expected {expected}, observed {observed}")

    def reset_silver_copy() -> None:
        sql(warehouse, f"CREATE SCHEMA IF NOT EXISTS {DRILL_SILVER}")
        for table in TABLES:
            sql(warehouse, f"CREATE OR REPLACE TABLE {DRILL_SILVER}.{table} "
                           f"AS SELECT * FROM {SILVER}.{table}")

    def count(query: str) -> int:
        return int(sql(warehouse, query)[0][0])

    print("baseline: a copy of silver, built into gold_drill")
    reset_silver_copy()
    status = dbt_build()
    check("baseline: nothing fails or warns", [],
          sorted(n for n, s in status.items() if s not in ("success", "pass")))

    print("drill 1: a duplicate event_id slips past silver's dedup")
    sql(warehouse, f"INSERT INTO {DRILL_SILVER}.events SELECT * FROM {SILVER}.events "
                   "WHERE event_type = 'fork' ORDER BY event_id LIMIT 1")
    status = dbt_build("stg_events")
    check("gold's unique test on event_id catches it", "fail",
          status.get("unique_stg_events_event_id"))

    print(f"drill 2: the snapshot poll of {MISSED_DAY} never happened")
    reset_silver_copy()
    sql(warehouse, f"DELETE FROM {DRILL_SILVER}.events WHERE event_type IN "
                   f"('stars_total', 'forks_total', 'pulls_total') "
                   f"AND to_date(event_ts) = '{MISSED_DAY}'")
    status = dbt_build()
    check("the gap test warns", "warn", status.get("no_new_snapshot_gaps"))
    stars_on = (f"SELECT count(*) FROM {DRILL_GOLD}.fct_entity_signal_daily "
                "WHERE signal = 'stars_net' AND signal_date = ")
    check("no stars delta for the day before or the missed day", [0, 0],
          [count(stars_on + "date_sub(date'2026-11-12', 1)"), count(stars_on + "date'2026-11-12'")])
    check("the neighbouring days keep theirs", [25, 25],
          [count(stars_on + "date'2026-11-10'"), count(stars_on + "date'2026-11-13'")])
    check("evidence still matches every signal row", "pass",
          status.get("evidence_matches_signal"))

    print("drill 3: a decode failure lands in quarantine")
    sql(warehouse, f"INSERT INTO {DRILL_SILVER}.quarantine VALUES ('github', 'decode_failed', "
                   "named_struct('topic', 'radar.github.events.v1', 'partition', 0, 'offset', -1), "
                   "'/w== not avro', current_timestamp())")
    status = dbt_build("stg_quarantine")
    check("the quarantine alarm fails the build", "fail", status.get("quarantine_volume"))

    if not args.keep:
        for schema in (DRILL_SILVER, DRILL_GOLD):
            sql(warehouse, f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    print(f"\n{sum(results)}/{len(results)} checks passed")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
