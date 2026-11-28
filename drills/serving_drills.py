# Databricks notebook source
# MAGIC %md
# MAGIC # Serving drills (D029)
# MAGIC Run on Databricks against scratch database `radar_drill`, never `radar`. Reads the real
# MAGIC gold marts, so the documents are the ones production would write.
# MAGIC
# MAGIC | Drill | Expected |
# MAGIC |---|---|
# MAGIC | 1. Sync twice | identical documents (apart from `synced_at`), one per registry entity |
# MAGIC | 2. A stale field and a departed entity | the field is gone, the departed doc is deleted |
# MAGIC | 3. Empty or partial gold | the guard refuses and the collection is untouched |

# COMMAND ----------

# MAGIC %pip install "pymongo>=4.8" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../databricks/_bootstrap

# COMMAND ----------

import json
import os

os.environ["MONGODB_URI"] = dbutils.secrets.get("radar", "mongodb-uri")

from serving import documents, sync_mongo

DRILL_DB = "radar_drill"
GOLD = "radar.gold"


def rows(table: str) -> list[dict]:
    return [r.asDict(recursive=True) for r in spark.read.table(table).collect()]


docs = documents.build_documents(
    entities=rows(f"{GOLD}.dim_entity"),
    signals=rows(f"{GOLD}.dim_signal"),
    rolling=rows(f"{GOLD}.entity_signal_rolling"),
    comparisons=rows(f"{GOLD}.entity_attention_vs_adoption"),
    reconciliation=rows(f"{GOLD}.fork_reconciliation"),
)
expected = [r.entity_id for r in spark.read.table("radar.silver.entity_registry").collect()]

client = sync_mongo.connect()
coll = client[DRILL_DB][sync_mongo.COLLECTION]
# Atlas's readWrite role can drop a collection but not a database; an empty database vanishes.
coll.drop()
results: list[dict] = []


def check(name: str, want: object, got: object) -> None:
    results.append({"check": name, "expected": want, "observed": got, "pass": want == got})
    print(f"{'PASS' if want == got else 'FAIL'}  {name}: expected {want}, observed {got}")


def stored() -> dict[str, dict]:
    return {d["_id"]: {k: v for k, v in d.items() if k != "synced_at"} for d in coll.find()}


# COMMAND ----------

# Drill 1: sync twice
first = sync_mongo.sync(coll, docs, expected)
after_first = stored()
second = sync_mongo.sync(coll, docs, expected)
after_second = stored()
check("first sync inserts one document per entity", len(expected), first["upserted"])
check("second sync replaces, inserts nothing", [len(expected), 0],
      [second["matched"], second["upserted"]])
check("documents are identical after both syncs", True, after_first == after_second)
check("stored documents equal the built ones", True,
      after_second == {d["_id"]: d for d in docs})

# COMMAND ----------

# Drill 2: a stale field and an entity that left the registry
coll.update_one({"_id": "vllm"}, {"$set": {"stale_field": "from an old version"}})
coll.insert_one({"_id": "ray-serve", "name": "Ray Serve"})
third = sync_mongo.sync(coll, docs, expected)
check("the departed entity's document is deleted", 1, third["deleted"])
check("no document outside the registry remains", sorted(expected), sorted(stored()))
check("the stale field is gone", False, "stale_field" in coll.find_one({"_id": "vllm"}))

# COMMAND ----------

# Drill 3: empty and partial gold
before = stored()
refused = []
for label, attempt in [("empty", []), ("partial", docs[:-1])]:
    try:
        sync_mongo.sync(coll, attempt, expected)
        refused.append(f"{label}: written")
    except sync_mongo.SyncRefused:
        refused.append(f"{label}: refused")
check("the guard refuses empty and partial gold", ["empty: refused", "partial: refused"], refused)
check("the collection is untouched", True, stored() == before)

# COMMAND ----------

coll.drop()
passed = sum(r["pass"] for r in results)
print(f"\n{passed}/{len(results)} checks passed")
dbutils.notebook.exit(json.dumps({"passed": passed, "total": len(results), "results": results}))
