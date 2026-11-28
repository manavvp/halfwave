# Databricks notebook source
# MAGIC %md
# MAGIC # Sync gold to MongoDB Atlas (D029)
# MAGIC Job task after `gold`, which must have succeeded. Reads the gold marts, builds one document
# MAGIC per entity (`serving/documents.py`) and replaces them by `_id` in `radar.entities`
# MAGIC (`serving/sync_mongo.py`). Refuses to write unless gold has exactly the registry's entities.

# COMMAND ----------

# MAGIC %pip install "pymongo>=4.8" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("gold_schema", "gold")
dbutils.widgets.text("database", "radar")  # drills write to radar_drill

import json
import os

os.environ["MONGODB_URI"] = dbutils.secrets.get("radar", "mongodb-uri")

from serving import documents, sync_mongo

GOLD = f"radar.{dbutils.widgets.get('gold_schema').strip()}"
DATABASE = dbutils.widgets.get("database").strip()


def rows(table: str) -> list[dict]:
    return [r.asDict(recursive=True) for r in spark.read.table(table).collect()]


docs = documents.build_documents(
    entities=rows(f"{GOLD}.dim_entity"),
    signals=rows(f"{GOLD}.dim_signal"),
    rolling=rows(f"{GOLD}.entity_signal_rolling"),
    comparisons=rows(f"{GOLD}.entity_attention_vs_adoption"),
    reconciliation=rows(f"{GOLD}.fork_reconciliation"),
)
# The registry the pipeline ran with is the yardstick for "gold is complete".
expected = [r.entity_id for r in spark.read.table("radar.silver.entity_registry").collect()]

client = sync_mongo.connect()
result = sync_mongo.sync(client[DATABASE][sync_mongo.COLLECTION], docs, expected)
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
