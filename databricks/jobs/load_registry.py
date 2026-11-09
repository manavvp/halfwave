# Databricks notebook source
# MAGIC %md
# MAGIC # Load `config/entities.yaml` into `radar.silver.entity_registry` (D022)
# MAGIC
# MAGIC Runs as a job task before the silver pipeline, which resolves entities against this table.
# MAGIC The YAML stays the source of truth; this table is a loaded copy. It's only rewritten when
# MAGIC the registry changed, so each Delta version of the table is a real registry change.

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("schema", "silver")  # dev runs load into another schema

from pyspark.sql import functions as F

from producers.registry import load_registry
from transforms import silver

TABLE = f"{silver.CATALOG}.{dbutils.widgets.get('schema').strip()}.entity_registry"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TABLE.rsplit('.', 1)[0]}")

# Validates the YAML (unique ids, aliases and handles) before anything is written.
new = spark.createDataFrame(silver.registry_rows(load_registry()), silver.REGISTRY_SCHEMA)

unchanged = False
if spark.catalog.tableExists(TABLE):
    current = spark.read.table(TABLE).drop("loaded_at")
    unchanged = current.exceptAll(new).isEmpty() and new.exceptAll(current).isEmpty()

if unchanged:
    print(f"{TABLE}: registry unchanged, no new version written")
else:
    (
        new.withColumn("loaded_at", F.current_timestamp())
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABLE)
    )
    print(f"{TABLE}: wrote {new.count()} entities")
