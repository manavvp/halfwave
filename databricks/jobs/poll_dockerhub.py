# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Docker Hub poll
# MAGIC Thin wrapper for the scheduled job (D008). Copies secrets from scope `radar` into env vars,
# MAGIC then runs the same producer we run locally.

# COMMAND ----------

# MAGIC %pip install "confluent-kafka[avro,schemaregistry]>=2.5" "pyyaml>=6.0" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

import os

SECRET_SCOPE = "radar"
SETTINGS = [
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_API_KEY",
    "KAFKA_API_SECRET",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET",
]
for name in SETTINGS:
    os.environ[name] = dbutils.secrets.get(SECRET_SCOPE, name.lower().replace("_", "-"))

# COMMAND ----------

from producers.sources import dockerhub

exit_code = dockerhub.main([])
if exit_code != 0:
    # Fail the job run so the failure is visible in the Jobs UI (and alerts, if configured).
    raise RuntimeError(f"Docker Hub poll finished with errors (exit code {exit_code})")
