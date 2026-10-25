# Databricks notebook source
# MAGIC %md
# MAGIC # Bootstrap: put the repo root on `sys.path`
# MAGIC Run this from any notebook in the Git folder with `%run` (relative path), so `producers` and
# MAGIC `transforms` import directly. `%run` executes this in the caller's context.

# COMMAND ----------

import os
import sys


def find_repo_root() -> str:
    """Walk upward until we find the repo (pyproject.toml next to producers/).

    Starts from the notebook's own workspace path, which survives restartPython(), and falls back
    to the working directory, which can change after a Python restart.
    """
    starts = []
    try:
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        nb_path = context.notebookPath().get()
        if not nb_path.startswith("/Workspace"):
            nb_path = "/Workspace" + nb_path
        starts.append(os.path.dirname(nb_path))
    except Exception as exc:  # context API unavailable: fall back to cwd only
        print(f"notebook path lookup failed: {exc}")
    starts.append(os.getcwd())

    for start in starts:
        d = start
        while d and d != "/":
            if os.path.isfile(os.path.join(d, "pyproject.toml")) and os.path.isdir(
                os.path.join(d, "producers")
            ):
                return d
            d = os.path.dirname(d)
    raise RuntimeError(
        f"Couldn't find the repo root, searched upward from {starts}. Is this notebook inside "
        "the halfwave Git folder, and has the Git folder been pulled since the last push?"
    )


REPO_ROOT = find_repo_root()
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
print(f"repo root: {REPO_ROOT}")
