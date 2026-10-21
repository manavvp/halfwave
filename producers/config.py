"""Configuration comes from environment variables only.

Locally they're loaded from `.env` by the shell. On Databricks, the job's notebook wrapper copies
them from secret scope `radar` into the environment before calling the producer, so this code
never needs to know where it's running.
"""

from __future__ import annotations

import os


class ConfigError(RuntimeError):
    """A required setting is missing."""


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"missing environment variable {name} (see .env.example)")
    return value
