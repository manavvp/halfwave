"""Serving documents: one per entity, built from gold rows (D029).

Pure Python on purpose: rows in (dicts, as Spark's `Row.asDict()` or a test gives them),
documents out. No Spark and no Mongo here, so the whole document shape is unit-tested locally.

A document holds what the website shows for one project: the registry entry, the latest
attention-vs-adoption comparison and its recent label history, each signal's latest windows and
full daily series, and the latest fork reconciliation. Evidence stays in gold for now (D029).
"""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

Row = Mapping[str, Any]
Document = dict[str, Any]

WINDOWS = (7, 14, 30)
WINDOW_FIELDS = ("sum", "avg", "days_with_data", "low_coverage")
LABEL_HISTORY_DAYS = 30
ENTITY_FIELDS = (
    "entity_id",
    "name",
    "entity_type",
    "aliases",
    "github_repo",
    "pypi_package",
    "docker_image",
    "adoption_proxy_notes",
)


def _day(value: dt.date | str) -> str:
    """A date as 'YYYY-MM-DD', whether it arrives as a date, a datetime or a string."""
    if isinstance(value, dt.date):
        return value.isoformat()[:10]
    return str(value)[:10]


def _signal_block(meta: Row, rows: list[Row]) -> dict[str, Any]:
    """One signal for one entity: what it is, its latest windows, and its daily series."""
    rows = sorted(rows, key=lambda r: _day(r["signal_date"]))
    last = rows[-1]
    return {
        "signal_class": meta["signal_class"],
        "source": meta["source"],
        "kind": meta["kind"],
        "comparable_from": _day(meta["comparable_from"]),
        "description": meta["description"],
        "latest": {
            "date": _day(last["signal_date"]),
            "value": last["value"],
            **{f"{f}_{n}d": last[f"{f}_{n}d"] for n in WINDOWS for f in WINDOW_FIELDS},
        },
        # Days with no data: a missed poll or an unpublished PyPI day. Count signals have none,
        # because a day without events is a real 0 (D026).
        "missing_days": [_day(r["signal_date"]) for r in rows if not r["has_data"]],
        "series": [[_day(r["signal_date"]), r["value"]] for r in rows],
    }


def _comparison(row: Row) -> dict[str, Any]:
    return {
        "date": _day(row["signal_date"]),
        "attention_signal": row["attention_signal"],
        "adoption_signal": row["adoption_signal"],
        "attention": {
            "prior_avg": row["attention_prior_avg"],
            "current_avg": row["attention_current_avg"],
            "growth": row["attention_growth"],
        },
        "adoption": {
            "prior_avg": row["adoption_prior_avg"],
            "current_avg": row["adoption_current_avg"],
            "growth": row["adoption_growth"],
        },
        "label": row["label"],
        "insufficient_reason": row["insufficient_reason"],
    }


def _comparisons(rows: list[Row]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """The latest day's comparisons, and every label in the LABEL_HISTORY_DAYS up to it."""
    if not rows:
        return [], []
    latest = max(_day(r["signal_date"]) for r in rows)
    since = (dt.date.fromisoformat(latest) - dt.timedelta(days=LABEL_HISTORY_DAYS - 1)).isoformat()
    ordered = sorted(rows, key=lambda r: (_day(r["signal_date"]), r["adoption_signal"]))
    current = [_comparison(r) for r in ordered if _day(r["signal_date"]) == latest]
    history = [
        {
            "date": _day(r["signal_date"]),
            "adoption_signal": r["adoption_signal"],
            "label": r["label"],
        }
        for r in ordered
        if _day(r["signal_date"]) >= since
    ]
    return current, history


def _reconciliation(rows: list[Row]) -> dict[str, Any] | None:
    """The latest day the fork events could be compared with GitHub's counter (D028)."""
    compared = [r for r in rows if r["gap_pct_7d"] is not None]
    if not compared:
        return None
    last = max(compared, key=lambda r: _day(r["signal_date"]))
    return {
        "date": _day(last["signal_date"]),
        "fork_events_7d": last["fork_events_7d"],
        "counter_change_7d": last["counter_change_7d"],
        "days_compared_7d": last["days_compared_7d"],
        "gap_pct_7d": last["gap_pct_7d"],
    }


def build_documents(
    entities: Iterable[Row],
    signals: Iterable[Row],
    rolling: Iterable[Row],
    comparisons: Iterable[Row],
    reconciliation: Iterable[Row],
) -> list[Document]:
    """One document per entity, sorted by entity_id. `_id` is the entity_id.

    Inputs are rows of dim_entity, dim_signal, entity_signal_rolling,
    entity_attention_vs_adoption and fork_reconciliation. Deterministic: the same gold gives
    the same documents, which is what makes the sync idempotent.
    """
    meta = {s["signal"]: s for s in signals}
    by_entity: dict[str, dict[str, list[Row]]] = defaultdict(lambda: defaultdict(list))
    for r in rolling:
        by_entity[r["entity_id"]][r["signal"]].append(r)
    comps: dict[str, list[Row]] = defaultdict(list)
    for r in comparisons:
        comps[r["entity_id"]].append(r)
    recon: dict[str, list[Row]] = defaultdict(list)
    for r in reconciliation:
        recon[r["entity_id"]].append(r)

    docs = []
    for e in sorted(entities, key=lambda e: e["entity_id"]):
        eid = e["entity_id"]
        blocks = {
            signal: _signal_block(meta[signal], rows)
            for signal, rows in sorted(by_entity[eid].items())
        }
        current, history = _comparisons(comps[eid])
        docs.append(
            {
                "_id": eid,
                **{f: (list(e[f]) if f == "aliases" else e[f]) for f in ENTITY_FIELDS},
                # The newest day gold has for any of this entity's signals.
                "as_of": max((b["latest"]["date"] for b in blocks.values()), default=None),
                "comparisons": current,
                "label_history": history,
                "signals": blocks,
                "reconciliation": _reconciliation(recon[eid]),
            }
        )
    return docs
