import datetime as dt

import pytest

from serving.documents import build_documents
from serving.sync_mongo import SyncRefused, check_complete, sync

ENTITY = {
    "entity_id": "vllm",
    "name": "vLLM",
    "entity_type": "inference_engine",
    "aliases": ("vllm",),
    "github_repo": "vllm-project/vllm",
    "pypi_package": "vllm",
    "docker_image": "vllm/vllm-openai",
    "adoption_proxy_notes": "Measures the server.",
}
ATTENTION_ONLY = {**ENTITY, "entity_id": "mlc-llm", "pypi_package": None, "docker_image": None}
SIGNALS = [
    {"signal": "fork", "signal_class": "attention", "source": "github", "kind": "count",
     "comparable_from": dt.date(2026, 8, 2), "description": "Forks created that day."},
    {"signal": "stars_net", "signal_class": "attention", "source": "github", "kind": "delta",
     "comparable_from": dt.date(2026, 10, 30), "description": "Change in the star counter."},
]


def rolling(entity, signal, day, value):
    windows = {f"{f}_{n}d": None for n in (7, 14, 30)
               for f in ("sum", "avg", "days_with_data", "low_coverage")}
    return {"entity_id": entity, "signal": signal, "signal_date": dt.date(2026, 11, day),
            "value": value, "has_data": value is not None, **windows, "avg_14d": 1.5}


def comparison(day, adoption_signal, label):
    return {"entity_id": "vllm", "signal_date": dt.date(2026, 11, day),
            "attention_signal": "fork", "adoption_signal": adoption_signal,
            "attention_prior_avg": 10.0, "attention_current_avg": 13.0, "attention_growth": 0.3,
            "adoption_prior_avg": 1000.0, "adoption_current_avg": 1010.0,
            "adoption_growth": 0.01, "label": label, "insufficient_reason": None}


def build(**overrides):
    inputs = {
        "entities": [ENTITY, ATTENTION_ONLY],
        "signals": SIGNALS,
        "rolling": [
            rolling("vllm", "stars_net", 6, None),   # a missed poll
            rolling("vllm", "stars_net", 5, 31),
            rolling("vllm", "fork", 5, 0),
            rolling("vllm", "fork", 6, 4),
            rolling("mlc-llm", "fork", 6, 1),
        ],
        "comparisons": [
            comparison(25, "pypi_downloads", "attention_without_adoption"),
            comparison(25, "docker_pulls", "no_clear_divergence"),
            comparison(26, "pypi_downloads", "no_clear_divergence"),
            comparison(26, "docker_pulls", "both_rising"),
        ],
        "reconciliation": [
            {"entity_id": "vllm", "signal_date": dt.date(2026, 11, 26), "fork_events_7d": 9,
             "counter_change_7d": None, "days_compared_7d": 0, "gap_pct_7d": None},
            {"entity_id": "vllm", "signal_date": dt.date(2026, 11, 25), "fork_events_7d": 295,
             "counter_change_7d": 244, "days_compared_7d": 7, "gap_pct_7d": 0.21},
        ],
    }
    return {d["_id"]: d for d in build_documents(**{**inputs, **overrides})}


def test_one_document_per_entity_keyed_by_entity_id():
    docs = build()
    assert list(docs) == ["mlc-llm", "vllm"]
    assert docs["vllm"]["name"] == "vLLM"
    assert docs["vllm"]["aliases"] == ["vllm"]  # a plain list, whatever Spark hands over


def test_series_are_sorted_and_dates_are_iso_strings():
    stars = build()["vllm"]["signals"]["stars_net"]
    assert stars["series"] == [["2026-11-05", 31], ["2026-11-06", None]]
    assert stars["latest"]["date"] == "2026-11-06"
    assert stars["latest"]["avg_14d"] == 1.5
    assert stars["comparable_from"] == "2026-10-30"


def test_missing_days_are_only_days_without_data():
    signals = build()["vllm"]["signals"]
    assert signals["stars_net"]["missing_days"] == ["2026-11-06"]
    assert signals["fork"]["missing_days"] == []  # a 0 is data, not a gap


def test_latest_comparisons_and_label_history():
    vllm = build()["vllm"]
    assert [(c["date"], c["adoption_signal"], c["label"]) for c in vllm["comparisons"]] == [
        ("2026-11-26", "docker_pulls", "both_rising"),
        ("2026-11-26", "pypi_downloads", "no_clear_divergence"),
    ]
    assert vllm["comparisons"][0]["attention"] == {
        "prior_avg": 10.0, "current_avg": 13.0, "growth": 0.3}
    assert [h["date"] for h in vllm["label_history"]] == ["2026-11-25"] * 2 + ["2026-11-26"] * 2


def test_label_history_keeps_only_the_last_30_days():
    old = comparison(1, "pypi_downloads", "both_falling") | {"signal_date": dt.date(2026, 10, 27)}
    kept = comparison(1, "pypi_downloads", "both_falling") | {"signal_date": dt.date(2026, 10, 28)}
    history = build(comparisons=[old, kept, comparison(26, "pypi_downloads", "both_rising")])[
        "vllm"]["label_history"]
    assert [h["date"] for h in history] == ["2026-10-28", "2026-11-26"]


def test_reconciliation_is_the_latest_compared_day():
    assert build()["vllm"]["reconciliation"]["date"] == "2026-11-25"
    assert build()["vllm"]["reconciliation"]["gap_pct_7d"] == 0.21


def test_attention_only_entity_has_no_comparisons():
    mlc = build()["mlc-llm"]
    assert mlc["comparisons"] == [] and mlc["label_history"] == []
    assert mlc["reconciliation"] is None
    assert mlc["as_of"] == "2026-11-06"


def test_same_input_gives_the_same_documents():
    assert build() == build()


@pytest.mark.parametrize("ids", [[], ["vllm"], ["vllm", "mlc-llm", "extra"]])
def test_guard_refuses_documents_that_dont_match_the_registry(ids):
    docs = list(build().values())
    with pytest.raises(SyncRefused):
        check_complete(docs, ids)


def test_guard_refuses_duplicate_ids():
    doc = build()["vllm"]
    with pytest.raises(SyncRefused):
        check_complete([doc, doc], ["vllm"])


class FakeCollection:
    """Just enough of pymongo's Collection for ReplaceOne + delete_many($nin)."""

    def __init__(self, docs=()):
        self.docs = {d["_id"]: d for d in docs}

    def bulk_write(self, ops, ordered):
        matched = upserted = 0
        for op in ops:
            doc = op._doc
            matched += doc["_id"] in self.docs
            upserted += doc["_id"] not in self.docs
            self.docs[doc["_id"]] = doc

        class Result:
            matched_count, upserted_count = matched, upserted
        return Result()

    def delete_many(self, query):
        gone = [i for i in self.docs if i not in query["_id"]["$nin"]]
        for i in gone:
            del self.docs[i]

        class Result:
            deleted_count = len(gone)
        return Result()


def test_sync_replaces_whole_documents_and_deletes_departed_entities():
    stale = {**build()["vllm"], "stale_field": 1}
    coll = FakeCollection([stale, {"_id": "ghost"}])
    at = dt.datetime(2026, 11, 28, tzinfo=dt.UTC)
    result = sync(coll, list(build().values()), ["vllm", "mlc-llm"], synced_at=at)
    assert result == {"documents": 2, "matched": 1, "upserted": 1, "deleted": 1}
    assert sorted(coll.docs) == ["mlc-llm", "vllm"]
    assert "stale_field" not in coll.docs["vllm"]
    assert coll.docs["vllm"]["synced_at"] == at


def test_refused_sync_writes_nothing():
    coll = FakeCollection([{"_id": "vllm"}])
    with pytest.raises(SyncRefused):
        sync(coll, [], ["vllm"])
    assert list(coll.docs) == ["vllm"]
