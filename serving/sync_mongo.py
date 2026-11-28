"""Write serving documents to MongoDB Atlas, one per entity, replaced by `_id` (D029).

The sync makes the collection mirror gold:
  - every document is replaced whole (upsert), so a field gold no longer produces disappears
    and a rerun gives the same documents
  - documents for entities that left the registry are deleted
  - a guard refuses to write anything unless gold produced exactly the registry's entities, so an
    empty or half-built gold can never wipe what the API serves

It runs only after the gold task succeeded, so a failing dbt test keeps yesterday's documents
online instead of publishing numbers that failed their checks.
"""

from __future__ import annotations

import datetime as dt
import os
from collections.abc import Iterable

from pymongo import MongoClient, ReplaceOne
from pymongo.collection import Collection

from serving.documents import Document

DATABASE = "radar"
COLLECTION = "entities"


class SyncRefused(RuntimeError):
    """Gold's documents don't match the registry, so nothing was written."""


def check_complete(docs: list[Document], expected_ids: Iterable[str]) -> None:
    """Raise SyncRefused unless there is exactly one document per registry entity."""
    expected = set(expected_ids)
    got = [d["_id"] for d in docs]
    if not expected:
        raise SyncRefused("the registry is empty")
    if len(got) != len(set(got)):
        raise SyncRefused("duplicate _id in documents")
    if set(got) != expected:
        raise SyncRefused(
            f"documents don't match the registry: missing {sorted(expected - set(got))}, "
            f"unexpected {sorted(set(got) - expected)}"
        )


def sync(
    collection: Collection,
    docs: list[Document],
    expected_ids: Iterable[str],
    synced_at: dt.datetime | None = None,
) -> dict[str, int]:
    """Replace every document by `_id`, then delete documents gold no longer has."""
    expected = sorted(set(expected_ids))
    check_complete(docs, expected)
    synced_at = synced_at or dt.datetime.now(dt.UTC)
    result = collection.bulk_write(
        [ReplaceOne({"_id": d["_id"]}, {**d, "synced_at": synced_at}, upsert=True) for d in docs],
        ordered=False,
    )
    deleted = collection.delete_many({"_id": {"$nin": expected}}).deleted_count
    return {
        "documents": len(docs),
        "matched": result.matched_count,
        "upserted": result.upserted_count,
        "deleted": deleted,
    }


def connect(uri: str | None = None) -> MongoClient:
    """A client for MONGODB_URI. Fails within 10 s if Atlas can't be reached."""
    return MongoClient(
        uri or os.environ["MONGODB_URI"], serverSelectionTimeoutMS=10_000, appname="halfwave-sync"
    )
