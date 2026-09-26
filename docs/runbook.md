# Runbook

Failure drills and operational notes. Each drill records expected vs observed behaviour.

## Operational gotchas

- **Resetting a bronze checkpoint requires a new `txnAppId`.** Bronze writes use
  `txnVersion = batchId` (D002). A fresh checkpoint restarts `batchId` at 0, and Delta would skip
  every write as already committed.

## Drills

_None yet. Drills are added as each layer lands._
