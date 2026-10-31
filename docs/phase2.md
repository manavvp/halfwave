# Phase 2 ideas

Not in V1. Ideas land here by default instead of expanding scope.

- **Better adoption proxies.** Dependents counts (deps.dev, GitHub dependency graph) and full PyPI
  history (BigQuery public dataset). Both need tools outside the V1 stack.
- **GitHub events from OpenDigger, with completeness monitoring (D015 Direction 2).** Health flags
  per hour × event type from the mirror's per-minute manifests, plus reconciliation against repo
  snapshots. It brings real late data (pushes arrive up to 33 h late), but it's 10 GB a day and
  depends on one volunteer.
- **Star history from OpenDigger WatchEvents (D018).** Rebuild per-star events back to
  2026-09-06, the only public source left for star timestamps. About 10 GB a day to scan.
- **Bluesky Jetstream.** A public WebSocket stream of every post, filtered for project names:
  the only genuinely push-based source. It needs an always-on producer, which serverless can't
  host.
- **HN via the Firebase `updates` feed** instead of polling Algolia: items change (points,
  comments), so it's an upsert/CDC story rather than append-only.
- **Conda download stats** (weekly batch files) for more adoption coverage.
