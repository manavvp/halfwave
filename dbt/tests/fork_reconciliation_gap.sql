-- Warn when we counted fewer fork events than GitHub's fork counter grew, by more than 10% over
-- the latest 7 days: the sign of missing events. The other direction is normal: the counter is
-- net of deleted forks, so events exceed it for every project (by 9% to 107%, 2026-11-25).
-- Small bases (under 20 forks) and thin windows are skipped. Warn only (D018).
{{ config(severity='warn') }}

select *
from {{ ref('fork_reconciliation') }}
where signal_date = (select max(signal_date) from {{ ref('fork_reconciliation') }})
    and counter_change_7d >= 20
    and days_compared_7d >= 5
    and gap_pct_7d < -0.10
