-- A missed producer run looks like a real zero in event counts. No day across all 25 projects
-- has zero GitHub events, so a day with none is a collection gap. Warn: the data is still
-- usable, but the gap should be recovered (runbook, 2026-11-25).
{{ config(severity='warn') }}

with days as (
    select signal_date, sum(value) as events
    from {{ ref('entity_signal_rolling') }}
    where signal in ('fork', 'release', 'pr_opened', 'pr_merged', 'issue_opened')
    group by signal_date
)

select * from days where events = 0
