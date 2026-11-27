-- Grain: entity x day. Fork events we counted vs the change in GitHub's own fork counter
-- (D018): a completeness check against a reference the producer doesn't control. Daily
-- numbers never match exactly (polls run around 06:00 UTC, and deleted forks lower the
-- counter), so the comparison is over 7 days. Only days where both exist are compared.
with fork_events as (
    select entity_id, signal_date, value as fork_events
    from {{ ref('entity_signal_rolling') }}
    where signal = 'fork'
),

counter as (
    select
        entity_id,
        event_date,
        value as forks_total,
        lead(event_date) over w as next_date,
        lead(value) over w as next_total
    from {{ ref('stg_events') }}
    where source = 'github' and event_type = 'forks_total'
    window w as (partition by entity_id order by event_date)
),

compared as (
    select
        c.entity_id,
        c.event_date as signal_date,
        f.fork_events,
        c.next_total - c.forks_total as counter_change
    from counter as c
    inner join fork_events as f
        on f.entity_id = c.entity_id and f.signal_date = c.event_date
    where datediff(c.next_date, c.event_date) = 1
)

select
    concat_ws('|', entity_id, cast(signal_date as string)) as reconciliation_key,
    entity_id,
    signal_date,
    fork_events,
    counter_change,
    sum(fork_events) over w as fork_events_7d,
    sum(counter_change) over w as counter_change_7d,
    count(*) over w as days_compared_7d,
    (sum(fork_events) over w - sum(counter_change) over w)
        / nullif(sum(counter_change) over w, 0) as gap_pct_7d
from compared
window w as (
    partition by entity_id
    order by unix_date(signal_date)
    range between 6 preceding and current row
)
