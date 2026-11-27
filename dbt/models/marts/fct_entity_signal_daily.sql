-- Grain: entity x day x signal (long and narrow, D026). Rows exist only where a value was
-- observed: no row for a day without events (the rolling mart fills those with 0) and no row
-- for a snapshot delta whose neighbouring poll is missing.
with signals as (
    select * from {{ ref('dim_signal') }}
),

event_counts as (
    select
        d.entity_id,
        d.event_date as signal_date,
        s.signal,
        d.event_count as value
    from {{ ref('stg_github_events_daily') }} as d
    inner join signals as s
        on s.kind = 'count' and s.silver_event_type = d.event_type
),

snapshots as (
    select
        e.entity_id,
        e.event_date as snapshot_date,
        s.signal,
        e.value as total,
        lag(e.event_date) over w as prev_snapshot_date,
        lag(e.value) over w as prev_total
    from {{ ref('stg_events') }} as e
    inner join signals as s
        on s.kind = 'delta' and s.source = e.source and s.silver_event_type = e.event_type
    window w as (partition by e.entity_id, s.signal order by e.event_date)
),

-- A delta is the change from one day's poll to the next day's poll. Polls run around 06:00 UTC,
-- so the interval is mostly the earlier day: the delta is dated to that day. If the next day's
-- poll is missing, there is no delta for either day, and nothing is spread or invented (D026).
snapshot_deltas as (
    select
        entity_id,
        prev_snapshot_date as signal_date,
        signal,
        total - prev_total as value
    from snapshots
    where datediff(snapshot_date, prev_snapshot_date) = 1
),

daily_totals as (
    select
        e.entity_id,
        e.event_date as signal_date,
        s.signal,
        e.value
    from {{ ref('stg_events') }} as e
    inner join signals as s
        on s.kind = 'daily_total' and s.source = e.source and s.silver_event_type = e.event_type
),

unioned as (
    select * from event_counts
    union all
    select * from snapshot_deltas
    union all
    select * from daily_totals
)

select
    concat_ws('|', u.entity_id, cast(u.signal_date as string), u.signal) as signal_key,
    u.entity_id,
    u.signal_date,
    u.signal,
    s.signal_class,
    s.source,
    u.value,
    u.signal_date >= s.comparable_from as is_comparable
from unioned as u
inner join signals as s on s.signal = u.signal
