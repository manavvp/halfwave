-- Grain: one row per (signal row, silver event that produced it). Every number in
-- fct_entity_signal_daily traces to silver event_ids, bronze offsets and source URLs.
--   count       -> each event counted that day           (role 'event')
--   delta       -> the two snapshots it's computed from  ('snapshot_start', 'snapshot_end')
--   daily_total -> the one daily row                     ('daily_total')
with fct as (
    select f.*, s.kind, s.silver_event_type
    from {{ ref('fct_entity_signal_daily') }} as f
    inner join {{ ref('dim_signal') }} as s on s.signal = f.signal
),

events as (
    select * from {{ ref('stg_events') }}
),

matched as (
    select
        f.signal_key,
        f.entity_id,
        f.signal_date,
        f.signal,
        case
            when f.kind = 'count' then 'event'
            when f.kind = 'daily_total' then 'daily_total'
            when e.event_date = f.signal_date then 'snapshot_start'
            else 'snapshot_end'
        end as evidence_role,
        e.event_id,
        e.event_ts,
        e.value as event_value,
        e.source_url,
        e.bronze_topic,
        e.bronze_partition,
        e.bronze_offset
    from fct as f
    inner join events as e
        on e.entity_id = f.entity_id
        and e.source = f.source
        and e.event_type = f.silver_event_type
        and (
            e.event_date = f.signal_date
            or (f.kind = 'delta' and e.event_date = date_add(f.signal_date, 1))
        )
)

select
    concat_ws('|', signal_key, event_id) as evidence_key,
    *
from matched
