-- Daily GitHub event counts per entity and event type. Only closed days are present (D023).
select
    day as event_date,
    entity_id,
    event_type,
    signal_class,
    event_count
from {{ source('silver', 'github_events_daily') }}
