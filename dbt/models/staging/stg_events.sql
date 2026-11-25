-- One row per silver fact. Flattens bronze_ref so evidence queries don't need struct syntax.
select
    event_id,
    source_event_id,
    entity_id,
    source,
    event_type,
    signal_class,
    event_ts,
    to_date(event_ts) as event_date,
    ingest_ts,
    value,
    source_url,
    bronze_ref.topic as bronze_topic,
    bronze_ref.partition as bronze_partition,
    bronze_ref.offset as bronze_offset
from {{ source('silver', 'events') }}
