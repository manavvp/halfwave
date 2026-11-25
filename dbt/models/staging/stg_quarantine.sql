select
    source,
    reason,
    bronze_ref.topic as bronze_topic,
    bronze_ref.partition as bronze_partition,
    bronze_ref.offset as bronze_offset,
    payload,
    quarantined_at,
    to_date(quarantined_at) as quarantined_date
from {{ source('silver', 'quarantine') }}
