-- The quarantine alarm (D021). Silver never blocks on a bad row, so this is where it gets loud.
-- Decode failures are usually systematic (a bad deploy, an unregistered schema), so any in the
-- last 2 days fails the build. Unresolved entities fail above var max_unresolved_2d.
select reason, count(*) as rows_2d
from {{ ref('stg_quarantine') }}
where quarantined_at >= current_timestamp() - interval 2 days
group by reason
having
    (reason = 'decode_failed' and count(*) > 0)
    or (reason = 'unresolved_entity' and count(*) > {{ var('max_unresolved_2d') }})
