-- Snapshots can't be backfilled, so a missed poll is lost for good. Known gaps are listed in
-- var known_snapshot_gaps; any other day without a snapshot is new and is flagged. Warn only.
{{ config(severity='warn') }}

{% set gaps = var('known_snapshot_gaps') %}

with polled as (
    select source, event_date
    from {{ ref('stg_events') }}
    where event_type in ('stars_total', 'pulls_total')
    group by source, event_date
),

expected as (
    select source, explode(sequence(min(event_date), max(event_date), interval 1 day)) as event_date
    from polled
    group by source
)

select e.source, e.event_date
from expected as e
left join polled as p on p.source = e.source and p.event_date = e.event_date
where p.event_date is null
    and not (
        {%- for source, dates in gaps.items() %}
        (e.source = '{{ source }}' and e.event_date in ({% for d in dates %}date'{{ d }}'{{ "," if not loop.last }}{% endfor %})){{ " or" if not loop.last }}
        {%- endfor %}
    )
