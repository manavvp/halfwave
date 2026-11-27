-- Grain: entity x day x signal, one row for every day of every signal the entity has, from the
-- signal's comparable_from to its latest observed day (a date spine). Rolling 7/14/30-day
-- windows live here, never in streaming state.
--   count signals: a day without events is 0, so their windows are always fully covered.
--   delta and daily_total signals: a day without data stays null. avg_Nd is the average per day
--   with data, so a missed poll doesn't drag the average down; days_with_data_Nd and
--   low_coverage_Nd say how much the window rests on (D026).
-- Only comparable days enter a window, so no PyPI window spans the 2026-08-24 break (D019).
{% set windows = [7, 14, 30] %}

with signals as (
    select * from {{ ref('dim_signal') }}
),

fct as (
    select * from {{ ref('fct_entity_signal_daily') }}
),

latest as (
    select signal, max(signal_date) as last_date
    from fct
    group by signal
),

entity_signals as (
    select
        e.entity_id,
        s.signal,
        s.signal_class,
        s.kind,
        s.comparable_from,
        l.last_date
    from {{ ref('dim_entity') }} as e
    cross join signals as s
    inner join latest as l on l.signal = s.signal
    where
        case s.source
            when 'pypi' then e.has_pypi
            when 'dockerhub' then e.has_docker
            else e.github_repo is not null
        end
),

spine as (
    select
        entity_id,
        signal,
        signal_class,
        kind,
        explode(sequence(comparable_from, last_date, interval 1 day)) as signal_date
    from entity_signals
),

daily as (
    select
        sp.entity_id,
        sp.signal_date,
        sp.signal,
        sp.signal_class,
        case when sp.kind = 'count' then coalesce(f.value, 0) else f.value end as value
    from spine as sp
    left join fct as f
        on f.entity_id = sp.entity_id
        and f.signal = sp.signal
        and f.signal_date = sp.signal_date
)

select
    concat_ws('|', entity_id, cast(signal_date as string), signal) as signal_key,
    entity_id,
    signal_date,
    signal,
    signal_class,
    value,
    value is not null as has_data,
    {%- for n in windows %}
    sum(value) over w{{ n }} as sum_{{ n }}d,
    avg(value) over w{{ n }} as avg_{{ n }}d,
    count(value) over w{{ n }} as days_with_data_{{ n }}d,
    count(value) over w{{ n }} < ceil({{ n }} * {{ var('min_window_coverage') }})
        as low_coverage_{{ n }}d{{ "," if not loop.last }}
    {%- endfor %}
from daily
window
    {%- for n in windows %}
    w{{ n }} as (
        partition by entity_id, signal
        order by signal_date
        rows between {{ n - 1 }} preceding and current row
    ){{ "," if not loop.last }}
    {%- endfor %}
