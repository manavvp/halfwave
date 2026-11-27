-- Grain: entity x day x adoption proxy. Compares attention growth (forks) with each adoption
-- proxy's growth separately. No composite score: the numbers and a label, with the rule in
-- _marts.yml (D027).
-- Growth = average per day with data over the last 14 days vs the 14 days before, minus 1.
with rolling as (
    select *
    from {{ ref('entity_signal_rolling') }}
    where signal in ('fork', 'pypi_downloads', 'docker_pulls')
),

growth as (
    select
        cur.entity_id,
        cur.signal_date,
        cur.signal,
        cur.avg_14d as current_avg,
        prior.avg_14d as prior_avg,
        prior.sum_14d as prior_sum,
        cur.avg_14d / nullif(prior.avg_14d, 0) - 1 as growth,
        cur.low_coverage_14d or coalesce(prior.low_coverage_14d, true) as low_coverage
    from rolling as cur
    left join rolling as prior
        on prior.entity_id = cur.entity_id
        and prior.signal = cur.signal
        and prior.signal_date = date_sub(cur.signal_date, 14)
),

paired as (
    select
        adoption.entity_id,
        adoption.signal_date,
        attention.signal as attention_signal,
        attention.current_avg as attention_current_avg,
        attention.prior_avg as attention_prior_avg,
        attention.growth as attention_growth,
        adoption.signal as adoption_signal,
        adoption.current_avg as adoption_current_avg,
        adoption.prior_avg as adoption_prior_avg,
        adoption.growth as adoption_growth,
        case
            when attention.low_coverage or adoption.low_coverage then 'low_coverage'
            when coalesce(attention.prior_sum, 0) < {{ var('min_prior_forks_14d') }}
                then 'low_attention_volume'
            when coalesce(adoption.prior_avg, 0) < {{ var('min_prior_adoption_per_day') }}
                then 'low_adoption_volume'
        end as insufficient_reason
    from growth as adoption
    inner join growth as attention
        on attention.entity_id = adoption.entity_id
        and attention.signal_date = adoption.signal_date
        and attention.signal = 'fork'
    where adoption.signal in ('pypi_downloads', 'docker_pulls')
)

select
    concat_ws('|', entity_id, cast(signal_date as string), adoption_signal) as comparison_key,
    *,
    case
        when insufficient_reason is not null then 'insufficient_data'
        when attention_growth > {{ var('rising_above') }}
            and adoption_growth > {{ var('rising_above') }} then 'both_rising'
        when attention_growth < {{ var('falling_below') }}
            and adoption_growth < {{ var('falling_below') }} then 'both_falling'
        when attention_growth > {{ var('rising_above') }}
            and adoption_growth <= {{ var('flat_within') }} then 'attention_without_adoption'
        when adoption_growth > {{ var('rising_above') }}
            and attention_growth <= {{ var('flat_within') }} then 'adoption_without_attention'
        else 'no_clear_divergence'
    end as label
from paired
