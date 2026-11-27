-- Every signal row is fully traced: a count has exactly as many evidence rows as its value, a
-- delta has its two snapshots, a daily total its one row.
with evidence as (
    select signal_key, count(*) as evidence_rows
    from {{ ref('signal_evidence') }}
    group by signal_key
)

select f.signal_key, f.value, s.kind, e.evidence_rows
from {{ ref('fct_entity_signal_daily') }} as f
inner join {{ ref('dim_signal') }} as s on s.signal = f.signal
left join evidence as e on e.signal_key = f.signal_key
where coalesce(e.evidence_rows, 0) != case s.kind
    when 'count' then f.value
    when 'delta' then 2
    else 1
end
