-- The signals gold publishes, and how each is derived from silver (D026).
--   count       : events per day, from silver's daily GitHub counts. A day without events is 0.
--   delta       : change between consecutive daily snapshots of a counter.
--   daily_total : already one number per day (PyPI).
-- comparable_from is the first day the signal means the same thing as today.
select
    signal,
    source,
    silver_event_type,
    signal_class,
    kind,
    cast(comparable_from as date) as comparable_from,
    description
from values
    ('fork', 'github', 'fork', 'attention', 'count', '2026-08-02',
     'Forks created that day.'),
    ('stars_net', 'github', 'stars_total', 'attention', 'delta', '2026-10-30',
     'Change in the star counter from one daily poll to the next (new stars minus unstars).'),
    ('release', 'github', 'release', 'activity', 'count', '2026-08-02',
     'Releases published that day. Inflated by CI for some projects.'),
    ('pr_opened', 'github', 'pr_opened', 'activity', 'count', '2026-08-02',
     'Pull requests opened that day.'),
    ('pr_merged', 'github', 'pr_merged', 'activity', 'count', '2026-08-02',
     'Pull requests merged that day.'),
    ('issue_opened', 'github', 'issue_opened', 'activity', 'count', '2026-08-02',
     'Issues opened that day. Partly a usage signal.'),
    ('pypi_downloads', 'pypi', 'downloads', 'adoption', 'daily_total',
     '{{ var("pypi_comparable_from") }}',
     'Downloads that day, mirrors excluded.'),
    ('docker_pulls', 'dockerhub', 'pulls_total', 'adoption', 'delta', '2026-10-21',
     'Change in the pull counter from one daily poll to the next.')
as t(signal, source, silver_event_type, signal_class, kind, comparable_from, description)
