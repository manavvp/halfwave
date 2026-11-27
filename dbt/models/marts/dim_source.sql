-- One row per source system.
select * from values
    ('github', 'GitHub GraphQL API', 'daily', '2026-08-02',
     'Fork, release, PR and issue events with GitHub timestamps, plus a daily repo snapshot '
     || '(star and fork counters). Stars only as a counter: GitHub stopped listing stargazers '
     || 'to non-admins (D018).'),
    ('pypi', 'pypistats.org', 'daily', '{{ var("pypi_comparable_from") }}',
     'Downloads per package per day, mirrors excluded. Counts before 2026-08-25 include '
     || 'installer metadata requests and are not comparable (D019).'),
    ('dockerhub', 'Docker Hub', 'daily', '2026-10-21',
     'Cumulative pull counter, read once a day. Only 13 of 25 projects publish there (D010).')
as t(source, name, cadence, comparable_from, caveat)
