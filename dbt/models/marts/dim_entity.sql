-- One row per tracked project, from the registry (D022). Coverage flags say which adoption
-- proxies exist; adoption_proxy_notes says what they actually measure.
select
    entity_id,
    name,
    entity_type,
    aliases,
    github_repo,
    pypi_package,
    docker_image,
    pypi_package is not null as has_pypi,
    docker_image is not null as has_docker,
    adoption_proxy_notes,
    loaded_at as registry_loaded_at
from {{ ref('stg_entity_registry') }}
