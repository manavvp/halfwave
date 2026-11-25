select
    entity_id,
    name,
    type as entity_type,
    aliases,
    github_repo,
    pypi_package,
    docker_image,
    adoption_proxy_notes,
    loaded_at
from {{ source('silver', 'entity_registry') }}
