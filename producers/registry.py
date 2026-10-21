"""Load and validate the entity registry (config/entities.yaml).

The registry is the single source of truth for what the radar tracks. Validation is strict
because silver resolves records by *exact* match on these handles: two entities sharing a
handle or alias would make resolution ambiguous, so that is rejected here, at load time.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import yaml

DEFAULT_PATH = Path(__file__).resolve().parents[1] / "config" / "entities.yaml"

ENTITY_TYPES = frozenset({"inference_engine", "serving_framework", "local_runtime", "llm_gateway"})

_FIELDS = (
    "id",
    "name",
    "type",
    "aliases",
    "github_repo",
    "pypi_package",
    "docker_image",
    "adoption_proxy_notes",
)
_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_OWNER_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


class RegistryError(ValueError):
    """The registry file is malformed or ambiguous."""


@dataclass(frozen=True)
class Entity:
    id: str
    name: str
    type: str
    aliases: tuple[str, ...]
    github_repo: str | None
    pypi_package: str | None
    docker_image: str | None
    adoption_proxy_notes: str


def load_registry(path: Path = DEFAULT_PATH) -> list[Entity]:
    with path.open() as f:
        return parse_registry(yaml.safe_load(f))


def parse_registry(raw: object) -> list[Entity]:
    if not isinstance(raw, dict) or not isinstance(raw.get("entities"), list):
        raise RegistryError("registry must be a mapping with an 'entities' list")

    entities = [_parse_entity(i, item) for i, item in enumerate(raw["entities"])]
    _check_unique(entities)
    return entities


def _parse_entity(index: int, item: object) -> Entity:
    where = f"entities[{index}]"
    if not isinstance(item, dict):
        raise RegistryError(f"{where}: expected a mapping")

    missing = [f for f in _FIELDS if f not in item]
    unknown = sorted(set(item) - set(_FIELDS))
    if missing:
        raise RegistryError(f"{where}: missing fields {missing}")
    if unknown:
        raise RegistryError(f"{where}: unknown fields {unknown}")

    entity_id = item["id"]
    where = f"entity '{entity_id}'"
    if not isinstance(entity_id, str) or not _ID_RE.match(entity_id):
        raise RegistryError(f"{where}: id must be a lower-case slug")
    if item["type"] not in ENTITY_TYPES:
        raise RegistryError(f"{where}: type must be one of {sorted(ENTITY_TYPES)}")
    for field in ("name", "adoption_proxy_notes"):
        if not isinstance(item[field], str) or not item[field].strip():
            raise RegistryError(f"{where}: {field} must be a non-empty string")

    aliases = item["aliases"]
    if (
        not isinstance(aliases, list)
        or not aliases
        or not all(isinstance(a, str) and a.strip() and a == a.lower() for a in aliases)
    ):
        raise RegistryError(f"{where}: aliases must be a non-empty list of lower-case strings")

    for field in ("github_repo", "docker_image"):
        value = item[field]
        if value is not None and not (isinstance(value, str) and _OWNER_NAME_RE.match(value)):
            raise RegistryError(f"{where}: {field} must be 'owner/name' or null")
    if item["pypi_package"] is not None and not isinstance(item["pypi_package"], str):
        raise RegistryError(f"{where}: pypi_package must be a string or null")
    if item["github_repo"] is None:
        # GH Archive is the attention backbone; an entity without a repo has no signal at all.
        raise RegistryError(f"{where}: github_repo is required")

    return Entity(
        id=entity_id,
        name=item["name"],
        type=item["type"],
        aliases=tuple(aliases),
        github_repo=item["github_repo"],
        pypi_package=item["pypi_package"],
        docker_image=item["docker_image"],
        adoption_proxy_notes=" ".join(item["adoption_proxy_notes"].split()),
    )


def _check_unique(entities: list[Entity]) -> None:
    """Every id, alias and source handle must point at exactly one entity.

    Handles are compared case-insensitively: GitHub, PyPI and Docker Hub all treat names that
    way, so `BerriAI/litellm` and `berriai/litellm` are the same repo.
    """
    seen: dict[tuple[str, str], str] = {}
    for e in entities:
        keys = [("id", e.id)] + [("alias", a) for a in e.aliases]
        keys += [
            (field, value.lower())
            for field, value in (
                ("github_repo", e.github_repo),
                ("pypi_package", e.pypi_package),
                ("docker_image", e.docker_image),
            )
            if value is not None
        ]
        for key in dict.fromkeys(keys):  # dedup within the entity, keep a stable order
            if key in seen:
                kind, value = key
                raise RegistryError(
                    f"{kind} '{value}' is claimed by both '{seen[key]}' and '{e.id}'"
                )
            seen[key] = e.id
