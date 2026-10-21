import copy

import pytest

from producers.registry import ENTITY_TYPES, RegistryError, load_registry, parse_registry

VALID = {
    "id": "vllm",
    "name": "vLLM",
    "type": "inference_engine",
    "aliases": ["vllm"],
    "github_repo": "vllm-project/vllm",
    "pypi_package": "vllm",
    "docker_image": "vllm/vllm-openai",
    "adoption_proxy_notes": "Measures the server.",
}


def entity(**overrides):
    return {**copy.deepcopy(VALID), **overrides}


def test_real_registry_loads_and_is_well_formed():
    entities = load_registry()
    assert 20 <= len(entities) <= 30
    assert {e.type for e in entities} == ENTITY_TYPES
    assert len({e.id for e in entities}) == len(entities)


def test_valid_entity_parses():
    [e] = parse_registry({"entities": [entity()]})
    assert e.id == "vllm"
    assert e.aliases == ("vllm",)


def test_folded_notes_are_normalised_to_single_spaces():
    [e] = parse_registry({"entities": [entity(adoption_proxy_notes="a\n  b   c\n")]})
    assert e.adoption_proxy_notes == "a b c"


@pytest.mark.parametrize(
    "bad, message",
    [
        (entity(type="database"), "type must be one of"),
        (entity(id="vLLM"), "lower-case slug"),
        (entity(aliases=[]), "aliases"),
        (entity(aliases=["vLLM"]), "aliases"),
        (entity(github_repo="vllm"), "github_repo must be 'owner/name'"),
        (entity(github_repo=None), "github_repo is required"),
        (entity(docker_image="vllm-openai"), "docker_image must be 'owner/name'"),
        (entity(adoption_proxy_notes=" "), "adoption_proxy_notes"),
        (entity(homepage="https://vllm.ai"), "unknown fields"),
    ],
)
def test_invalid_entity_is_rejected(bad, message):
    with pytest.raises(RegistryError, match=message):
        parse_registry({"entities": [bad]})


def test_missing_field_is_rejected():
    bad = entity()
    del bad["pypi_package"]
    with pytest.raises(RegistryError, match="missing fields"):
        parse_registry({"entities": [bad]})


DISTINCT = {
    "id": "other",
    "aliases": ["other"],
    "github_repo": "o/o",
    "pypi_package": "other",
    "docker_image": "o/o",
}


@pytest.mark.parametrize(
    "overlap, kind",
    [
        ({"id": "vllm"}, "id"),
        ({"aliases": ["other", "vllm"]}, "alias"),
        # Handles are case-insensitive on GitHub, so this is the same repo.
        ({"github_repo": "VLLM-Project/VLLM"}, "github_repo"),
        ({"pypi_package": "vllm"}, "pypi_package"),
        ({"docker_image": "vllm/vllm-openai"}, "docker_image"),
    ],
)
def test_shared_handle_is_ambiguous_and_rejected(overlap, kind):
    second = entity(**{**DISTINCT, **overlap})
    with pytest.raises(RegistryError, match=f"^{kind} .* claimed by both 'vllm' and"):
        parse_registry({"entities": [entity(), second]})


def test_null_handles_do_not_collide():
    a = entity(pypi_package=None, docker_image=None)
    b = entity(id="b", aliases=["b"], github_repo="b/b", pypi_package=None, docker_image=None)
    assert len(parse_registry({"entities": [a, b]})) == 2
