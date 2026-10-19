from pathlib import Path
from typing import Any

import pytest

from dome_open_data.registry import RegistryError, load_registry, parse_registry
from tests.fake_worldbank import make_source

ROOT = Path(__file__).resolve().parent.parent


def entry(**overrides: Any) -> dict[str, Any]:
    data = make_source().model_dump()
    data.update(overrides)
    return data


def test_repository_registry_loads() -> None:
    sources = load_registry(ROOT / "sources.yaml")
    wdi = sources["worldbank_wdi"]
    assert wdi.licence.commercial_ok == "yes"
    assert len(wdi.options["indicators"]) == len(set(wdi.options["indicators"]))


def test_licence_is_mandatory() -> None:
    data = entry()
    del data["licence"]
    with pytest.raises(RegistryError, match="licence"):
        parse_registry([data])


def test_incomplete_licence_is_refused() -> None:
    data = entry()
    del data["licence"]["attribution"]
    with pytest.raises(RegistryError, match="attribution"):
        parse_registry([data])


def test_yaml_boolean_commercial_ok_is_read_as_text() -> None:
    data = entry()
    data["licence"]["commercial_ok"] = False
    assert parse_registry([data])["worldbank_wdi"].licence.commercial_ok == "no"


def test_id_must_match_publisher_and_dataset() -> None:
    with pytest.raises(RegistryError, match="worldbank_wdi"):
        parse_registry([entry(id="worldbank_other")])


def test_duplicate_ids_are_refused() -> None:
    with pytest.raises(RegistryError, match="duplicate"):
        parse_registry([entry(), entry()])


def test_unknown_keys_are_refused() -> None:
    with pytest.raises(RegistryError, match="extra"):
        parse_registry([entry(colour="blue")])


def test_storage_prefixes() -> None:
    source = make_source()
    assert source.raw_prefix == "raw/worldbank/wdi"
    assert source.clean_prefix == "clean/economy/worldbank__wdi"
