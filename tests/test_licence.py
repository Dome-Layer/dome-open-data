"""The licence gate: no value from a source we may not republish reaches anything public."""

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from dome_open_data import ledger, licence
from dome_open_data.licence import LicenceError, check_public, publish, value_block
from dome_open_data.registry import Source, load_registry
from dome_open_data.storage import Storage
from tests.test_runner import sync

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = load_registry(ROOT / "sources.yaml")
ASK = [s for s in REGISTRY.values() if s.licence.commercial_ok != "yes"]
YES = [s for s in REGISTRY.values() if s.licence.commercial_ok == "yes"]


@pytest.fixture
def storage(tmp_path: Path) -> Storage:
    return Storage.local(tmp_path / "bucket")


def dashboard(block: dict[str, Any]) -> dict[str, Any]:
    """A public payload shaped like the coming dashboard JSON, with the block nested deep."""
    return {"as_of": "2026-11-25T00:00:00Z", "panels": [{"key_figures": [block]}]}


def test_the_registry_has_sources_on_both_sides_of_the_gate() -> None:
    assert ASK, "the IMF sources are `ask` today; without one this suite proves nothing"
    assert YES


@pytest.mark.parametrize("source", ASK, ids=lambda s: s.id)
def test_a_value_from_an_ask_source_never_reaches_the_public_json(
    source: Source, storage: Storage
) -> None:
    smuggled = {
        "source_id": source.id,
        "attribution": source.licence.attribution,
        "values": [["2026-01-01", 1.0]],
    }

    with pytest.raises(LicenceError, match="may not be published"):
        sync(publish(storage, "public/dashboard.json", dashboard(smuggled), REGISTRY))
    assert sync(storage.get("public/dashboard.json")) is None


@pytest.mark.parametrize("source", ASK, ids=lambda s: s.id)
def test_a_value_block_cannot_even_be_built_for_an_ask_source(source: Source) -> None:
    with pytest.raises(LicenceError, match="never values"):
        value_block(source, [1.0])


@pytest.mark.parametrize("source", YES, ids=lambda s: s.id)
def test_a_reusable_source_publishes_with_the_registry_attribution(
    source: Source, storage: Storage
) -> None:
    block = value_block(source, [["2026-11-23", 1.0939]], series_id="x")

    sync(publish(storage, "public/dashboard.json", dashboard(block), REGISTRY))

    body = sync(storage.get("public/dashboard.json"))
    assert body is not None
    written = json.loads(body)["panels"][0]["key_figures"][0]
    assert written["attribution"] == source.licence.attribution


def test_a_single_value_counts_as_a_value_too(storage: Storage) -> None:
    imf = ASK[0]
    block = {"source_id": imf.id, "attribution": imf.licence.attribution, "value": 3.2}
    with pytest.raises(LicenceError):
        check_public(dashboard(block), REGISTRY)


def test_a_value_with_no_source_is_refused() -> None:
    with pytest.raises(LicenceError, match="no source_id"):
        check_public(dashboard({"values": [1.0]}), REGISTRY)


def test_a_value_from_an_unknown_source_is_refused() -> None:
    with pytest.raises(LicenceError, match="not in the registry"):
        check_public(dashboard({"source_id": "made_up", "values": [1.0]}), REGISTRY)


def test_a_retyped_attribution_is_refused() -> None:
    block = value_block(REGISTRY["ecb_fx"], [1.0])
    block["attribution"] = "Source: ECB."
    with pytest.raises(LicenceError, match="attribution must be the registry's"):
        check_public(dashboard(block), REGISTRY)


def test_nothing_outside_public_is_written_by_publish(storage: Storage) -> None:
    with pytest.raises(LicenceError, match="not under public/"):
        sync(publish(storage, "clean/x.json", {}, REGISTRY))


def test_the_ledger_goes_through_the_gate_and_carries_no_value(storage: Storage) -> None:
    built = sync(ledger.build(storage, list(REGISTRY.values()), date.today()))

    assert check_public(built, REGISTRY) == 0
    sync(ledger.write(storage, built, REGISTRY))
    assert sync(storage.get(ledger.LEDGER_KEY)) is not None


def test_the_ledger_refuses_a_value_slipped_into_it(storage: Storage) -> None:
    imf = REGISTRY["imf_weo"]
    built = sync(ledger.build(storage, [imf], date.today()))
    built["sources"][0]["latest"] = {
        "source_id": imf.id,
        "attribution": imf.licence.attribution,
        "value": 0.7,
    }

    with pytest.raises(LicenceError):
        sync(ledger.write(storage, built, REGISTRY))
    assert sync(storage.get(ledger.LEDGER_KEY)) is None


def test_only_the_gate_writes_under_public() -> None:
    """Any other module writing a `public/` key would walk round the gate."""
    package = ROOT / "src" / "dome_open_data"
    offenders = [
        path.name
        for path in package.rglob("*.py")
        if path.name != "licence.py"
        and "storage.put(" in path.read_text()
        and "public/" in path.read_text()
    ]
    assert offenders == []
    assert licence.PUBLIC_PREFIX == "public/"
