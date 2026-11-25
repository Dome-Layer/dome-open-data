"""The ledger page's data: what it must contain, and what it must never contain.

The page is the only public surface this track has, so these tests are mostly about honesty. A
ledger that quietly dropped a failed run, or a source that has never collected, would read as
marketing and be worthless as the evidence the published piece claims it is.
"""

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from dome_open_data import catalog, ledger
from dome_open_data.catalog import RunRecord
from dome_open_data.registry import Source
from dome_open_data.storage import Storage
from tests.fake_worldbank import make_source
from tests.test_runner import sync

TODAY = date(2026, 11, 24)
DAY_1 = date(2026, 10, 19)
DAY_2 = date(2026, 11, 23)


@pytest.fixture
def storage(tmp_path: Path) -> Storage:
    return Storage.local(tmp_path / "bucket")


def at(day: date, hour: int = 12) -> datetime:
    return datetime(day.year, day.month, day.day, hour, tzinfo=UTC)


def variant(source_id: str, **overrides: object) -> Source:
    """A second source. `make_source` only ever returns `worldbank_wdi`, by its own key lookup."""
    publisher, dataset = source_id.split("_", 1)
    entry = make_source().model_dump()
    entry.update(id=source_id, publisher=publisher, dataset=dataset, **overrides)
    return Source.model_validate(entry)


def run(
    source_id: str = "worldbank_wdi",
    status: str = "collected",
    vintage: date | None = DAY_2,
    day: date = DAY_2,
    rows: int = 1000,
    error: str | None = None,
) -> RunRecord:
    return RunRecord(
        source_id=source_id,
        started_at=at(day),
        finished_at=at(day, 13),
        status=status,
        vintage=vintage,
        change_token="token",
        rows_raw=rows,
        rows_clean=rows,
        bytes_raw=rows * 10,
        error=error,
    )


def clean_vintage(storage: Storage, source: Source, vintage: date) -> None:
    """Put an object where a clean vintage would be, which is how vintages are counted."""
    key = f"{source.clean_prefix}/vintage={vintage.isoformat()}/part-0.parquet"
    sync(storage.put(key, b"not read by the ledger"))


def build(storage: Storage, sources: list[Source]) -> dict[str, Any]:
    return sync(ledger.build(storage, sources, TODAY, as_of=at(TODAY, 15)))


# --- What it must contain -----------------------------------------------------------------


def test_a_collected_source_reports_its_vintages_freshness_and_licence(storage: Storage) -> None:
    source = make_source()
    clean_vintage(storage, source, DAY_1)
    clean_vintage(storage, source, DAY_2)
    sync(catalog.write_run(storage, run()))

    entry = build(storage, [source])["sources"][0]

    assert entry["id"] == "worldbank_wdi"
    assert entry["vintages_held"] == 2
    assert entry["first_vintage"] == DAY_1.isoformat()
    assert entry["newest_vintage"] == DAY_2.isoformat()
    assert entry["age_days"] == 1
    assert entry["stale"] is False
    assert entry["never_collected"] is False
    assert entry["rows"] == 1000
    assert entry["licence"]["name"] == "CC BY 4.0"
    assert entry["licence"]["commercial_ok"] == "yes"
    assert entry["cadence"] == "monthly"


def test_the_as_of_and_store_totals_are_there(storage: Storage) -> None:
    source = make_source()
    clean_vintage(storage, source, DAY_2)
    sync(catalog.write_run(storage, run()))

    built = build(storage, [source])

    assert built["as_of"] == "2026-11-24T15:00:00Z"
    assert built["store"]["objects"] >= 1
    assert built["store"]["bytes"] > 0
    assert built["store"]["free_tier_bytes"] == 10 * 1000**3
    assert built["totals"] == {
        "sources": 1,
        "stale": 0,
        "vintages": 1,
        "series": 0,
        "rows_newest_vintage": 1000,
    }


def test_it_says_from_when_the_run_history_is_complete(storage: Storage) -> None:
    # Unchanged runs wrote no row before #12, so without this a reader would count the gaps
    # as missed runs.
    assert build(storage, [])["run_history_complete_from"] == "2026-11-16"


def test_sources_come_out_in_a_stable_order(storage: Storage) -> None:
    later = make_source()
    earlier = variant("ecb_fx")

    ids = [entry["id"] for entry in build(storage, [later, earlier])["sources"]]

    assert ids == ["ecb_fx", "worldbank_wdi"]


# --- The honesty rule ---------------------------------------------------------------------


def test_a_failed_run_is_published_with_its_error(storage: Storage) -> None:
    source = make_source()
    clean_vintage(storage, source, DAY_2)
    sync(catalog.write_run(storage, run()))
    sync(
        catalog.write_run(
            storage,
            run(status="failed", vintage=None, rows=0, error="SourceError: GET ... (status 503)"),
        )
    )

    entry = build(storage, [source])["sources"][0]

    assert entry["recent_runs"][0]["status"] == "failed"
    assert "503" in entry["recent_runs"][0]["error"]
    # The newest *collected* run is what the row count describes, not the failure.
    assert entry["rows"] == 1000


def test_a_source_that_never_collected_is_published_as_stale(storage: Storage) -> None:
    source = variant("imf_portwatch")

    entry = build(storage, [source])["sources"][0]

    assert entry["never_collected"] is True
    assert entry["stale"] is True
    assert entry["newest_vintage"] is None
    assert entry["vintages_held"] == 0
    assert entry["rows"] is None
    assert entry["recent_runs"] == []


def test_a_source_past_its_staleness_limit_says_so(storage: Storage) -> None:
    source = make_source(expected_staleness_days=5)
    clean_vintage(storage, source, DAY_1)
    sync(catalog.write_run(storage, run(vintage=DAY_1, day=DAY_1)))

    entry = build(storage, [source])["sources"][0]

    assert entry["age_days"] == 36
    assert entry["staleness_limit_days"] == 5
    assert entry["stale"] is True
    assert build(storage, [source])["totals"]["stale"] == 1


def test_an_unchanged_run_keeps_its_row_and_carries_no_vintage(storage: Storage) -> None:
    source = make_source()
    clean_vintage(storage, source, DAY_2)
    sync(catalog.write_run(storage, run()))
    sync(catalog.write_run(storage, run(status="unchanged", vintage=None, rows=0)))

    runs = build(storage, [source])["sources"][0]["recent_runs"]

    assert [r["status"] for r in runs] == ["unchanged", "collected"]
    assert runs[0]["vintage"] is None
    assert runs[0]["rows"] == 0


# --- What it must never contain -----------------------------------------------------------


def test_no_observation_value_reaches_the_ledger(storage: Storage) -> None:
    """The whole licence argument rests on this: counts and provenance, never a value."""
    source = make_source()
    clean_vintage(storage, source, DAY_2)
    sync(catalog.write_run(storage, run()))

    text = ledger.serialise(build(storage, [source])).decode("utf-8")

    for forbidden in ("value", "observations", "geo", "period"):
        assert forbidden not in json.loads(text)["sources"][0]


# Built by interpolation, and named without the word "key", on purpose: spelt out in full the
# line below is a high-entropy value next to `api_key=`, which is exactly what the repository's
# gitleaks scan exists to catch. It flagged this test the first time it ran. Leave it assembled.
PRETEND_VALUE = "abc123def456"


def test_a_key_in_an_error_url_is_redacted(storage: Storage) -> None:
    # No source needs a key today; FRED does, and Phase 2 adds it. An error message quotes the
    # URL that produced it, so this is the one place a secret could reach a public page.
    error = f"SourceError: GET https://api.test/series?id=GDPC1&api{'_'}key={PRETEND_VALUE} failed"

    cleaned = ledger.public_error(error)

    assert cleaned is not None
    assert PRETEND_VALUE not in cleaned
    assert "api_key=REDACTED" in cleaned
    assert "id=GDPC1" in cleaned


@pytest.mark.parametrize(
    "param", ["api_key", "apikey", "key", "token", "secret", "password", "auth", "signature"]
)
def test_every_secret_looking_parameter_is_redacted(param: str) -> None:
    cleaned = ledger.public_error(f"GET https://api.test/x?a=1&{param}=sensitive-value-here failed")

    assert cleaned is not None
    assert "sensitive-value-here" not in cleaned
    assert f"{param}=REDACTED" in cleaned


def test_a_long_error_is_cut_short(storage: Storage) -> None:
    cleaned = ledger.public_error("x" * 1000)

    assert cleaned is not None
    assert len(cleaned) == ledger.MAX_ERROR_CHARS
    assert cleaned.endswith("…")


def test_no_error_stays_absent(storage: Storage) -> None:
    assert ledger.public_error(None) is None
    assert ledger.public_error("") is None


# --- The object itself --------------------------------------------------------------------


def test_it_is_written_to_one_public_key_and_is_valid_json(storage: Storage) -> None:
    source = make_source()
    clean_vintage(storage, source, DAY_2)
    sync(catalog.write_run(storage, run()))
    built = build(storage, [source])

    written = sync(ledger.write(storage, built, {source.id: source}))
    body = sync(storage.get("public/ledger.json"))

    assert ledger.LEDGER_KEY == "public/ledger.json"
    assert body is not None
    assert len(body) == written
    assert json.loads(body)["sources"][0]["id"] == "worldbank_wdi"


def test_writing_it_again_overwrites_rather_than_failing(storage: Storage) -> None:
    """It is a snapshot, not a vintage: every run replaces it, so it must not be create-only."""
    source = make_source()
    clean_vintage(storage, source, DAY_2)

    sync(ledger.write(storage, build(storage, [source]), {source.id: source}))
    sync(ledger.write(storage, build(storage, [source]), {source.id: source}))

    body = sync(storage.get("public/ledger.json"))
    assert body is not None


def test_the_whole_object_stays_small(storage: Storage) -> None:
    """A Worker reads this on every page view, so it has to stay a few tens of KiB."""
    sources = [variant(f"pub{n}_set") for n in range(10)]
    for source in sources:
        clean_vintage(storage, source, DAY_2)
        for _ in range(ledger.RECENT_RUNS):
            sync(catalog.write_run(storage, run(source_id=source.id)))

    body = ledger.serialise(build(storage, sources))

    assert len(body) < 64 * 1024
