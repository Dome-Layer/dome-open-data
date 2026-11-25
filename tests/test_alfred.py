"""ALFRED: publication-dated vintages, write once, and the 2008 revision story.

The fixtures in `tests/fixtures/alfred/` are real: the 2007 and 2008 rows of `GDPC1` exactly as
`alfredgraph.csv` served them on 2026-11-25 for three publication dates.
"""

import asyncio
import gzip
import json
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pytest

from dome_open_data import alfred, catalog, ops
from dome_open_data.connectors.alfred import (
    EMPTY_ATTEMPTS,
    ShapeError,
    parse_series_info,
    series_id,
)
from dome_open_data.http import USER_AGENT, SourceError, new_client
from dome_open_data.registry import Source, load_registry
from dome_open_data.storage import Storage
from dome_open_data.validate import ValidationError
from tests.test_runner import sync

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures" / "alfred"
PUBLISHED_2008 = date(2008, 4, 30)
REVISED_2009 = date(2009, 7, 31)
TODAY_2026 = date(2026, 11, 25)
DATES = [PUBLISHED_2008, REVISED_2009, TODAY_2026]


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    real_sleep = asyncio.sleep

    async def instant(delay: float, result: Any = None) -> Any:
        return await real_sleep(0, result)

    monkeypatch.setattr(asyncio, "sleep", instant)


@pytest.fixture
def storage(tmp_path: Path) -> Storage:
    return Storage.local(tmp_path / "bucket")


@pytest.fixture
def source() -> Source:
    registry = load_registry(ROOT / "sources.yaml")
    entry = registry["fred_alfred"].model_dump()
    entry["endpoint"] = "https://alfred.test/"
    entry["rate_limit"] = {"per_second": 1000}
    entry["options"] = {"series": ["GDPC1"]}
    return Source.model_validate(entry)


def fixture_csv(published: date) -> bytes:
    return (FIXTURES / f"GDPC1_{published:%Y%m%d}.csv").read_bytes()


def series_json(code: str, dates: list[date], **overrides: Any) -> dict[str, Any]:
    entry = {
        "series_id": code,
        "title": "Real Gross Domestic Product",
        "season_short": "SAAR",
        "frequency_short": "Q",
        "units": "Billions of Chained 2017 Dollars",
        "notes": "BEA Account Code: A191RX",
        "available_revision_dates": [d.isoformat() for d in dates],
    }
    entry.update(overrides)
    return {"status": "ok", "chart_series": [{"series_objects": {"a": entry}}]}


class FakeAlfred:
    """ALFRED's two keyless routes, with knobs for each way the real one can misbehave."""

    def __init__(self, dates: list[date]) -> None:
        self.dates = list(dates)
        self.csv: dict[date, bytes] = {d: fixture_csv(d) for d in dates}
        self.empty_first: dict[date, int] = {}
        self.info_override: dict[str, Any] = {}
        self.payload: Any = None
        self.requests: list[httpx.Request] = []
        self.in_flight = 0
        self.max_in_flight = 0

    def transport(self) -> httpx.MockTransport:
        async def handle(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
            try:
                await asyncio.sleep(0)
                return self.answer(request)
            finally:
                self.in_flight -= 1

        return httpx.MockTransport(handle)

    def answer(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/graph/api/series/":
            assert request.url.params["mode"] == "alfred"
            body = self.payload or series_json(
                request.url.params["id"], self.dates, **self.info_override
            )
            return httpx.Response(200, json=body)
        if path == "/graph/alfredgraph.csv":
            published = date.fromisoformat(request.url.params["vintage_date"])
            if self.empty_first.get(published, 0) > 0:
                self.empty_first[published] -= 1
                return httpx.Response(200, content=b"", headers={"content-type": "application/csv"})
            return httpx.Response(
                200, content=self.csv[published], headers={"content-type": "application/csv"}
            )
        return httpx.Response(404)

    def csv_requests(self) -> list[date]:
        return [
            date.fromisoformat(r.url.params["vintage_date"])
            for r in self.requests
            if r.url.path.endswith("alfredgraph.csv")
        ]


def collect(
    source: Source, storage: Storage, fake: FakeAlfred, **kwargs: Any
) -> alfred.AlfredResult:
    async def go() -> alfred.AlfredResult:
        async with new_client(fake.transport()) as client:
            return await alfred.run_alfred(source, storage, client, **kwargs)

    return sync(go())


def statuses(storage: Storage, source: Source) -> list[str]:
    return [row["status"] for row in sync(catalog.recent_runs(storage, source.id, limit=10))]


# --- The 2008 revision story --------------------------------------------------------------


def q1_2008_growth(storage: Storage, source: Source) -> dict[date, float]:
    """Q1 2008 annualised growth over Q4 2007, as each publication date stated it."""
    keys = sync(alfred.committed_clean_keys(storage, source, "GDPC1"))
    con = storage.duckdb()
    try:
        rows = con.execute(
            """
            SELECT vintage,
                   (max(value) FILTER (period = DATE '2008-01-01')
                    / max(value) FILTER (period = DATE '2007-10-01')) ** 4 - 1
            FROM read_parquet(?, hive_partitioning = false)
            WHERE series_id = ?
            GROUP BY vintage ORDER BY vintage
            """,
            [[storage.duckdb_path(k) for k in keys], series_id("GDPC1", "Q")],
        ).fetchall()
    finally:
        con.close()
    return {vintage: round(100 * growth, 2) for vintage, growth in rows}


def test_the_2008_revision_story_is_reproduced_from_the_store(
    source: Source, storage: Storage
) -> None:
    """Published as growth, revised to a contraction, and deeper still today."""
    collect(source, storage, FakeAlfred(DATES))

    assert q1_2008_growth(storage, source) == {
        PUBLISHED_2008: 0.60,
        REVISED_2009: -0.72,
        TODAY_2026: -1.70,
    }


# --- Collecting ---------------------------------------------------------------------------


def test_a_backfill_collects_every_publication_date_with_two_dates_per_object(
    source: Source, storage: Storage
) -> None:
    result = collect(source, storage, FakeAlfred(DATES))

    assert result.status == "collected"
    assert result.new == 3
    raw = sync(storage.get("raw/fred/alfred/series=GDPC1/vintage=2008-04-30/GDPC1_20080430.csv.gz"))
    assert raw is not None and gzip.decompress(raw) == fixture_csv(PUBLISHED_2008)
    [manifest] = sync(alfred.manifests(storage, source, "GDPC1"))
    entry = manifest["vintages"][0]
    # The key carries the publication date; the manifest the collection time and the hash.
    assert entry["publication_date"] == "2008-04-30"
    assert entry["collected_at"].startswith(str(date.today().year))
    assert len(entry["sha256"]) == 64
    assert entry["rows_clean"] == 5
    assert sync(catalog.vintages(storage, source)) == DATES
    assert statuses(storage, source) == ["collected"]


def test_requests_are_serial_and_carry_the_honest_user_agent(
    source: Source, storage: Storage
) -> None:
    fake = FakeAlfred(DATES)
    collect(source, storage, fake)

    assert fake.max_in_flight == 1
    agents = {r.headers["user-agent"] for r in fake.requests}
    assert agents == {USER_AGENT}
    assert not any(word in USER_AGENT for word in ("Mozilla", "Chrome", "Safari"))
    assert all(r.url.host == "alfred.test" for r in fake.requests)


def test_a_normal_run_fetches_only_what_is_new(source: Source, storage: Storage) -> None:
    fake = FakeAlfred(DATES[:2])
    collect(source, storage, fake)

    later = FakeAlfred(DATES)
    result = collect(source, storage, later)

    # The newest held date is checked again; only the new one is collected.
    assert later.csv_requests() == [REVISED_2009, TODAY_2026]
    assert result.new == 1
    assert sync(catalog.vintages(storage, source)) == DATES
    assert q1_2008_growth(storage, source)[TODAY_2026] == -1.70


def test_nothing_new_writes_only_a_run_row(source: Source, storage: Storage) -> None:
    collect(source, storage, FakeAlfred(DATES))
    before = sync(storage.list_keys(""))

    result = collect(source, storage, FakeAlfred(DATES))

    assert result.status == "unchanged"
    added = sorted(set(sync(storage.list_keys(""))) - set(before))
    assert len(added) == 1 and added[0].startswith("catalog/runs/source=fred_alfred/")
    assert statuses(storage, source) == ["unchanged", "collected"]


def test_recollecting_held_dates_only_verifies_their_hash(source: Source, storage: Storage) -> None:
    collect(source, storage, FakeAlfred(DATES))
    before = sync(storage.list_keys(""))

    result = collect(source, storage, FakeAlfred(DATES), recheck="all")

    assert result.series[0].verified == DATES
    added = set(sync(storage.list_keys(""))) - set(before)
    assert all(key.startswith("catalog/runs/") for key in added)


def test_a_changed_version_is_written_beside_the_first_and_raised(
    source: Source, storage: Storage
) -> None:
    collect(source, storage, FakeAlfred(DATES))
    changed = FakeAlfred(DATES)
    changed.csv[TODAY_2026] = changed.csv[TODAY_2026].replace(b"16843.003", b"16843.004")

    with pytest.raises(alfred.ArchiveChangedError, match="GDPC1 2026-11-25"):
        collect(source, storage, changed)

    folder = sync(storage.list_keys("raw/fred/alfred/series=GDPC1/vintage=2026-11-25/"))
    assert len(folder) == 2
    first = sync(storage.get(folder[0] if folder[0].endswith("20261125.csv.gz") else folder[1]))
    assert first is not None and gzip.decompress(first) == fixture_csv(TODAY_2026)
    # Kept out of the clean zone: the store still reads the version it first held.
    assert q1_2008_growth(storage, source)[TODAY_2026] == -1.70
    manifests = sync(alfred.manifests(storage, source, "GDPC1"))
    assert manifests[-1]["conflicts"][0]["publication_date"] == "2026-11-25"
    assert statuses(storage, source)[0] == "conflict"
    # And it is never treated as a blip.
    assert ops.should_alert(["conflict", "collected"], "daily", False).alert


def test_an_empty_csv_is_asked_for_again(source: Source, storage: Storage) -> None:
    fake = FakeAlfred(DATES)
    fake.empty_first[REVISED_2009] = EMPTY_ATTEMPTS - 1

    result = collect(source, storage, fake)

    assert result.new == 3


def test_an_empty_csv_every_time_fails_the_run(source: Source, storage: Storage) -> None:
    fake = FakeAlfred(DATES)
    fake.empty_first[REVISED_2009] = EMPTY_ATTEMPTS

    with pytest.raises(SourceError, match="empty CSV"):
        collect(source, storage, fake)
    assert statuses(storage, source) == ["failed"]
    # Nothing was committed, so nothing counts as held.
    assert sync(catalog.vintages(storage, source)) == []


def test_a_header_for_another_date_fails_the_run(source: Source, storage: Storage) -> None:
    fake = FakeAlfred(DATES)
    fake.csv[REVISED_2009] = fixture_csv(PUBLISHED_2008)

    with pytest.raises(SourceError, match="did not serve the version asked for"):
        collect(source, storage, fake)


def test_a_run_that_dies_part_way_is_redone_cleanly(source: Source, storage: Storage) -> None:
    broken = FakeAlfred(DATES)
    broken.csv[TODAY_2026] = b"observation_date,GDPC1_20261125\n2007-10-01,abc\n"
    with pytest.raises(SourceError):
        collect(source, storage, broken)

    collect(source, storage, FakeAlfred(DATES))

    # The leftover raw files from the dead run were reused, and no date is counted twice.
    checked = sync(alfred.verify_alfred(source, storage))
    assert checked.publication_dates == 3
    assert q1_2008_growth(storage, source)[PUBLISHED_2008] == 0.60


def test_the_per_run_cap_leaves_the_rest_for_the_next_run(source: Source, storage: Storage) -> None:
    first = collect(source, storage, FakeAlfred(DATES), max_new=2)
    assert first.new == 2 and first.series[0].left_for_later == 1

    second = collect(source, storage, FakeAlfred(DATES), max_new=2)
    assert second.new == 1
    assert sync(catalog.vintages(storage, source)) == DATES


def test_a_date_alfred_stops_listing_is_raised(source: Source, storage: Storage) -> None:
    collect(source, storage, FakeAlfred(DATES))

    with pytest.raises(alfred.ArchiveChangedError, match="no longer lists"):
        collect(source, storage, FakeAlfred([PUBLISHED_2008, TODAY_2026]))


# --- The date list's shape ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda p: p.pop("chart_series"), "no chart_series"),
        (lambda p: p["chart_series"][0].pop("series_objects"), "no series_objects.a"),
        (lambda p: p["chart_series"][0]["series_objects"]["a"].update(series_id="X"), "series_id"),
        (
            lambda p: p["chart_series"][0]["series_objects"]["a"].update(
                available_revision_dates=[]
            ),
            "missing or empty",
        ),
        (
            lambda p: p["chart_series"][0]["series_objects"]["a"].update(
                available_revision_dates=["2009-07-31", "2008-04-30"]
            ),
            "ascending",
        ),
        (
            lambda p: p["chart_series"][0]["series_objects"]["a"].update(
                available_revision_dates=["31/07/2009"]
            ),
            "changed shape",
        ),
        (
            lambda p: p["chart_series"][0]["series_objects"]["a"].update(frequency_short="SA"),
            "frequency_short",
        ),
    ],
)
def test_a_date_list_in_another_shape_fails_loudly(
    mutate: Callable[[dict[str, Any]], Any], message: str
) -> None:
    payload = series_json("GDPC1", DATES)
    mutate(payload)
    with pytest.raises(ShapeError, match=message):
        parse_series_info(payload, "GDPC1")


def test_the_real_shape_read_on_2026_11_25_parses() -> None:
    info = parse_series_info(series_json("GDPC1", DATES), "GDPC1")
    assert info.dates == DATES and info.freq == "Q"


def test_a_series_with_a_copyright_notice_is_refused(source: Source, storage: Storage) -> None:
    fake = FakeAlfred(DATES)
    fake.info_override = {"notes": "Copyright, 2026, S&P Dow Jones Indices LLC."}

    with pytest.raises(SourceError, match="not public domain"):
        collect(source, storage, fake)
    assert fake.csv_requests() == []


# --- Verify -------------------------------------------------------------------------------


def test_verify_counts_every_publication_date(source: Source, storage: Storage) -> None:
    collect(source, storage, FakeAlfred(DATES))

    checked = sync(alfred.verify_alfred(source, storage))

    assert (checked.series, checked.publication_dates, checked.rows) == (1, 3, 5 + 8 + 8)


def test_verify_catches_a_tampered_raw_file(source: Source, storage: Storage) -> None:
    collect(source, storage, FakeAlfred(DATES))
    key = "raw/fred/alfred/series=GDPC1/vintage=2009-07-31/GDPC1_20090731.csv.gz"
    local = Path(storage.duckdb_path(key))
    local.write_bytes(gzip.compress(b"observation_date,GDPC1_20090731\n2007-01-01,1\n"))

    with pytest.raises(ValidationError, match="checksum mismatch"):
        sync(alfred.verify_alfred(source, storage))


def test_verify_with_nothing_held_fails(source: Source, storage: Storage) -> None:
    with pytest.raises(ValidationError, match="nothing held"):
        sync(alfred.verify_alfred(source, storage))


def test_the_registry_entry_is_public_domain_and_daily() -> None:
    entry = load_registry(ROOT / "sources.yaml")["fred_alfred"]
    assert entry.connector == "alfred"
    assert entry.cadence == "daily"
    assert entry.licence.commercial_ok == "yes"
    assert alfred.codes(entry)[0] == "GDPC1"
    assert "alfred.stlouisfed.org" in entry.endpoint
    json.dumps(entry.options)
