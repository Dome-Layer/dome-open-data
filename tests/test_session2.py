"""Session 2 connectors (sdmx, bulk_file) and the crosswalk, against fake publishers."""

import asyncio
import io
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from dome_open_data import crosswalk
from dome_open_data.connectors import connector_for
from dome_open_data.http import SourceError, new_client
from dome_open_data.model import parse_sdmx_period
from dome_open_data.registry import Source, load_registry
from dome_open_data.runner import RunResult, run_source, verify_source
from dome_open_data.storage import Storage
from tests import fakes_session2 as fake
from tests.test_runner import sync

ROOT = Path(__file__).resolve().parent.parent
DAY_1 = date(2026, 10, 21)
DAY_2 = date(2026, 10, 22)


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    real_sleep = asyncio.sleep

    async def instant(delay: float, result: Any = None) -> Any:
        return await real_sleep(0, result)

    monkeypatch.setattr(asyncio, "sleep", instant)


@pytest.fixture
def storage(tmp_path: Path) -> Storage:
    return Storage.local(tmp_path / "bucket")


def collect(
    publisher: fake.FakePublisher, source: Source, storage: Storage, vintage: date = DAY_1
) -> RunResult:
    async def go() -> RunResult:
        async with new_client(publisher.transport()) as client:
            return await run_source(source, storage, client, vintage=vintage)

    return sync(go())


def clean(storage: Storage, source: Source, vintage: date = DAY_1) -> list[dict[str, Any]]:
    key = f"{source.clean_prefix}/vintage={vintage.isoformat()}/part-0.parquet"
    data = sync(storage.get(key))
    assert data is not None, f"no clean file {key}"
    return pq.read_table(io.BytesIO(data)).to_pylist()


def series_meta(storage: Storage, source: Source) -> list[dict[str, Any]]:
    data = sync(storage.get(f"catalog/series/source={source.id}/part-0.parquet"))
    assert data is not None
    return pq.read_table(io.BytesIO(data)).to_pylist()


# Periods


@pytest.mark.parametrize(
    ("text", "freq", "expected"),
    [
        ("2026", "A", (date(2026, 1, 1), "A")),
        ("2026-Q3", "Q", (date(2026, 7, 1), "Q")),
        ("2026-09", "M", (date(2026, 9, 1), "M")),
        ("1986-M01", "M", (date(1986, 1, 1), "M")),
        ("2026-09-16", "D", (date(2026, 9, 16), "D")),
        ("2026-09-16", "B", (date(2026, 9, 16), "D")),
    ],
)
def test_sdmx_periods(text: str, freq: str, expected: tuple[date, str]) -> None:
    assert parse_sdmx_period(text, freq) == expected


def test_sdmx_period_mismatch_is_an_error() -> None:
    with pytest.raises(ValueError, match="unrecognised period"):
        parse_sdmx_period("2026-09", "A")


# SDMX: ECB


def test_ecb_titles_units_and_missing_values(storage: Storage) -> None:
    source = fake.ecb_source()
    result = collect(fake.ecb_publisher(), source, storage)
    assert (result.status, result.rows_raw, result.rows_clean) == ("collected", 4, 3)
    rows = clean(storage, source)
    assert {r["series_id"] for r in rows} == {
        "ecb.fx.USD.EUR.SP00.A.EMU.D",
        "ecb.fx.JPY.EUR.SP00.A.EMU.D",
    }
    usd = sorted((r["period"], r["value"]) for r in rows if "USD" in r["series_id"])
    assert usd == [(date(2026, 10, 19), 1.10), (date(2026, 10, 20), 1.12)]
    meta = {m["code"]: m for m in series_meta(storage, source)}
    assert meta["USD.EUR.SP00.A"]["title"] == "USD/Euro"
    assert meta["JPY.EUR.SP00.A"]["unit"] == "JPY per EUR"
    assert sync(verify_source(source, storage)).rows_from_raw == 3


def test_ecb_change_follows_last_modified(storage: Storage) -> None:
    source = fake.ecb_source()
    assert collect(fake.ecb_publisher(), source, storage).status == "collected"
    assert collect(fake.ecb_publisher(), source, storage, DAY_2).status == "unchanged"
    newer = fake.ecb_publisher("Wed, 21 Oct 2026 13:57:02 GMT")
    assert collect(newer, source, storage, DAY_2).status == "collected"


def test_sdmx_html_answer_fails_and_stores_no_clean(storage: Storage) -> None:
    publisher = fake.ecb_publisher()
    publisher.responses["GET ecb.test/service/data/EXR/D..EUR.SP00.A"] = fake.file_response(
        b"<html>challenge</html>", "text/html", **{"last-modified": "x"}
    )
    with pytest.raises(SourceError, match="not CSV"):
        collect(publisher, fake.ecb_source(), storage)
    assert sync(storage.list_keys("clean/")) == []


# SDMX: IMF


def test_imf_geo_crosswalk_projections_and_drops(storage: Storage) -> None:
    source = fake.imf_source()
    result = collect(fake.imf_publisher(), source, storage)
    # 3 countries x 3 years x 2 indicators; 2027 is empty everywhere.
    assert (result.rows_raw, result.rows_clean) == (18, 12)
    rows = clean(storage, source)
    assert {r["geo"] for r in rows} == {"ITA", "XKX", "G001"}  # IMF KOS -> XKX
    status = {(r["period"].year, r["obs_status"]) for r in rows}
    assert status == {(2025, None), (2026, "projection")}
    assert {r["unit"] for r in rows if ".NGDPD." in r["series_id"]} == {"U.S. dollars"}


def test_imf_token_follows_named_vintages(storage: Storage) -> None:
    source = fake.imf_source()
    assert collect(fake.imf_publisher(), source, storage).status == "collected"
    with_er_change = fake.imf_publisher(
        (("WEO", "9.0.0"), ("WEO_2026_APR_VINTAGE", "1.0.0"), ("ER", "5.0.0"))
    )
    assert collect(with_er_change, source, storage, DAY_2).status == "unchanged"
    october = fake.imf_publisher(
        (("WEO", "9.0.1"), ("WEO_2026_APR_VINTAGE", "1.0.0"), ("WEO_2026_OCT_VINTAGE", "1.0.0"))
    )
    assert collect(october, source, storage, DAY_2).status == "collected"


def test_imf_series_without_title_and_unit_fail(storage: Storage) -> None:
    source = fake.imf_source(series={"NGDPD": {"title": "GDP", "unit": "U.S. dollars"}})
    with pytest.raises(SourceError, match="no title and unit for series code LUR"):
        collect(fake.imf_publisher(), source, storage)


# bulk_file: Pink Sheet (discovered link)

PAGE = "https://wb.test/commodity-markets"
FILE = "https://docs.wb.test/en/doc/abc-0050012026/related/CMO-Historical-Data-Monthly.xlsx"
PATTERN = r"https://docs\.wb\.test/[^\"'\s<>]*/CMO-Historical-Data-Monthly\.xlsx"


def pinksheet_publisher(
    page_html: str | None = None, file: bytes | None = None
) -> fake.FakePublisher:
    html = page_html or f'<a href="{FILE}">Monthly prices</a> <a href="{FILE}">again</a>'
    path = FILE.removeprefix("https://")
    lm = {"last-modified": "Fri, 02 Oct 2026 18:57:55 GMT"}
    return fake.FakePublisher(
        {
            "GET wb.test/commodity-markets": fake.file_response(html.encode(), "text/html"),
            f"HEAD {path}": fake.file_response(b"", fake.XLSX, **lm),
            f"GET {path}": fake.file_response(file or fake.pinksheet_file(), fake.XLSX, **lm),
        }
    )


def pinksheet_source() -> Source:
    return fake.bulk_source(
        "pinksheet", discover={"page": PAGE, "pattern": PATTERN}, file_type="xlsx"
    )


def test_pinksheet_discovers_the_link_and_parses(storage: Storage) -> None:
    source = pinksheet_source()
    result = collect(pinksheet_publisher(), source, storage)
    assert (result.rows_raw, result.rows_clean) == (6, 4)  # "…" and an empty cell dropped
    rows = clean(storage, source)
    assert {r["series_id"] for r in rows} == {
        "worldbank.pinksheet.CRUDE_OIL_BRENT.WLD.M",
        "worldbank.pinksheet.COAL_SOUTH_AFRICAN.WLD.M",
        "worldbank.pinksheet.GOLD.WLD.M",
    }
    meta = {m["code"]: m for m in series_meta(storage, source)}
    assert meta["COAL_SOUTH_AFRICAN"]["title"] == "Coal, South African"
    assert meta["CRUDE_OIL_BRENT"]["unit"] == "$/bbl"
    assert meta["GOLD"]["source_url"] == PAGE
    assert sync(verify_source(source, storage)).rows_from_raw == 4


def test_pinksheet_needs_exactly_one_link(storage: Storage) -> None:
    other = FILE.replace("abc", "def")
    publisher = pinksheet_publisher(page_html=f'"{FILE}" "{other}"')
    with pytest.raises(SourceError, match="expected one link"):
        collect(publisher, pinksheet_source(), storage)


def test_challenge_page_is_never_stored(storage: Storage) -> None:
    publisher = pinksheet_publisher()
    publisher.responses[f"GET {FILE.removeprefix('https://')}"] = fake.file_response(
        b"<!doctype html><title>Making sure you're not a bot!</title>", "text/html"
    )
    with pytest.raises(SourceError, match="not a xlsx file"):
        collect(publisher, pinksheet_source(), storage)
    assert sync(storage.list_keys("raw/")) == []
    assert sync(storage.list_keys("clean/")) == []


# bulk_file: WPP (fixed URL, ETag)

WPP = "https://un.test/wpp/WPP2024_Demographic_Indicators_Medium.csv.gz"


def test_wpp_location_types_aggregates_and_projections(storage: Storage) -> None:
    source = fake.bulk_source(
        "wpp",
        url=WPP,
        file_type="gzip",
        loc_types=["Country/Area", "World"],
        projection_from=2024,
        series={
            "TPopulation1July": {"title": "Population", "unit": "Thousands"},
            "TFR": {"title": "Total fertility rate", "unit": "Live births per woman"},
        },
    )
    path = WPP.removeprefix("https://")
    publisher = fake.FakePublisher(
        {
            f"HEAD {path}": fake.file_response(b"", "application/x-gzip", etag='"v1"'),
            f"GET {path}": fake.file_response(fake.wpp_file(), "application/x-gzip", etag='"v1"'),
        }
    )
    result = collect(publisher, source, storage)
    # 2 kept locations x 2 years x 2 series; the 2024 populations are empty.
    assert (result.rows_raw, result.rows_clean) == (8, 6)
    rows = clean(storage, source)
    assert {r["geo"] for r in rows} == {"ITA", "UN900"}
    assert {(r["period"].year, r["obs_status"]) for r in rows} == {
        (2023, None),
        (2024, "projection"),
    }
    assert sync(verify_source(source, storage)).rows_from_raw == 6


# bulk_file: dataverse (PWT, Maddison)


def dataverse_publisher(
    filename: str, file_id: int, content: bytes, minor: int = 0
) -> fake.FakePublisher:
    return fake.FakePublisher(
        {
            "GET dataverse.test/api/datasets/:persistentId/": fake.dataverse_json(
                filename, file_id, minor
            ),
            f"GET dataverse.test/api/access/datafile/{file_id}": fake.file_response(
                content, fake.XLSX
            ),
        }
    )


def test_pwt_units_from_legend_and_version_token(storage: Storage) -> None:
    source = fake.bulk_source(
        "pwt",
        dataverse={"doi": "doi:10.34894/FABVLR", "filename": "pwt110.xlsx"},
        file_type="xlsx",
        exclude=["i_xr"],
    )
    publisher = dataverse_publisher("pwt110.xlsx", 554105, fake.pwt_file())
    result = collect(publisher, source, storage)
    assert (result.rows_raw, result.rows_clean) == (4, 3)  # rgdpe 2020 is empty
    meta = {m["code"]: m for m in series_meta(storage, source)}
    assert set(meta) == {"rgdpe", "hc"}
    assert meta["rgdpe"]["unit"] == "mil. 2021US$"
    assert meta["rgdpe"]["title"] == "Expenditure-side real GDP at chained PPPs"
    assert (meta["hc"]["title"], meta["hc"]["unit"]) == ("Human capital index", "")
    assert meta["hc"]["source_url"] == "https://doi.org/10.34894/FABVLR"
    assert collect(publisher, source, storage, DAY_2).status == "unchanged"
    newer = dataverse_publisher("pwt110.xlsx", 554105, fake.pwt_file(), minor=1)
    assert collect(newer, source, storage, DAY_2).status == "collected"


def test_maddison_keeps_year_one_and_historical_codes(storage: Storage) -> None:
    source = fake.bulk_source(
        "maddison",
        dataverse={"doi": "doi:10.34894/INZBF2", "filename": "mpd2023_web.xlsx"},
        file_type="xlsx",
        series={
            "gdppc": {"title": "Real GDP per capita", "unit": "2011 international dollars"},
            "pop": {"title": "Population", "unit": "Thousands"},
        },
    )
    publisher = dataverse_publisher("mpd2023_web.xlsx", 421302, fake.maddison_file())
    assert collect(publisher, source, storage).rows_clean == 3
    rows = {(r["geo"], r["period"]) for r in clean(storage, source)}
    assert ("ALB", date(1, 1, 1)) in rows and ("SUN", date(1950, 1, 1)) in rows


def test_dataverse_without_the_file_fails(storage: Storage) -> None:
    source = fake.bulk_source(
        "maddison",
        dataverse={"doi": "doi:10.34894/INZBF2", "filename": "missing.xlsx"},
        file_type="xlsx",
        series={"pop": {"title": "Population", "unit": "Thousands"}},
    )
    publisher = dataverse_publisher("mpd2023_web.xlsx", 421302, fake.maddison_file())
    with pytest.raises(SourceError, match="no single file named missing.xlsx"):
        collect(publisher, source, storage)


# Registry and crosswalk


def test_every_registry_source_has_a_connector() -> None:
    for source in load_registry(ROOT / "sources.yaml").values():
        connector_for(source)


def test_repository_crosswalk_is_consistent() -> None:
    countries = crosswalk.countries()
    codes = [row["iso3"] for row in countries]
    assert len(codes) == len(set(codes))
    assert all(row["kind"] in {"current", "historical"} for row in countries)
    known = set(codes)
    for row in countries:
        assert set(filter(None, row["successors"].split(";"))) <= known, row["iso3"]
    imf = crosswalk.publisher_codes("imf")
    assert imf["KOS"] == "XKX" and imf["WBG"] == "PSE"
    aggregates = crosswalk.read_csv(crosswalk.ROOT / "aggregates.csv")
    assert not {row["code"] for row in aggregates} & known
    assert {"WLD", "G001", "UN900"} <= {row["code"] for row in aggregates}


def test_unknown_dialect_is_refused() -> None:
    source = fake.ecb_source()
    with pytest.raises(NotImplementedError):
        connector_for(source.model_copy(update={"dialect": "oecd"}))
