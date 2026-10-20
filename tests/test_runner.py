import asyncio
import io
import json
from collections.abc import Awaitable
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from dome_open_data.http import SourceError, new_client
from dome_open_data.registry import Source
from dome_open_data.runner import RunResult, VerifyResult, run_source, verify_source
from dome_open_data.storage import Storage, VintageExistsError
from dome_open_data.validate import ValidationError
from tests.fake_worldbank import FakeWorldBank, make_source, record

DAY_1 = date(2026, 10, 19)
DAY_2 = date(2026, 10, 20)
RAW = "raw/worldbank/wdi/vintage=2026-10-19/"
CLEAN = "clean/economy/worldbank__wdi/vintage=2026-10-19/part-0.parquet"


def sync[T](awaitable: Awaitable[T]) -> T:
    async def wrapper() -> T:
        return await awaitable

    return asyncio.run(wrapper())


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
    api: FakeWorldBank,
    storage: Storage,
    vintage: date = DAY_1,
    force: bool = False,
    source: Source | None = None,
) -> RunResult:
    async def go() -> RunResult:
        async with new_client(api.transport()) as client:
            return await run_source(
                source or make_source(), storage, client, vintage=vintage, force=force
            )

    return sync(go())


def keys(storage: Storage, prefix: str = "") -> list[str]:
    return sync(storage.list_keys(prefix))


def runs(storage: Storage) -> list[dict[str, Any]]:
    rows = []
    for key in keys(storage, "catalog/runs/"):
        data = sync(storage.get(key))
        assert data is not None
        rows.extend(pq.read_table(io.BytesIO(data)).to_pylist())
    return rows


def test_first_run_lands_raw_clean_and_catalog(storage: Storage) -> None:
    result = collect(FakeWorldBank(), storage)

    assert result.status == "collected"
    assert (result.rows_raw, result.rows_clean) == (10, 6)
    stored = keys(storage)
    assert f"{RAW}_manifest.json" in stored
    assert f"{RAW}_indicators.json.gz" in stored
    assert f"{RAW}NY.GDP.MKTP.CD.p1.json.gz" in stored
    assert f"{RAW}NY.GDP.MKTP.CD.p2.json.gz" in stored
    assert CLEAN in stored
    assert "catalog/sources/source=worldbank_wdi/part-0.parquet" in stored
    assert "catalog/series/source=worldbank_wdi/part-0.parquet" in stored

    manifest = json.loads(sync(storage.get(f"{RAW}_manifest.json")) or b"")
    assert manifest["dropped"] == {"null_value": 2, "no_geo": 2}
    assert [run["status"] for run in runs(storage)] == ["collected"]


def test_clean_rows_follow_the_common_model(storage: Storage) -> None:
    collect(FakeWorldBank(), storage)
    data = sync(storage.get(CLEAN))
    assert data is not None
    rows = pq.read_table(io.BytesIO(data)).to_pylist()
    ita = next(
        r
        for r in rows
        if r["series_id"] == "worldbank.wdi.NY.GDP.MKTP.CD.ITA.A"
        and r["period"] == date(2021, 1, 1)
    )
    assert ita == {
        "series_id": "worldbank.wdi.NY.GDP.MKTP.CD.ITA.A",
        "geo": "ITA",
        "period": date(2021, 1, 1),
        "freq": "A",
        "value": 2.1,
        "unit": "current US$",
        "obs_status": None,
        "vintage": DAY_1,
    }


def test_verify_matches_duckdb_with_the_raw_recount(storage: Storage) -> None:
    collect(FakeWorldBank(), storage)
    result: VerifyResult = sync(verify_source(make_source(), storage))
    assert (result.vintage, result.rows_duckdb, result.rows_from_raw) == (DAY_1, 6, 6)


def test_verify_catches_a_tampered_clean_file(storage: Storage, tmp_path: Path) -> None:
    collect(FakeWorldBank(), storage)
    path = tmp_path / "bucket" / CLEAN
    table = pq.read_table(path)
    pq.write_table(table.slice(0, 5), path)
    with pytest.raises(ValidationError, match="DuckDB counts 5"):
        sync(verify_source(make_source(), storage))


def test_unchanged_source_writes_nothing(storage: Storage) -> None:
    api = FakeWorldBank()
    collect(api, storage)
    before = keys(storage)
    result = collect(api, storage, vintage=DAY_2)
    assert result.status == "unchanged"
    assert keys(storage) == before
    assert api.requests[-1] == "sources/2"


def test_changed_catalogue_date_collects_a_new_vintage(storage: Storage) -> None:
    api = FakeWorldBank()
    collect(api, storage)
    api.lastupdated = "2026-10-20"
    result = collect(api, storage, vintage=DAY_2)
    assert result.vintage == DAY_2
    assert "clean/economy/worldbank__wdi/vintage=2026-10-20/part-0.parquet" in keys(storage)
    assert CLEAN in keys(storage)


def test_changed_indicator_list_collects_a_new_vintage(storage: Storage) -> None:
    api = FakeWorldBank()
    collect(api, storage)
    smaller = make_source(
        options={"source_id": 2, "start_year": 2020, "indicators": ["SP.POP.TOTL"]}
    )
    assert collect(api, storage, vintage=DAY_2, source=smaller).status == "collected"


def test_existing_vintage_is_never_overwritten(storage: Storage) -> None:
    collect(FakeWorldBank(), storage)
    before = {k: sync(storage.get(k)) for k in keys(storage, "raw/") + [CLEAN]}
    with pytest.raises(VintageExistsError):
        collect(FakeWorldBank(), storage, force=True)
    assert {k: sync(storage.get(k)) for k in before} == before
    assert [run["status"] for run in runs(storage)] == ["collected", "failed"]


def test_transient_errors_are_retried(storage: Storage) -> None:
    api = FakeWorldBank(fail_first={"sources/2", "country/all/indicator/SP.POP.TOTL"})
    assert collect(api, storage).status == "collected"


def test_html_challenge_page_is_never_stored_as_data(storage: Storage) -> None:
    api = FakeWorldBank(html_for={"country/all/indicator/SP.POP.TOTL"})
    with pytest.raises(SourceError, match="not JSON"):
        collect(api, storage)
    assert not keys(storage, "raw/")
    assert not keys(storage, "clean/")
    assert runs(storage)[0]["status"] == "failed"


def test_truncated_response_fails_the_run(storage: Storage) -> None:
    with pytest.raises(SourceError, match="header says"):
        collect(FakeWorldBank(truncate={"SP.POP.TOTL"}), storage)
    assert not keys(storage, "clean/")


def test_duplicate_rows_block_the_clean_write(storage: Storage) -> None:
    api = FakeWorldBank()
    code = "SP.POP.TOTL"
    api.rows[code].append(record(code, "ITA", "2021", 59.0))
    with pytest.raises(ValidationError, match="duplicate"):
        collect(api, storage)
    assert f"{RAW}_manifest.json" in keys(storage)
    assert not keys(storage, "clean/")


def test_missing_indicator_fails_before_any_write(storage: Storage) -> None:
    source = make_source(options={"source_id": 2, "start_year": 2020, "indicators": ["XX.MISSING"]})
    with pytest.raises(SourceError, match="XX.MISSING"):
        collect(FakeWorldBank(), storage, source=source)
    assert not keys(storage, "raw/")


def test_raw_is_not_stored_when_keep_raw_is_false(storage: Storage) -> None:
    collect(FakeWorldBank(), storage, source=make_source(keep_raw=False))
    assert keys(storage, RAW) == [f"{RAW}_manifest.json"]
    result: VerifyResult = sync(verify_source(make_source(keep_raw=False), storage))
    assert result.rows_from_raw is None
    assert result.rows_duckdb == 6
