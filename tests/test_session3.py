"""Session 3: the vintage gate, the weekly ops checks, and the ArcGIS Hub export connector."""

import asyncio
import json
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pytest

from dome_open_data import ops
from dome_open_data.connectors.bulk import hub_date, number
from dome_open_data.http import SourceError, new_client
from dome_open_data.ops import STORAGE_ALERT_BYTES, Freshness, Usage
from dome_open_data.registry import Source
from dome_open_data.runner import clean_key, run_source, verify_source
from dome_open_data.storage import Storage
from dome_open_data.validate import ValidationError, VintageStats, validate_against_previous
from tests import fakes_session2 as fake
from tests.test_runner import sync

DAY_1 = date(2026, 11, 2)
DAY_2 = date(2026, 11, 9)
ITEM = "3da2b9ca97684916b75c4013f95d18ab"
SERVICE = "https://svc.test/FeatureServer/0"


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    real_sleep = asyncio.sleep

    async def instant(delay: float, result: Any = None) -> Any:
        return await real_sleep(0, result)

    monkeypatch.setattr(asyncio, "sleep", instant)


@pytest.fixture
def storage(tmp_path: Path) -> Storage:
    return Storage.local(tmp_path / "bucket")


# --- The gate -----------------------------------------------------------------------------


def test_a_small_drop_is_left_alone() -> None:
    previous = VintageStats(vintage=DAY_1, rows=1000, series=10)
    validate_against_previous(600, 10, previous)


def test_more_than_half_the_rows_gone_blocks_the_write() -> None:
    previous = VintageStats(vintage=DAY_1, rows=1000, series=10)
    with pytest.raises(ValidationError, match="499 rows against 1000"):
        validate_against_previous(499, 10, previous)


def test_more_than_half_the_series_gone_blocks_the_write() -> None:
    previous = VintageStats(vintage=DAY_1, rows=1000, series=10)
    with pytest.raises(ValidationError, match="4 series against 10"):
        validate_against_previous(1000, 4, previous)


def test_series_are_not_compared_when_the_previous_count_is_unknown() -> None:
    previous = VintageStats(vintage=DAY_1, rows=1000, series=None)
    validate_against_previous(1000, 1, previous)


# --- The weekly checks --------------------------------------------------------------------


def _mark_collected(storage: Storage, source: Source, vintage: date) -> None:
    sync(storage.put_new(clean_key(source, vintage), b"not a real parquet"))


def test_freshness_names_the_sources_that_stopped(storage: Storage) -> None:
    fresh = portwatch_source(id_suffix="fresh")
    stale = portwatch_source(id_suffix="stale")
    never = portwatch_source(id_suffix="never")
    _mark_collected(storage, fresh, date(2026, 11, 1))
    _mark_collected(storage, stale, date(2026, 1, 1))

    report = {
        row.source_id: row
        for row in sync(ops.freshness(storage, [fresh, stale, never], date(2026, 11, 6)))
    }
    assert report[fresh.id].stale is False
    assert report[fresh.id].age_days == 5
    assert report[stale.id].stale is True
    assert report[never.id].vintage is None
    assert report[never.id].stale is True
    assert "never collected" in report[never.id].line


def test_usage_counts_objects_and_bytes(storage: Storage) -> None:
    sync(storage.put_new("raw/a", b"x" * 10))
    sync(storage.put_new("raw/b", b"y" * 25))
    report = sync(ops.usage(storage))
    assert (report.objects, report.total_bytes) == (2, 35)
    assert report.over is False


def test_the_storage_guard_trips_at_the_alert_line() -> None:
    assert Usage(objects=1, total_bytes=STORAGE_ALERT_BYTES).over is True
    assert Usage(objects=1, total_bytes=STORAGE_ALERT_BYTES - 1).over is False
    assert "alert at 8 GiB" in Usage(objects=1, total_bytes=0).line


def test_a_source_within_its_own_limit_is_not_stale() -> None:
    row = Freshness("x", "daily", date(2026, 11, 1), 5, 7)
    assert row.stale is False
    assert Freshness("x", "daily", date(2026, 11, 1), 8, 7).stale is True


# --- The ArcGIS Hub export connector ------------------------------------------------------


def portwatch_source(id_suffix: str = "", **options: Any) -> Source:
    dataset = f"portwatch{f'_{id_suffix}' if id_suffix else ''}"
    return fake.source(
        id=f"imf_{dataset}",
        name="IMF PortWatch chokepoints",
        publisher="imf",
        dataset=dataset,
        domain="trade",
        cadence="weekly",
        connector="bulk_file",
        dialect="arcgis_hub",
        endpoint="https://hub.test/",
        change_signal="dataset_version",
        options={
            "hub": {"item_id": ITEM, "layer": 0, "service_layer": SERVICE},
            "file_type": "csv",
            "freq": "D",
            "date_field": "date",
            "geo_field": "portid",
            "label_field": "portname",
            "series": {
                "n_total": {"title": "Ship transits, all types", "unit": "ships per day"},
                "capacity": {"title": "Trade volume", "unit": "metric tons per day"},
            },
        }
        | options,
    )


def chokepoint_rows(days: tuple[str, ...] = ("2026-10-24", "2026-10-25")) -> list[dict[str, Any]]:
    rows = []
    for day in days:
        for portid, portname, transits in (
            ("chokepoint1", "Suez Canal", "41"),
            ("chokepoint2", "Panama Canal", "33"),
        ):
            rows.append(
                {
                    "date": day,
                    "portid": portid,
                    "portname": portname,
                    "n_total": transits,
                    "capacity": "1234567",
                }
            )
    return rows


def hub_publisher(
    last_edit: int = 1793715201792,
    csv_rows: list[dict[str, Any]] | None = None,
    export: httpx.Response | None = None,
    layer: httpx.Response | None = None,
) -> fake.FakePublisher:
    def layer_handler(request: httpx.Request) -> httpx.Response:
        if layer is not None:
            return layer
        return httpx.Response(
            200,
            text=json.dumps(
                {"name": "Daily_Chokepoints_Data", "editingInfo": {"lastEditDate": last_edit}}
            ),
            headers={"content-type": "application/json"},
        )

    def export_handler(request: httpx.Request) -> httpx.Response:
        if export is not None:
            return export
        return httpx.Response(
            200,
            text=fake.csv_text(csv_rows if csv_rows is not None else chokepoint_rows()),
            headers={"content-type": "text/csv"},
        )

    return fake.FakePublisher(
        {
            "GET svc.test/FeatureServer/0": layer_handler,
            f"GET hub.test/api/download/v1/items/{ITEM}/csv": export_handler,
        }
    )


def collect(
    publisher: fake.FakePublisher,
    storage: Storage,
    source: Source | None = None,
    vintage: date = DAY_1,
) -> Any:
    source = source or portwatch_source()

    async def go() -> Any:
        async with new_client(publisher.transport()) as client:
            return await run_source(source, storage, client, vintage=vintage)

    return sync(go())


def test_the_chokepoint_export_lands_as_long_format(storage: Storage) -> None:
    source = portwatch_source()
    result = collect(hub_publisher(), storage, source)

    # 2 days x 2 chokepoints x 2 measures
    assert (result.status, result.rows_clean) == ("collected", 8)
    verified = sync(verify_source(source, storage))
    assert verified.rows_duckdb == 8

    import io

    import pyarrow.parquet as pq

    data = sync(storage.get(clean_key(source, DAY_1)))
    assert data is not None
    table = pq.read_table(io.BytesIO(data)).to_pylist()
    suez = [row for row in table if row["geo"] == "chokepoint1" and row["freq"] == "D"]
    assert len(suez) == 4
    transit = next(row for row in suez if row["series_id"].endswith("n_total.chokepoint1.D"))
    assert transit["value"] == 41.0
    assert transit["period"] == date(2026, 10, 24)
    assert transit["unit"] == "ships per day"


def test_the_place_name_goes_into_the_series_title(storage: Storage) -> None:
    publisher = hub_publisher()
    source = portwatch_source()
    collect(publisher, storage, source)

    import io

    import pyarrow.parquet as pq

    data = sync(storage.get(f"catalog/series/source={source.id}/part-0.parquet"))
    assert data is not None
    titles = {row["series_id"]: row["title"] for row in pq.read_table(io.BytesIO(data)).to_pylist()}
    assert "Ship transits, all types, Suez Canal" in titles.values()
    assert "Trade volume, Panama Canal" in titles.values()


def test_an_unchanged_layer_writes_nothing(storage: Storage) -> None:
    source = portwatch_source()
    assert collect(hub_publisher(), storage, source).status == "collected"
    again = collect(hub_publisher(), storage, source, vintage=DAY_2)
    assert again.status == "unchanged"
    assert sync(storage.get(clean_key(source, DAY_2))) is None


def test_an_edited_layer_collects_a_new_vintage(storage: Storage) -> None:
    source = portwatch_source()
    collect(hub_publisher(), storage, source)
    result = collect(hub_publisher(last_edit=1793715299999), storage, source, vintage=DAY_2)
    assert result.status == "collected"


def test_an_export_that_is_still_generating_fails_cleanly(storage: Storage) -> None:
    pending = httpx.Response(
        202,
        text=json.dumps({"status": "PagingData", "progressInPercent": 5}),
        headers={"content-type": "application/json"},
    )
    with pytest.raises(SourceError, match="the export is PagingData, 5% generated"):
        collect(hub_publisher(export=pending), storage)
    assert sync(storage.list_keys("clean/")) == []


def test_an_arcgis_error_under_a_200_is_not_treated_as_data(storage: Storage) -> None:
    refusal = httpx.Response(
        200,
        text=json.dumps(
            {
                "error": {
                    "code": 429,
                    "message": "Unable to perform query. Too many requests.",
                    "details": ["API calls quota exceeded"],
                }
            }
        ),
        headers={"content-type": "application/json"},
    )
    with pytest.raises(SourceError, match="ArcGIS error 429"):
        collect(hub_publisher(layer=refusal), storage)


def test_a_layer_without_an_edit_date_cannot_be_watched(storage: Storage) -> None:
    bare = httpx.Response(
        200, text=json.dumps({"name": "x"}), headers={"content-type": "application/json"}
    )
    with pytest.raises(SourceError, match="no editingInfo.lastEditDate"):
        collect(hub_publisher(layer=bare), storage)


def test_the_gate_blocks_a_collapsed_second_vintage(storage: Storage) -> None:
    source = portwatch_source()
    collect(hub_publisher(), storage, source)
    shrunk = hub_publisher(
        last_edit=1793715299999, csv_rows=chokepoint_rows(days=("2026-10-24",))[:1]
    )
    with pytest.raises(ValidationError, match="rows against 8"):
        collect(shrunk, storage, source, vintage=DAY_2)
    # The previous vintage is untouched and the new one was never written.
    assert sync(storage.get(clean_key(source, DAY_1))) is not None
    assert sync(storage.get(clean_key(source, DAY_2))) is None


def test_missing_columns_fail_before_anything_is_stored(storage: Storage) -> None:
    rows = [{"date": "2026-10-24", "portid": "chokepoint1", "n_total": "4"}]
    with pytest.raises(SourceError, match=r"columns missing \['capacity', 'portname'\]"):
        collect(hub_publisher(csv_rows=rows), storage)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2026-10-25", date(2026, 10, 25)),
        ("2026/10/25", date(2026, 10, 25)),
        ("2026-10-25 00:00:00+00", date(2026, 10, 25)),
        ("1761350400000", date(2025, 10, 25)),
        ("", None),
        ("last Tuesday", None),
    ],
)
def test_hub_dates_in_every_shape_the_hub_writes(text: str, expected: date | None) -> None:
    assert hub_date(text) == expected


@pytest.mark.parametrize(
    ("cell", "expected"),
    [("3", 3.0), ("3.5", 3.5), ("", None), (None, None), ("n/a", None)],
)
def test_numbers_that_are_not_numbers_are_dropped(cell: str | None, expected: float | None) -> None:
    assert number(cell) == expected
