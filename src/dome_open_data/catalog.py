"""catalog/ holds one partition per source (`source=<id>/`), so parallel source jobs never
write the same object. Read everything at once with a glob, for example
`read_parquet('r2://dome-open-data/catalog/runs/*/*.parquet', hive_partitioning = true)`."""

import io
import re
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from dome_open_data.connectors import SeriesMeta
from dome_open_data.model import SERIES_SCHEMA, schema_of
from dome_open_data.registry import Source
from dome_open_data.storage import Storage

RUNS_SCHEMA = schema_of(
    pa.field("source_id", pa.string(), nullable=False),
    pa.field("started_at", pa.timestamp("s", tz="UTC"), nullable=False),
    pa.field("finished_at", pa.timestamp("s", tz="UTC"), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("vintage", pa.date32(), nullable=True),
    pa.field("change_token", pa.string(), nullable=True),
    pa.field("rows_raw", pa.int64(), nullable=False),
    pa.field("rows_clean", pa.int64(), nullable=False),
    pa.field("bytes_raw", pa.int64(), nullable=False),
    pa.field("error", pa.string(), nullable=True),
)

SOURCES_SCHEMA = schema_of(
    pa.field("id", pa.string(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
    pa.field("publisher", pa.string(), nullable=False),
    pa.field("dataset", pa.string(), nullable=False),
    pa.field("domain", pa.string(), nullable=False),
    pa.field("connector", pa.string(), nullable=False),
    pa.field("endpoint", pa.string(), nullable=False),
    pa.field("cadence", pa.string(), nullable=False),
    pa.field("change_signal", pa.string(), nullable=False),
    pa.field("keep_raw", pa.bool_(), nullable=False),
    pa.field("expected_staleness_days", pa.int32(), nullable=False),
    pa.field("licence", pa.string(), nullable=False),
    pa.field("commercial_ok", pa.string(), nullable=False),
    pa.field("share_alike", pa.bool_(), nullable=False),
    pa.field("attribution", pa.string(), nullable=False),
    pa.field("updated_at", pa.timestamp("s", tz="UTC"), nullable=False),
)

_VINTAGE = re.compile(r"/vintage=(\d{4}-\d{2}-\d{2})/")


@dataclass(frozen=True)
class RunRecord:
    source_id: str
    started_at: datetime
    finished_at: datetime
    status: str
    vintage: date | None
    change_token: str | None
    rows_raw: int
    rows_clean: int
    bytes_raw: int
    error: str | None


def parquet_bytes(table: pa.Table) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    return buffer.getvalue()


def runs_prefix(source_id: str) -> str:
    return f"catalog/runs/source={source_id}/"


async def write_run(storage: Storage, run: RunRecord) -> None:
    table = pa.Table.from_pylist([asdict(run)], schema=RUNS_SCHEMA)
    # Sorts by start time; the suffix keeps two runs that start in the same second apart.
    name = f"run={run.started_at:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}.parquet"
    await storage.put_new(f"{runs_prefix(run.source_id)}{name}", parquet_bytes(table))


async def last_collected_token(storage: Storage, source_id: str) -> str | None:
    keys = [k for k in await storage.list_keys(runs_prefix(source_id)) if k.endswith(".parquet")]
    for key in reversed(keys):
        data = await storage.get(key)
        if data is None:
            continue
        row = pq.read_table(io.BytesIO(data)).to_pylist()[0]
        if row["status"] == "collected":
            token: str | None = row["change_token"]
            return token
    return None


async def write_source_entry(storage: Storage, source: Source) -> None:
    row = {
        "id": source.id,
        "name": source.name,
        "publisher": source.publisher,
        "dataset": source.dataset,
        "domain": source.domain,
        "connector": source.connector,
        "endpoint": source.endpoint,
        "cadence": source.cadence,
        "change_signal": source.change_signal,
        "keep_raw": source.keep_raw,
        "expected_staleness_days": source.expected_staleness_days,
        "licence": source.licence.name,
        "commercial_ok": source.licence.commercial_ok,
        "share_alike": source.licence.share_alike,
        "attribution": source.licence.attribution,
        "updated_at": datetime.now(UTC).replace(microsecond=0),
    }
    table = pa.Table.from_pylist([row], schema=SOURCES_SCHEMA)
    await storage.put(f"catalog/sources/source={source.id}/part-0.parquet", parquet_bytes(table))


async def write_series(
    storage: Storage, source: Source, series: list[SeriesMeta], vintage: date
) -> None:
    rows = [
        {
            **asdict(meta),
            "source_id": source.id,
            "licence": source.licence.name,
            "attribution": source.licence.attribution,
            "vintage": vintage,
        }
        for meta in series
    ]
    table = pa.Table.from_pylist(rows, schema=SERIES_SCHEMA)
    await storage.put(f"catalog/series/source={source.id}/part-0.parquet", parquet_bytes(table))


async def vintages(storage: Storage, source: Source) -> list[date]:
    found = {
        date.fromisoformat(match[1])
        for key in await storage.list_keys(f"{source.clean_prefix}/")
        if (match := _VINTAGE.search(key))
    }
    return sorted(found)
