"""Collecting ALFRED: every published version of a series, kept by its publication date.

The other sources are collected as one vintage per run, dated by the day DOME collected it. Here
the publisher dates the vintages itself, so each object carries two dates: the **publication
date** (ALFRED's vintage) in its key, and DOME's **collection time** in the manifest beside the
SHA-256 of the bytes as received. A backfill is one run; a normal run fetches only the
publication dates not yet held.

Layout, per series:

```
raw/fred/alfred/series=<ID>/vintage=<publication date>/<ID>_<YYYYMMDD>.csv.gz   bytes as received
raw/fred/alfred/series=<ID>/run=<UTC time>-<suffix>/_manifest.json               the commit record
clean/economy/fred__alfred/series=<ID>/run=<UTC time>-<suffix>/part-0.parquet    rows of that run
```

**The manifest is written last and is what makes a run's work count.** A run that dies part way
leaves raw and clean objects with no manifest beside them; they are never read (readers ask
`committed_clean_keys`, not a glob), and the next run fetches those dates again. Nothing is
overwritten and nothing is deleted, so a crash can cost a refetch but never corrupt the record.

**Write once.** Each run also fetches the newest publication date it already holds and checks the
hash (all of them with `recheck="all"`). The same hash writes nothing. A different hash means the
archive changed a version it had already published: the new bytes are written beside the first
under a name carrying the collection time, recorded in the manifest as a conflict, kept out of
the clean zone, and the run ends with status `conflict`, which always opens an issue.
"""

import asyncio
import gzip
import hashlib
import json
import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Literal

import duckdb
import httpx
import pyarrow as pa

from dome_open_data import catalog
from dome_open_data.connectors import SeriesMeta
from dome_open_data.connectors.alfred import (
    UNIT,
    SeriesInfo,
    fetch_vintage,
    parse_vintage,
    series_id,
    series_info,
    vintage_column,
)
from dome_open_data.http import RateLimiter
from dome_open_data.model import TIMESERIES_SCHEMA, observations_table
from dome_open_data.registry import Source
from dome_open_data.storage import Storage
from dome_open_data.validate import ValidationError, validate_timeseries

log = logging.getLogger(__name__)

MANIFEST = "_manifest.json"
Recheck = Literal["none", "latest", "all"]
_RUN = re.compile(r"/run=([^/]+)/_manifest\.json$")


class ArchiveChangedError(RuntimeError):
    """ALFRED served different bytes for a publication date already held."""


def _code(source: Source, entry: Any) -> str:
    """A series is a bare code or a mapping that records its copyright check.

    A mapping must say `copyright: none`: anything else is a recorded notice, and the series is
    refused here before any request, as the connector would refuse it on reading the notes.
    """
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict) and isinstance(entry.get("code"), str):
        copyright_ = str(entry.get("copyright", "")).lower()
        if copyright_ != "none":
            raise ValueError(
                f"{source.id}: {entry['code']} records copyright {copyright_ or 'unchecked'!r}; "
                "only series with no copyright notice are collected"
            )
        return str(entry["code"])
    raise ValueError(f"{source.id}: options.series entry {entry!r} has no code")


def codes(source: Source, only: list[str] | None = None) -> list[str]:
    configured = [_code(source, entry) for entry in source.options.get("series", [])]
    if not configured:
        raise ValueError(f"{source.id}: options.series is empty")
    if only:
        unknown = sorted(set(only) - set(configured))
        if unknown:
            raise ValueError(f"{source.id}: not in options.series: {', '.join(unknown)}")
        return [code for code in configured if code in only]
    return configured


def series_prefix(source: Source, code: str) -> str:
    return f"{source.raw_prefix}/series={code}/"


def raw_key(source: Source, code: str, published: date, suffix: str = "") -> str:
    name = vintage_column(code, published) + (f".at={suffix}" if suffix else "")
    return f"{series_prefix(source, code)}vintage={published.isoformat()}/{name}.csv.gz"


def manifest_key(source: Source, code: str, run: str) -> str:
    return f"{series_prefix(source, code)}run={run}/{MANIFEST}"


def clean_key(source: Source, code: str, run: str) -> str:
    return f"{source.clean_prefix}/series={code}/run={run}/part-0.parquet"


def _run_name(at: datetime) -> str:
    return f"{at:%Y%m%dT%H%M%S%fZ}-{uuid.uuid4().hex[:8]}"


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


async def manifests(storage: Storage, source: Source, code: str) -> list[dict[str, Any]]:
    """Every committed run manifest for one series, oldest first."""
    found = []
    for key in await storage.list_keys(series_prefix(source, code)):
        if _RUN.search(key):
            data = await storage.get(key)
            if data is not None:
                found.append(json.loads(data))
    return found


def held(found: list[dict[str, Any]]) -> dict[date, dict[str, Any]]:
    """Publication date to its committed entry. A date committed twice is a bug, said loudly."""
    entries: dict[date, dict[str, Any]] = {}
    for manifest in found:
        for entry in manifest["vintages"]:
            published = date.fromisoformat(entry["publication_date"])
            if published in entries:
                raise ValidationError(
                    f"{manifest['series']} {published} is committed by two runs "
                    f"({entries[published]['run']} and {manifest['run']})"
                )
            entries[published] = {**entry, "run": manifest["run"]}
    return entries


async def vintages(storage: Storage, source: Source) -> list[date]:
    """Every publication date held, across the source's series."""
    dates: set[date] = set()
    for code in codes(source):
        dates.update(held(await manifests(storage, source, code)))
    return sorted(dates)


async def committed_clean_keys(storage: Storage, source: Source, code: str) -> list[str]:
    """The clean files a reader may use: only those a committed manifest points at."""
    return [m["clean_key"] for m in await manifests(storage, source, code) if m.get("clean_key")]


@dataclass
class SeriesRun:
    code: str
    new: list[date] = field(default_factory=list)
    verified: list[date] = field(default_factory=list)
    conflicts: list[date] = field(default_factory=list)
    rows_clean: int = 0
    bytes_raw: int = 0
    left_for_later: int = 0


@dataclass
class AlfredResult:
    source_id: str
    status: str
    series: list[SeriesRun]

    @property
    def new(self) -> int:
        return sum(len(s.new) for s in self.series)

    @property
    def newest(self) -> date | None:
        dates = [d for s in self.series for d in s.new]
        return max(dates) if dates else None


async def _write_raw(
    storage: Storage, source: Source, code: str, published: date, data: bytes, stamp: str
) -> str:
    """Create-only. A leftover from a run that died before its manifest is reused when it holds
    exactly these bytes; with other bytes it stays, and these go beside it. Returns the key."""
    key = raw_key(source, code, published)
    compressed = gzip.compress(data, mtime=0)
    existing = await storage.get(key)
    if existing is None:
        await storage.put_new(key, compressed)
    elif gzip.decompress(existing) != data:
        key = raw_key(source, code, published, stamp)
        await storage.put_new(key, compressed)
    return key


async def _collect_series(
    source: Source,
    storage: Storage,
    client: httpx.AsyncClient,
    limiter: RateLimiter,
    code: str,
    *,
    run: str,
    started: datetime,
    budget: int | None,
    recheck: Recheck,
) -> tuple[SeriesRun, SeriesInfo]:
    result = SeriesRun(code)
    info = await series_info(client, source.endpoint, code, limiter)
    committed = held(await manifests(storage, source, code))

    gone = sorted(set(committed) - set(info.dates))
    if gone:
        raise ArchiveChangedError(
            f"ALFRED no longer lists {code} publication dates held here: "
            f"{', '.join(d.isoformat() for d in gone[:5])}"
        )

    todo = [d for d in info.dates if d not in committed]
    if budget is not None and len(todo) > budget:
        result.left_for_later = len(todo) - budget
        todo = todo[:budget]

    to_check = sorted(committed)
    if recheck == "latest":
        to_check = to_check[-1:]
    elif recheck == "none":
        to_check = []

    conflicts: list[dict[str, Any]] = []
    for published in to_check:
        data, url = await fetch_vintage(client, source.endpoint, code, published, limiter)
        digest = hashlib.sha256(data).hexdigest()
        if digest == committed[published]["sha256"]:
            result.verified.append(published)
            continue
        stamp = f"{started:%Y%m%dT%H%M%SZ}"
        key = raw_key(source, code, published, stamp)
        await storage.put_new(key, gzip.compress(data, mtime=0))
        result.conflicts.append(published)
        conflicts.append(
            {
                "publication_date": published.isoformat(),
                "held_sha256": committed[published]["sha256"],
                "sha256": digest,
                "raw_key": key,
                "url": url,
            }
        )
        log.error(
            "archive changed a published version",
            extra={"series": code, "publication_date": published.isoformat()},
        )

    entries: list[dict[str, Any]] = []
    tables: list[pa.Table] = []
    for published in todo:
        data, url = await fetch_vintage(client, source.endpoint, code, published, limiter)
        observations, missing = parse_vintage(data, info, published)
        table = observations_table(observations, published)
        validate_timeseries(table, len(observations))
        key = await _write_raw(storage, source, code, published, data, f"{started:%Y%m%dT%H%M%SZ}")
        result.bytes_raw += len(data)
        tables.append(table)
        entries.append(
            {
                "publication_date": published.isoformat(),
                "collected_at": _now().isoformat(),
                "url": url,
                "raw_key": key,
                "sha256": hashlib.sha256(data).hexdigest(),
                "bytes": len(data),
                "rows_clean": table.num_rows,
                "missing_values": missing,
            }
        )
        result.new.append(published)

    if not entries and not conflicts:
        return result, info

    written_clean: str | None = None
    if tables:
        combined = pa.concat_tables(tables)
        written_clean = clean_key(source, code, run)
        await storage.put_new(written_clean, catalog.parquet_bytes(combined))
        result.rows_clean = combined.num_rows

    manifest = {
        "source_id": source.id,
        "series": code,
        "series_id": series_id(code, info.freq),
        "run": run,
        "started_at": started.isoformat(),
        "committed_at": _now().isoformat(),
        "clean_key": written_clean,
        "unit": UNIT,
        "vintages": entries,
        "conflicts": conflicts,
    }
    await storage.put_new(
        manifest_key(source, code, run), json.dumps(manifest, indent=1).encode("utf-8")
    )
    return result, info


def _meta(info: SeriesInfo, source: Source) -> SeriesMeta:
    return SeriesMeta(
        series_id=series_id(info.code, info.freq),
        code=info.code,
        geo="USA",
        freq=info.freq,
        title=info.title,
        definition=(
            f"Every published version, by publication date. Levels are in each version's own "
            f"units (today: {info.units}); compare growth rates across versions, not levels."
        ),
        unit=UNIT,
        seasonal_adjustment=info.seasonal_adjustment,
        source_url=f"https://alfred.stlouisfed.org/series?seid={info.code}",
    )


async def run_alfred(
    source: Source,
    storage: Storage,
    client: httpx.AsyncClient,
    *,
    only: list[str] | None = None,
    max_new: int | None = None,
    recheck: Recheck = "latest",
) -> AlfredResult:
    """Collect every publication date not yet held, for each configured series, serially."""
    started = _now()
    run = _run_name(datetime.now(UTC))
    limiter = RateLimiter(source.rate_limit.per_second)
    if max_new is None:
        configured = source.options.get("max_new_per_run")
        max_new = int(configured) if configured is not None else None
    budget = max_new
    runs: list[SeriesRun] = []
    infos: list[SeriesInfo] = []
    status = "failed"
    error: str | None = None
    try:
        for code in codes(source, only):
            series_run, info = await _collect_series(
                source,
                storage,
                client,
                limiter,
                code,
                run=run,
                started=started,
                budget=budget,
                recheck=recheck,
            )
            runs.append(series_run)
            infos.append(info)
            if budget is not None:
                budget = max(0, budget - len(series_run.new))
        conflicts = [f"{r.code} {d.isoformat()}" for r in runs for d in r.conflicts]
        if any(r.new for r in runs):
            newest = max(d for r in runs for d in r.new)
            await catalog.write_series(storage, source, [_meta(i, source) for i in infos], newest)
            await catalog.write_source_entry(storage, source)
        if conflicts:
            status = "conflict"
            error = (
                f"ArchiveChangedError: ALFRED changed published versions: {', '.join(conflicts)}"
            )
        else:
            status = "collected" if any(r.new for r in runs) else "unchanged"
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        result = AlfredResult(source.id, status, runs)
        token = "; ".join(
            f"{r.code} +{len(r.new)}" + (f" ({r.left_for_later} left)" if r.left_for_later else "")
            for r in runs
        )
        try:
            await catalog.write_run(
                storage,
                catalog.RunRecord(
                    source_id=source.id,
                    started_at=started,
                    finished_at=_now(),
                    status=status,
                    vintage=result.newest,
                    change_token=token or None,
                    rows_raw=sum(len(r.new) for r in runs),
                    rows_clean=sum(r.rows_clean for r in runs),
                    bytes_raw=sum(r.bytes_raw for r in runs),
                    error=error,
                ),
            )
        except Exception:
            log.exception("could not record the run", extra={"source": source.id})
    for series_run in runs:
        log.info(
            "alfred series",
            extra={
                "source": source.id,
                "series": series_run.code,
                "new": len(series_run.new),
                "verified": len(series_run.verified),
                "conflicts": len(series_run.conflicts),
                "left_for_later": series_run.left_for_later,
                "rows_clean": series_run.rows_clean,
            },
        )
    if status == "conflict":
        raise ArchiveChangedError(error)
    return result


@dataclass(frozen=True)
class AlfredVerify:
    source_id: str
    series: int
    publication_dates: int
    rows: int


async def verify_alfred(
    source: Source, storage: Storage, only: list[str] | None = None, concurrency: int = 16
) -> AlfredVerify:
    """Everything held, checked against itself.

    Each raw file must match the SHA-256 its manifest recorded, each clean file must have the
    common schema, and DuckDB must count, per publication date, exactly the rows the manifest
    says that version holds. Raw reads run in parallel: this is the store, not the publisher.
    """
    gate = asyncio.Semaphore(concurrency)
    total_dates = total_rows = series_checked = 0

    async def check_raw(entry: dict[str, Any]) -> None:
        async with gate:
            data = await storage.get(entry["raw_key"])
        if data is None:
            raise ValidationError(f"{entry['raw_key']} is missing")
        if hashlib.sha256(gzip.decompress(data)).hexdigest() != entry["sha256"]:
            raise ValidationError(f"checksum mismatch on {entry['raw_key']}")

    for code in codes(source, only):
        found = await manifests(storage, source, code)
        committed = held(found)
        if not committed:
            continue
        series_checked += 1
        await asyncio.gather(*(check_raw(entry) for entry in committed.values()))

        paths = [storage.duckdb_path(m["clean_key"]) for m in found if m.get("clean_key")]
        con = storage.duckdb()
        try:
            columns = [
                row[0]
                for row in con.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)", [paths]
                ).fetchall()
            ]
            if columns != TIMESERIES_SCHEMA.names:
                raise ValidationError(f"{source.id} {code}: clean columns {columns}")
            counted = dict(
                con.execute(
                    "SELECT vintage, count(*) FROM read_parquet(?, hive_partitioning = false) GROUP BY vintage",
                    [paths],
                ).fetchall()
            )
        except duckdb.Error as exc:
            raise ValidationError(f"{source.id} {code}: DuckDB could not read: {exc}") from exc
        finally:
            con.close()
        expected = {d: int(e["rows_clean"]) for d, e in committed.items()}
        if counted != expected:
            wrong = sorted(set(counted.items()) ^ set(expected.items()))[:5]
            raise ValidationError(f"{source.id} {code}: DuckDB and manifests disagree: {wrong}")
        total_dates += len(committed)
        total_rows += sum(expected.values())
    if series_checked == 0:
        raise ValidationError(f"{source.id}: nothing held to verify")
    log.info(
        "verified",
        extra={
            "source": source.id,
            "series": series_checked,
            "dates": total_dates,
            "rows": total_rows,
        },
    )
    return AlfredVerify(source.id, series_checked, total_dates, total_rows)
