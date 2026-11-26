"""Reading the clean zone for the insights stage, with a record of exactly what was read.

Every object read is kept as a `Read`: its key and the SHA-256 of the bytes as they came back.
Candidates carry those reads as their evidence, and the checker reads the same keys again, from
scratch, and refuses a candidate whose bytes no longer hash the same.

Only sources whose licence allows values are loaded at all (`licence.values_allowed`). The IMF
sources, `ask` under their licence, never reach a detector, so no insight can quote them.
"""

import hashlib
import io
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pyarrow.parquet as pq

from dome_open_data import alfred, catalog, licence
from dome_open_data.registry import Source
from dome_open_data.storage import Storage

Point = tuple[date, float]


class DataError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Read:
    """One object read from the store, as evidence."""

    key: str
    sha256: str

    def as_json(self) -> dict[str, str]:
        return {"key": self.key, "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class Series:
    series_id: str
    title: str
    unit: str
    freq: str
    points: list[Point]

    def at(self, period: date) -> float | None:
        for when, value in self.points:
            if when == period:
                return value
        return None

    def until(self, period: date) -> list[Point]:
        return [p for p in self.points if p[0] <= period]

    @property
    def last(self) -> Point:
        return self.points[-1]


@dataclass(frozen=True, slots=True)
class Snapshot:
    """One vintage of a source collected as a whole (every source but ALFRED)."""

    source: Source
    vintage: date
    read: Read
    series: dict[str, Series]


@dataclass(frozen=True, slots=True)
class Versions:
    """Every publication of one ALFRED series that the store holds, by publication date."""

    source: Source
    code: str
    series_id: str
    versions: dict[date, list[Point]]
    raw: dict[date, Read] = field(default_factory=dict)

    @property
    def dates(self) -> list[date]:
        return sorted(self.versions)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _allowed(source: Source) -> None:
    if not licence.values_allowed(source):
        raise licence.LicenceError(
            f"{source.id} is {source.licence.commercial_ok!r}: its values are not read for insights"
        )


async def _titles(storage: Storage, source: Source) -> dict[str, dict[str, Any]]:
    data = await storage.get(f"catalog/series/source={source.id}/part-0.parquet")
    if data is None:
        return {}
    return {row["series_id"]: row for row in pq.read_table(io.BytesIO(data)).to_pylist()}


def snapshot_key(source: Source, vintage: date) -> str:
    return f"{source.clean_prefix}/vintage={vintage.isoformat()}/part-0.parquet"


async def snapshot(storage: Storage, source: Source, vintage: date | None = None) -> Snapshot:
    """A vintage of a source (the newest by default), every series sorted by period."""
    _allowed(source)
    if source.connector == "alfred":
        raise DataError(f"{source.id} is dated by publication: use versions()")
    if vintage is None:
        held = await catalog.vintages(storage, source)
        if not held:
            raise DataError(f"{source.id} has never collected")
        vintage = held[-1]
    key = snapshot_key(source, vintage)
    data = await storage.get(key)
    if data is None:
        raise DataError(f"{key} is not in the store")
    rows = pq.read_table(
        io.BytesIO(data), columns=["series_id", "period", "freq", "value", "unit"]
    ).to_pylist()
    titles = await _titles(storage, source)
    grouped: dict[str, list[Point]] = {}
    meta: dict[str, tuple[str, str]] = {}
    for row in rows:
        grouped.setdefault(row["series_id"], []).append((row["period"], float(row["value"])))
        meta.setdefault(row["series_id"], (row["unit"], row["freq"]))
    series = {
        sid: Series(
            series_id=sid,
            title=str(titles.get(sid, {}).get("title") or sid),
            unit=meta[sid][0],
            freq=meta[sid][1],
            points=sorted(points),
        )
        for sid, points in grouped.items()
    }
    return Snapshot(source=source, vintage=vintage, read=Read(key, _sha(data)), series=series)


async def versions(storage: Storage, source: Source, code: str) -> Versions:
    """Every committed publication of one ALFRED series, read only through its manifests."""
    _allowed(source)
    prefix = f"fred.alfred.{code}."
    sid = ""
    found: dict[date, list[Point]] = {}
    for key in await alfred.committed_clean_keys(storage, source, code):
        data = await storage.get(key)
        if data is None:
            raise DataError(f"{key} is committed but missing")
        table = pq.read_table(io.BytesIO(data), columns=["series_id", "period", "value", "vintage"])
        for row in table.to_pylist():
            if row["series_id"].startswith(prefix):
                sid = row["series_id"]
                found.setdefault(row["vintage"], []).append((row["period"], float(row["value"])))
    committed = alfred.held(await alfred.manifests(storage, source, code))
    raw = {
        published: Read(str(entry["raw_key"]), str(entry["sha256"]))
        for published, entry in committed.items()
    }
    return Versions(
        source=source,
        code=code,
        series_id=sid,
        versions={d: sorted(points) for d, points in found.items()},
        raw=raw,
    )


class Reader:
    """Loads each snapshot or ALFRED series once. The checker builds its own, never shares one."""

    def __init__(self, storage: Storage, sources: Mapping[str, Source]) -> None:
        self.storage = storage
        self.sources = sources
        self._snapshots: dict[tuple[str, date | None], Snapshot] = {}
        self._versions: dict[tuple[str, str], Versions] = {}

    def source(self, source_id: str) -> Source:
        if source_id not in self.sources:
            raise DataError(f"{source_id} is not in the registry")
        return self.sources[source_id]

    async def snapshot(self, source_id: str, vintage: date | None = None) -> Snapshot:
        cache_key = (source_id, vintage)
        if cache_key not in self._snapshots:
            snap = await snapshot(self.storage, self.source(source_id), vintage)
            self._snapshots[cache_key] = snap
            self._snapshots[(source_id, snap.vintage)] = snap
        return self._snapshots[cache_key]

    async def versions(self, source_id: str, code: str) -> Versions:
        cache_key = (source_id, code)
        if cache_key not in self._versions:
            self._versions[cache_key] = await versions(self.storage, self.source(source_id), code)
        return self._versions[cache_key]
