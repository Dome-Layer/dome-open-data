import gzip
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime

import duckdb
import httpx

from dome_open_data import catalog
from dome_open_data.connectors import connector_for
from dome_open_data.model import TIMESERIES_SCHEMA, observations_table
from dome_open_data.registry import Source
from dome_open_data.storage import Storage, VintageExistsError
from dome_open_data.validate import ValidationError, validate_timeseries

log = logging.getLogger(__name__)

MANIFEST = "_manifest.json"


@dataclass(frozen=True)
class RunResult:
    source_id: str
    status: str
    vintage: date | None
    change_token: str
    rows_raw: int = 0
    rows_clean: int = 0
    bytes_raw: int = 0


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def raw_vintage_prefix(source: Source, vintage: date) -> str:
    return f"{source.raw_prefix}/vintage={vintage.isoformat()}/"


def clean_key(source: Source, vintage: date) -> str:
    return f"{source.clean_prefix}/vintage={vintage.isoformat()}/part-0.parquet"


async def run_source(
    source: Source,
    storage: Storage,
    client: httpx.AsyncClient,
    *,
    vintage: date,
    force: bool = False,
) -> RunResult:
    connector = connector_for(source)
    started = _now()
    token: str | None = None
    rows_raw = rows_clean = bytes_raw = 0
    try:
        token = await connector.change_token(client, source)
        if not force and token == await catalog.last_collected_token(storage, source.id):
            log.info("unchanged, nothing written", extra={"source": source.id, "token": token})
            return RunResult(source.id, "unchanged", None, token)

        raw_prefix = raw_vintage_prefix(source, vintage)
        if await storage.list_keys(raw_prefix) or await storage.get(clean_key(source, vintage)):
            raise VintageExistsError(f"{source.id} vintage {vintage} already exists")

        collected = await connector.collect(client, source, vintage)
        rows_raw = collected.rows_raw

        files = []
        for raw in collected.raw_files:
            compressed = gzip.compress(raw.data, mtime=0)
            bytes_raw += len(compressed)
            files.append(
                {
                    "name": raw.name,
                    "url": raw.url,
                    "sha256": hashlib.sha256(raw.data).hexdigest(),
                    "bytes": len(raw.data),
                }
            )
            if source.keep_raw:
                await storage.put_new(f"{raw_prefix}{raw.name}.gz", compressed)

        table = observations_table(collected.observations, vintage)
        manifest = {
            "source_id": source.id,
            "vintage": vintage.isoformat(),
            "fetched_at": started.isoformat(),
            "change_token": token,
            "keep_raw": source.keep_raw,
            "rows_raw": rows_raw,
            "rows_clean": table.num_rows,
            "dropped": collected.dropped,
            "files": files,
        }
        await storage.put_new(f"{raw_prefix}{MANIFEST}", json.dumps(manifest, indent=2).encode())

        validate_timeseries(table, rows_raw - sum(collected.dropped.values()))
        await storage.put_new(clean_key(source, vintage), catalog.parquet_bytes(table))
        rows_clean = table.num_rows

        await catalog.write_series(storage, source, collected.series, vintage)
        await catalog.write_source_entry(storage, source)
    except Exception as exc:
        failed = catalog.RunRecord(
            source_id=source.id,
            started_at=started,
            finished_at=_now(),
            status="failed",
            vintage=None,
            change_token=token,
            rows_raw=rows_raw,
            rows_clean=rows_clean,
            bytes_raw=bytes_raw,
            error=f"{type(exc).__name__}: {exc}",
        )
        try:
            await catalog.write_run(storage, failed)
        except Exception:
            log.exception("could not record the failed run", extra={"source": source.id})
        raise

    await catalog.write_run(
        storage,
        catalog.RunRecord(
            source_id=source.id,
            started_at=started,
            finished_at=_now(),
            status="collected",
            vintage=vintage,
            change_token=token,
            rows_raw=rows_raw,
            rows_clean=rows_clean,
            bytes_raw=bytes_raw,
            error=None,
        ),
    )
    log.info(
        "collected",
        extra={
            "source": source.id,
            "vintage": vintage.isoformat(),
            "rows_raw": rows_raw,
            "rows_clean": rows_clean,
            "bytes_raw": bytes_raw,
            "series": len(collected.series),
        },
    )
    return RunResult(source.id, "collected", vintage, token, rows_raw, rows_clean, bytes_raw)


@dataclass(frozen=True)
class VerifyResult:
    source_id: str
    vintage: date
    rows_duckdb: int
    rows_from_raw: int | None
    rows_manifest: int


async def verify_source(
    source: Source, storage: Storage, vintage: date | None = None
) -> VerifyResult:
    """Recount the clean rows from the raw files and check DuckDB reads the same number."""
    if vintage is None:
        found = await catalog.vintages(storage, source)
        if not found:
            raise ValidationError(f"{source.id}: no clean vintage to verify")
        vintage = found[-1]

    raw_prefix = raw_vintage_prefix(source, vintage)
    manifest_bytes = await storage.get(f"{raw_prefix}{MANIFEST}")
    if manifest_bytes is None:
        raise ValidationError(f"{source.id} {vintage}: no manifest")
    manifest = json.loads(manifest_bytes)

    rows_from_raw: int | None = None
    if manifest["keep_raw"]:
        files: dict[str, bytes] = {}
        for entry in manifest["files"]:
            data = await storage.get(f"{raw_prefix}{entry['name']}.gz")
            if data is None:
                raise ValidationError(f"{source.id} {vintage}: raw file {entry['name']} missing")
            content = gzip.decompress(data)
            if hashlib.sha256(content).hexdigest() != entry["sha256"]:
                raise ValidationError(
                    f"{source.id} {vintage}: checksum mismatch on {entry['name']}"
                )
            files[entry["name"]] = content
        rows_from_raw = connector_for(source).count_clean_rows(files)

    con = storage.duckdb()
    path = storage.duckdb_path(clean_key(source, vintage))
    try:
        columns = [
            row[0]
            for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [path]).fetchall()
        ]
        if columns != TIMESERIES_SCHEMA.names:
            raise ValidationError(f"{source.id} {vintage}: clean columns {columns}")
        result = con.execute("SELECT count(*) FROM read_parquet(?)", [path]).fetchone()
    except duckdb.Error as exc:
        raise ValidationError(
            f"{source.id} {vintage}: DuckDB could not read {path}: {exc}"
        ) from exc
    finally:
        con.close()
    rows_duckdb = int(result[0]) if result else 0

    expected = {manifest["rows_clean"]} | ({rows_from_raw} if rows_from_raw is not None else set())
    if expected != {rows_duckdb}:
        raise ValidationError(
            f"{source.id} {vintage}: DuckDB counts {rows_duckdb}, raw recount {rows_from_raw}, "
            f"manifest {manifest['rows_clean']}"
        )
    log.info(
        "verified",
        extra={"source": source.id, "vintage": vintage.isoformat(), "rows": rows_duckdb},
    )
    return VerifyResult(source.id, vintage, rows_duckdb, rows_from_raw, manifest["rows_clean"])
