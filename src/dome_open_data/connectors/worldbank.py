"""World Bank Indicators API v2 (the `rest` connector, `worldbank` dialect).

Change signal: `lastupdated` on the source catalogue entry (`/v2/sources/{id}`). The API's
HTTP Last-Modified header is the CDN cache time, so it says nothing about the data.
"""

import asyncio
import hashlib
import json
import re
from collections import Counter
from collections.abc import Mapping
from datetime import date
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field

from dome_open_data.connectors import Collected, RawFile, SeriesMeta
from dome_open_data.http import RateLimiter, SourceError, get, require_json
from dome_open_data.model import Observation, parse_period
from dome_open_data.registry import Source

PAGE_SIZE = 20000
MAX_IN_FLIGHT = 4
_FIRST_PARENTHETICAL = re.compile(r"\(([^()]+)\)")


class WorldBankOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: int
    start_year: int = Field(ge=1900)
    indicators: list[str] = Field(min_length=1)


def _options(source: Source) -> WorldBankOptions:
    options = WorldBankOptions.model_validate(source.options)
    duplicates = [code for code, n in Counter(options.indicators).items() if n > 1]
    if duplicates:
        raise SourceError(f"{source.id}: duplicate indicators {duplicates}")
    return options


def _payload(body: Any, url: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    # Success is [header, rows]; an error is [{"message": [...]}] with status 200.
    if not isinstance(body, list) or not body or not isinstance(body[0], dict):
        raise SourceError(f"{url}: unexpected response shape")
    header = body[0]
    if "message" in header:
        raise SourceError(f"{url}: API error {header['message']}")
    if len(body) != 2:
        raise SourceError(f"{url}: unexpected response shape")
    return header, body[1] or []


def unit_from_title(title: str) -> str:
    match = _FIRST_PARENTHETICAL.search(title)
    return match[1].strip() if match else ""


def is_clean_row(record: dict[str, Any]) -> bool:
    return record.get("value") is not None and bool(record.get("countryiso3code"))


class WorldBank:
    async def change_token(self, client: httpx.AsyncClient, source: Source) -> str:
        options = _options(source)
        limiter = RateLimiter(source.rate_limit.per_second)
        url = f"{source.endpoint}sources/{options.source_id}"
        response = await get(client, url, limiter, params={"format": "json"})
        _, rows = _payload(require_json(response), url)
        if not rows or not rows[0].get("lastupdated"):
            raise SourceError(f"{url}: no lastupdated in the source catalogue")
        selection = hashlib.sha256(",".join(sorted(options.indicators)).encode()).hexdigest()
        return f"{rows[0]['lastupdated']}|from:{options.start_year}|indicators:{selection[:12]}"

    async def collect(self, client: httpx.AsyncClient, source: Source, vintage: date) -> Collected:
        options = _options(source)
        limiter = RateLimiter(source.rate_limit.per_second)
        gate = asyncio.Semaphore(MAX_IN_FLIGHT)
        collected = Collected()

        meta_url = f"{source.endpoint}source/{options.source_id}/indicator"
        response = await get(
            client, meta_url, limiter, params={"format": "json", "per_page": PAGE_SIZE}
        )
        collected.raw_files.append(RawFile("_indicators.json", str(response.url), response.content))
        _, meta_rows = _payload(require_json(response), meta_url)
        metadata = {row["id"]: row for row in meta_rows}
        missing = sorted(set(options.indicators) - metadata.keys())
        if missing:
            raise SourceError(
                f"{source.id}: indicators not in source {options.source_id}: {missing}"
            )

        async def fetch(code: str) -> list[RawFile]:
            url = f"{source.endpoint}country/all/indicator/{code}"
            files: list[RawFile] = []
            page, pages, received, total = 1, 1, 0, 0
            while page <= pages:
                params = {
                    "format": "json",
                    "source": options.source_id,
                    "date": f"{options.start_year}:{vintage.year}",
                    "per_page": PAGE_SIZE,
                    "page": page,
                }
                async with gate:
                    resp = await get(client, url, limiter, params=params)
                header, rows = _payload(require_json(resp), url)
                pages, total = int(header["pages"]), int(header["total"])
                received += len(rows)
                files.append(RawFile(f"{code}.p{page}.json", str(resp.url), resp.content))
                page += 1
            if received != total:
                raise SourceError(f"{url}: received {received} rows, header says {total}")
            return files

        for files in await asyncio.gather(*(fetch(code) for code in options.indicators)):
            collected.raw_files.extend(files)

        dropped: Counter[str] = Counter()
        seen_series: dict[str, SeriesMeta] = {}
        for raw in collected.raw_files:
            if raw.name.startswith("_"):
                continue
            _, rows = _payload(json.loads(raw.data), raw.url)
            for record in rows:
                collected.rows_raw += 1
                if record.get("value") is None:
                    dropped["null_value"] += 1
                    continue
                if not record.get("countryiso3code"):
                    dropped["no_geo"] += 1
                    continue
                code = record["indicator"]["id"]
                meta = metadata[code]
                period, freq = parse_period(str(record["date"]))
                geo = record["countryiso3code"]
                series_id = f"{source.publisher}.{source.dataset}.{code}.{geo}.{freq}"
                unit = (meta.get("unit") or "").strip() or unit_from_title(meta["name"])
                collected.observations.append(
                    Observation(
                        series_id=series_id,
                        geo=geo,
                        period=period,
                        freq=freq,
                        value=float(record["value"]),
                        unit=unit,
                        obs_status=record.get("obs_status") or None,
                    )
                )
                if series_id not in seen_series:
                    seen_series[series_id] = SeriesMeta(
                        series_id=series_id,
                        code=code,
                        geo=geo,
                        freq=freq,
                        title=meta["name"],
                        definition=(meta.get("sourceNote") or "").strip() or None,
                        unit=unit,
                        seasonal_adjustment=None,
                        source_url=f"https://data.worldbank.org/indicator/{code}",
                    )
        collected.series = list(seen_series.values())
        collected.dropped = dict(dropped)
        return collected

    def count_clean_rows(self, files: Mapping[str, bytes]) -> int:
        count = 0
        for name, data in files.items():
            if name.startswith("_"):
                continue
            _, rows = _payload(json.loads(data), name)
            count += sum(1 for record in rows if is_clean_row(record))
        return count
