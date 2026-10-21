"""SDMX REST APIs (the `sdmx` connector), read as SDMX-CSV with `detail=dataonly`.

Dialects differ in three places:
- `imf` (api.imf.org, SDMX 2.1): CSV by `Accept` header. Change signal: the versions of the
  flow and of its named vintage flows (`WEO_2026_APR_VINTAGE`...). `dataonly` drops the unit
  attributes, so titles and units come from `options.series`.
- `ecb` (data-api.ecb.europa.eu): CSV by `format=csvdata`. Change signal: `Last-Modified`, which
  follows the data (stable across calls). Titles and units come from the latest observation
  fetched with full detail, kept as the raw file `_series.csv`.
`updatedAfter` is not used: the ECB answers 404, not 304, when nothing changed.
"""

import csv
import hashlib
import io
import json
import re
from collections import Counter
from collections.abc import Iterator, Mapping
from datetime import date
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator

from dome_open_data import crosswalk
from dome_open_data.connectors import Collected, RawFile, SeriesMeta
from dome_open_data.http import CSV_ACCEPT_IMF, RateLimiter, SourceError, get, require_csv
from dome_open_data.model import Observation, parse_sdmx_period
from dome_open_data.registry import Source

SDMX_DIALECTS = {"imf", "ecb"}
MISSING = {"", "NaN", "NA", "nan"}
SERIES_FILE = "_series.csv"
_DATAFLOW = re.compile(r"<str:Dataflow\b([^>]*)>")
_ATTRIBUTE = re.compile(r'(\w+)="([^"]*)"')


class SeriesSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    title: str = Field(min_length=1)
    unit: str


class SdmxOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    flow: str = Field(min_length=1)
    key: str = Field(min_length=1)
    start_period: str | None = None
    freq_dimension: str
    code_dimensions: list[str] = Field(min_length=1)
    geo_dimension: str | None = None
    geo: str | None = None
    series: dict[str, SeriesSpec] = Field(default_factory=dict)
    unit_template: str | None = None
    projection_from_vintage_year: bool = False
    source_url: str = Field(pattern=r"^https://")

    @model_validator(mode="after")
    def _one_geo(self) -> "SdmxOptions":
        if (self.geo is None) == (self.geo_dimension is None):
            raise ValueError("exactly one of geo, geo_dimension")
        return self


class Sdmx:
    def __init__(self, source: Source) -> None:
        self.source = source
        self.options = SdmxOptions.model_validate(source.options)
        self.limiter = RateLimiter(source.rate_limit.per_second)
        self.dialect = str(source.dialect)
        self.geo_map = crosswalk.publisher_codes("imf") if self.dialect == "imf" else {}
        if self.dialect == "imf" and not self.options.series:
            raise SourceError(f"{source.id}: imf sources need options.series (titles and units)")

    def _selection(self) -> str:
        options = self.options.model_dump(mode="json")
        return hashlib.sha256(json.dumps(options, sort_keys=True).encode()).hexdigest()[:12]

    def _data_request(
        self, extra: dict[str, Any], detail: str | None
    ) -> tuple[str, dict[str, Any], dict[str, str] | None]:
        options = self.options
        params: dict[str, Any] = dict(extra)
        if detail:
            params["detail"] = detail
        if options.start_period and "lastNObservations" not in params:
            params["startPeriod"] = options.start_period
        if self.dialect == "imf":
            return (
                f"{self.source.endpoint}data/{options.flow}/{options.key}",
                params,
                CSV_ACCEPT_IMF,
            )
        params["format"] = "csvdata"
        return f"{self.source.endpoint}{options.flow}/{options.key}", params, None

    async def _imf_flows(self, client: httpx.AsyncClient) -> list[str]:
        agency, flow_id = self.options.flow.split(",")[:2]
        url = f"{self.source.endpoint}dataflow/{agency}/all/latest"
        response = await get(client, url, self.limiter)
        if "xml" not in response.headers.get("content-type", ""):
            raise SourceError(f"GET {url} did not return SDMX-ML")
        flows = []
        for match in _DATAFLOW.finditer(response.text):
            attributes = dict(_ATTRIBUTE.findall(match[1]))
            name = attributes.get("id", "")
            if name == flow_id or name.startswith(f"{flow_id}_"):
                flows.append(f"{name}@{attributes.get('version', '')}")
        if not any(f.startswith(f"{flow_id}@") for f in flows):
            raise SourceError(f"{url}: dataflow {flow_id} not listed")
        return sorted(flows)

    async def change_token(self, client: httpx.AsyncClient, source: Source) -> str:
        if self.dialect == "imf":
            return f"{';'.join(await self._imf_flows(client))}|{self._selection()}"
        url, params, headers = self._data_request({"lastNObservations": 1}, None)
        response = await get(client, url, self.limiter, params=params, headers=headers)
        modified = response.headers.get("last-modified")
        if not modified:
            raise SourceError(f"GET {url}: no Last-Modified header to detect changes")
        return f"{modified}|{self._selection()}"

    async def collect(self, client: httpx.AsyncClient, source: Source, vintage: date) -> Collected:
        collected = Collected()
        specs: dict[str, SeriesSpec] = dict(self.options.series)
        if self.dialect == "ecb":
            url, params, headers = self._data_request({"lastNObservations": 1}, None)
            response = await get(client, url, self.limiter, params=params, headers=headers)
            text = require_csv(response)
            collected.raw_files.append(RawFile(SERIES_FILE, str(response.url), response.content))
            specs = self._ecb_specs(text) | specs

        url, params, headers = self._data_request({}, "dataonly")
        response = await get(client, url, self.limiter, params=params, headers=headers)
        require_csv(response)
        collected.raw_files.append(RawFile("data.csv", str(response.url), response.content))

        dropped: Counter[str] = Counter()
        seen: dict[str, SeriesMeta] = {}
        for row, verdict in self._rows(response.content):
            collected.rows_raw += 1
            if verdict:
                dropped[verdict] += 1
                continue
            code = self._code(row)
            spec = specs.get(code)
            if spec is None:
                raise SourceError(f"{source.id}: no title and unit for series code {code}")
            geo = self._geo(row)
            period, freq = parse_sdmx_period(row["TIME_PERIOD"], row[self.options.freq_dimension])
            status = row.get("OBS_STATUS") or None
            if self.options.projection_from_vintage_year and period.year >= vintage.year:
                status = "projection"
            series_id = f"{source.publisher}.{source.dataset}.{code}.{geo}.{freq}"
            collected.observations.append(
                Observation(
                    series_id, geo, period, freq, float(row["OBS_VALUE"]), spec.unit, status
                )
            )
            if series_id not in seen:
                seen[series_id] = SeriesMeta(
                    series_id=series_id,
                    code=code,
                    geo=geo,
                    freq=freq,
                    title=spec.title,
                    definition=None,
                    unit=spec.unit,
                    seasonal_adjustment=None,
                    source_url=self.options.source_url,
                )
        collected.series = list(seen.values())
        collected.dropped = dict(dropped)
        return collected

    def count_clean_rows(self, files: Mapping[str, bytes]) -> int:
        if "data.csv" not in files:
            raise SourceError(f"{self.source.id}: raw data.csv missing")
        return sum(1 for _, verdict in self._rows(files["data.csv"]) if not verdict)

    def _rows(self, content: bytes) -> Iterator[tuple[dict[str, str], str | None]]:
        """Every data row with the reason it is dropped, or None when it is kept."""
        reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
        needed = {"TIME_PERIOD", "OBS_VALUE", self.options.freq_dimension}
        needed |= set(self.options.code_dimensions)
        if self.options.geo_dimension:
            needed.add(self.options.geo_dimension)
        missing = needed - set(reader.fieldnames or [])
        if missing:
            raise SourceError(f"{self.source.id}: CSV columns missing {sorted(missing)}")
        for row in reader:
            if row["OBS_VALUE"].strip() in MISSING:
                yield row, "null_value"
            elif not self._geo(row):
                yield row, "no_geo"
            else:
                yield row, None

    def _code(self, row: Mapping[str, str]) -> str:
        return ".".join(row[d] for d in self.options.code_dimensions)

    def _geo(self, row: Mapping[str, str]) -> str:
        if self.options.geo:
            return self.options.geo
        code = row[str(self.options.geo_dimension)].strip()
        return self.geo_map.get(code, code)

    def _ecb_specs(self, text: str) -> dict[str, SeriesSpec]:
        specs: dict[str, SeriesSpec] = {}
        for row in csv.DictReader(io.StringIO(text)):
            template = self.options.unit_template or "{UNIT}"
            try:
                unit = template.format(**row)
            except KeyError as exc:
                raise SourceError(f"{self.source.id}: unit_template needs column {exc}") from exc
            specs[self._code(row)] = SeriesSpec(
                title=row.get("TITLE") or self._code(row), unit=unit
            )
        return specs
