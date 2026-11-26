"""ALFRED, the St. Louis Fed's archive of every published version of a FRED series.

Two requests, both keyless (Phase 2A step 0 spike, 2026-11-25):

- `graph/api/series/?id=<ID>&mode=alfred` is the JSON the series page's own graph reads. Its
  `chart_series[0].series_objects.a.available_revision_dates` lists every publication date,
  ascending. It is undocumented, so its shape is checked here and any change fails the run loudly
  rather than collecting nothing quietly. The documented fallback is FRED API v1 with a key.
- `graph/alfredgraph.csv?id=<ID>&vintage_date=<D>&cosd=&coed=` serves the series as it stood on
  publication date D. `fredgraph.csv` on the `fred.` host ignores `vintage_date`, so the host
  matters. The CSV's value column is named `<ID>_<YYYYMMDD>`, which is checked against the date
  asked for: a second proof that the archive served what was requested.

Requests are serial: parallel ones came back empty about one time in four on 2026-11-17. They
carry the repository's honest user agent from `http.py`. **Never a browser one:** the site sits
behind Akamai Bot Manager, which blocks a browser user agent sent by a non-browser client.
"""

import csv
import io
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx

from dome_open_data.http import (
    RETRY_STATUSES,
    RateLimiter,
    SourceError,
    get,
    require_csv,
    require_json,
)
from dome_open_data.model import Observation

DATES_PATH = "graph/api/series/"
CSV_PATH = "graph/alfredgraph.csv"
FREQUENCIES = {"A", "Q", "M", "W", "D"}
# How many times an empty CSV is asked for again before the run fails.
EMPTY_ATTEMPTS = 4
_COPYRIGHT = re.compile(r"copyright|©", re.IGNORECASE)
_MISSING = {"", "."}


class ShapeError(SourceError):
    """ALFRED answered, but not in the shape this connector was written against."""


@dataclass(frozen=True, slots=True)
class SeriesInfo:
    code: str
    title: str
    freq: str
    units: str
    seasonal_adjustment: str | None
    dates: list[date]


def _shape(condition: bool, code: str, what: str) -> None:
    if not condition:
        raise ShapeError(f"ALFRED date list for {code} changed shape: {what}")


def parse_series_info(payload: Any, code: str) -> SeriesInfo:
    """Read the date list and the series description, refusing anything unexpected."""
    _shape(isinstance(payload, dict), code, "the body is not a JSON object")
    charts = payload.get("chart_series")
    _shape(isinstance(charts, list) and len(charts) > 0, code, "no chart_series list")
    objects = charts[0].get("series_objects") if isinstance(charts[0], dict) else None
    _shape(isinstance(objects, dict) and "a" in objects, code, "no series_objects.a")
    assert isinstance(objects, dict)  # for the type checker; _shape has already refused it
    entry = objects["a"]
    _shape(isinstance(entry, dict), code, "series_objects.a is not an object")
    _shape(entry.get("series_id") == code, code, f"series_id is {entry.get('series_id')!r}")
    raw_dates = entry.get("available_revision_dates")
    _shape(
        isinstance(raw_dates, list) and len(raw_dates) > 0,
        code,
        "available_revision_dates is missing or empty",
    )
    try:
        dates = [date.fromisoformat(str(text)) for text in raw_dates]
    except ValueError as exc:
        raise ShapeError(f"ALFRED date list for {code} changed shape: {exc}") from exc
    _shape(dates == sorted(set(dates)), code, "available_revision_dates is not strictly ascending")

    # A series carrying a copyright notice is not public domain, and only public-domain series
    # belong in this source (see licence.py). Refuse it rather than collect it under the wrong
    # licence.
    notes = str(entry.get("notes") or "")
    if _COPYRIGHT.search(notes):
        raise SourceError(
            f"ALFRED series {code} carries a copyright notice; it is not public domain"
        )

    freq = str(entry.get("frequency_short") or "")
    _shape(freq in FREQUENCIES, code, f"frequency_short is {freq!r}")
    season = entry.get("season_short")
    return SeriesInfo(
        code=code,
        title=str(entry.get("title") or code),
        freq=freq,
        units=str(entry.get("units") or ""),
        seasonal_adjustment=str(season) if season else None,
        dates=dates,
    )


async def series_info(
    client: httpx.AsyncClient, endpoint: str, code: str, limiter: RateLimiter
) -> SeriesInfo:
    response = await get(client, f"{endpoint}{DATES_PATH}", limiter, {"id": code, "mode": "alfred"})
    return parse_series_info(require_json(response), code)


def vintage_column(code: str, published: date) -> str:
    return f"{code}_{published:%Y%m%d}"


def csv_url(endpoint: str, code: str, published: date) -> str:
    return f"{endpoint}{CSV_PATH}?id={code}&vintage_date={published.isoformat()}&cosd=&coed="


def check_csv(data: bytes, code: str, published: date) -> int:
    """The data rows in one vintage CSV, after checking its header names that vintage."""
    rows = list(csv.reader(io.StringIO(data.decode("utf-8"))))
    if not rows:
        return 0
    expected = ["observation_date", vintage_column(code, published)]
    if rows[0] != expected:
        raise SourceError(
            f"ALFRED served {rows[0]} for {code} at {published}, expected {expected}: "
            "the archive did not serve the version asked for"
        )
    return sum(1 for row in rows[1:] if row)


# The date asked for comes from ALFRED's own list, so a 404 for it is a blip, not a missing
# version: on 2026-11-25 INDPRO at 2009-09-16 answered 404 once and 200, with the same bytes, a
# few minutes later. It is retried with the usual backoff; a 404 that lasts still fails the run.
LISTED_DATE_RETRY = frozenset(RETRY_STATUSES | {404})


async def fetch_vintage(
    client: httpx.AsyncClient,
    endpoint: str,
    code: str,
    published: date,
    limiter: RateLimiter,
) -> tuple[bytes, str]:
    """One vintage CSV, asked for again while it comes back empty or answers 404."""
    url = f"{endpoint}{CSV_PATH}"
    params = {
        "id": code,
        "vintage_date": published.isoformat(),
        "cosd": "",
        "coed": "",
    }
    for _attempt in range(EMPTY_ATTEMPTS):
        response = await get(client, url, limiter, params, retry_statuses=LISTED_DATE_RETRY)
        require_csv(response)
        # The bytes as received: they are what the SHA-256 in the manifest proves.
        data = response.content
        if check_csv(data, code, published) > 0:
            return data, csv_url(endpoint, code, published)
    raise SourceError(
        f"ALFRED returned an empty CSV for {code} at {published} {EMPTY_ATTEMPTS} times"
    )


def series_id(code: str, freq: str) -> str:
    return f"fred.alfred.{code}.USA.{freq}"


# Older vintages are in the base year of their day (chained 2000 dollars in 2008, 2017 dollars
# now), and the CSV does not say which, so a level is only comparable within one vintage. Growth
# rates compare across vintages; levels do not.
UNIT = "as_published"


def parse_vintage(data: bytes, info: SeriesInfo, published: date) -> tuple[list[Observation], int]:
    """Observations in one vintage CSV, and how many rows were missing values."""
    reader = csv.reader(io.StringIO(data.decode("utf-8")))
    next(reader)
    observations: list[Observation] = []
    missing = 0
    sid = series_id(info.code, info.freq)
    for row in reader:
        if not row:
            continue
        if len(row) != 2:
            raise SourceError(f"{info.code} at {published}: malformed row {row}")
        if row[1].strip() in _MISSING:
            missing += 1
            continue
        try:
            value = float(row[1])
            period = date.fromisoformat(row[0])
        except ValueError as exc:
            raise SourceError(f"{info.code} at {published}: unreadable row {row}") from exc
        observations.append(
            Observation(
                series_id=sid,
                geo="USA",
                period=period,
                freq=info.freq,
                value=value,
                unit=UNIT,
                obs_status=None,
            )
        )
    return observations, missing
