"""Fake SDMX and bulk-file publishers for the session 2 connectors, with tiny synthetic files."""

import csv
import gzip
import io
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import httpx
import openpyxl

from dome_open_data.registry import Source, parse_registry

LICENCE = {"name": "CC BY 4.0", "commercial_ok": "yes", "share_alike": False, "attribution": "x"}


def source(**entry: Any) -> Source:
    base: dict[str, Any] = {
        "domain": "economy",
        "cadence": "monthly",
        "rate_limit": {"per_second": 1000},
        "keep_raw": True,
        "expected_staleness_days": 30,
        "licence": LICENCE,
    }
    base.update(entry)
    return parse_registry([base])[base["id"]]


def ecb_source(**options: Any) -> Source:
    return source(
        id="ecb_fx",
        name="ECB FX",
        publisher="ecb",
        dataset="fx",
        connector="sdmx",
        dialect="ecb",
        endpoint="https://ecb.test/service/data/",
        change_signal="last_modified",
        options={
            "flow": "EXR",
            "key": "D..EUR.SP00.A",
            "freq_dimension": "FREQ",
            "geo": "EMU",
            "code_dimensions": ["CURRENCY", "CURRENCY_DENOM", "EXR_TYPE", "EXR_SUFFIX"],
            "unit_template": "{CURRENCY} per EUR",
            "source_url": "https://ecb.test/EXR",
        }
        | options,
    )


def imf_source(**options: Any) -> Source:
    return source(
        id="imf_weo",
        name="IMF WEO",
        publisher="imf",
        dataset="weo",
        connector="sdmx",
        dialect="imf",
        endpoint="https://imf.test/sdmx/2.1/",
        change_signal="dataset_version",
        options={
            "flow": "IMF.RES,WEO",
            "key": ".NGDPD+LUR.A",
            "freq_dimension": "FREQUENCY",
            "geo_dimension": "COUNTRY",
            "code_dimensions": ["INDICATOR"],
            "projection_from_vintage_year": True,
            "source_url": "https://imf.test/WEO",
            "series": {
                "NGDPD": {"title": "GDP", "unit": "U.S. dollars"},
                "LUR": {"title": "Unemployment rate", "unit": "Percent"},
            },
        }
        | options,
    )


def bulk_source(dialect: str, **options: Any) -> Source:
    ids = {"pinksheet": "worldbank", "wpp": "un", "pwt": "ggdc", "maddison": "ggdc"}
    return source(
        id=f"{ids[dialect]}_{dialect}",
        name=dialect,
        publisher=ids[dialect],
        dataset=dialect,
        connector="bulk_file",
        dialect=dialect,
        endpoint="https://dataverse.test/",
        change_signal="etag" if dialect == "wpp" else "last_modified",
        options=options,
    )


def csv_text(rows: list[dict[str, Any]]) -> str:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=list(rows[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


def xlsx(sheets: dict[str, list[list[Any]]]) -> bytes:
    workbook = openpyxl.Workbook()
    for index, (name, rows) in enumerate(sheets.items()):
        sheet = workbook.worksheets[0] if index == 0 else workbook.create_sheet()
        sheet.title = name
        for row in rows:
            sheet.append(row)
    out = io.BytesIO()
    workbook.save(out)
    return out.getvalue()


def ecb_rows(detail: bool) -> list[dict[str, Any]]:
    rows = []
    for currency, values in (("USD", ["1.10", "1.12"]), ("JPY", ["160.1", "NaN"])):
        for day, value in zip(("2026-10-19", "2026-10-20"), values, strict=True):
            row = {
                "KEY": f"EXR.D.{currency}.EUR.SP00.A",
                "FREQ": "D",
                "CURRENCY": currency,
                "CURRENCY_DENOM": "EUR",
                "EXR_TYPE": "SP00",
                "EXR_SUFFIX": "A",
                "TIME_PERIOD": day,
                "OBS_VALUE": value,
            }
            if detail:
                row |= {"TITLE": f"{currency}/Euro", "UNIT": currency}
            rows.append(row)
    return rows


def imf_rows() -> list[dict[str, Any]]:
    rows = []
    for country in ("ITA", "KOS", "G001"):
        for year, value in (("2025", "100"), ("2026", "110"), ("2027", "")):
            for indicator in ("NGDPD", "LUR"):
                rows.append(
                    {
                        "DATAFLOW": "IMF.RES:WEO(9.0.0)",
                        "COUNTRY": country,
                        "INDICATOR": indicator,
                        "FREQUENCY": "A",
                        "TIME_PERIOD": year,
                        "OBS_VALUE": value,
                    }
                )
    return rows


def dataflows(*flows: tuple[str, str]) -> str:
    items = "".join(
        f'<str:Dataflow urn="x" id="{name}" agencyID="IMF.RES" version="{version}"></str:Dataflow>'
        for name, version in flows
    )
    return f"<message:Structure><str:Dataflows>{items}</str:Dataflows></message:Structure>"


Handler = Callable[[httpx.Request], httpx.Response]


@dataclass
class FakePublisher:
    """Routes by host + path. `responses` maps a key to a handler; requests are recorded."""

    responses: dict[str, Handler] = field(default_factory=dict)
    requests: list[str] = field(default_factory=list)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        key = f"{request.method} {request.url.host}{request.url.path}"
        self.requests.append(f"{key}?{request.url.query.decode()}")
        handler = self.responses.get(key)
        return handler(request) if handler else httpx.Response(404)


def ecb_publisher(last_modified: str = "Tue, 20 Oct 2026 13:57:02 GMT") -> FakePublisher:
    def data(request: httpx.Request) -> httpx.Response:
        detail = request.url.params.get("detail") != "dataonly"
        return httpx.Response(
            200,
            text=csv_text(ecb_rows(detail)),
            headers={"content-type": "text/csv", "last-modified": last_modified},
        )

    return FakePublisher({"GET ecb.test/service/data/EXR/D..EUR.SP00.A": data})


def imf_publisher(versions: tuple[tuple[str, str], ...] | None = None) -> FakePublisher:
    flows = versions or (("WEO", "9.0.0"), ("WEO_2026_APR_VINTAGE", "1.0.0"), ("ER", "4.0.1"))

    def structure(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, text=dataflows(*flows), headers={"content-type": "application/xml"}
        )

    def data(request: httpx.Request) -> httpx.Response:
        assert request.headers["accept"].startswith("application/vnd.sdmx.data+csv")
        return httpx.Response(
            200,
            text=csv_text(imf_rows()),
            headers={"content-type": "application/vnd.sdmx.data+csv;version=2.0.0"},
        )

    return FakePublisher(
        {
            "GET imf.test/sdmx/2.1/dataflow/IMF.RES/all/latest": structure,
            "GET imf.test/sdmx/2.1/data/IMF.RES,WEO/.NGDPD+LUR.A": data,
        }
    )


XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def pinksheet_file() -> bytes:
    return xlsx(
        {
            "Monthly Prices": [
                ["World Bank Commodity Price Data (The Pink Sheet)"],
                ["Updated on October 02, 2026"],
                [None, "Crude oil, Brent", "Coal, South African **", "Gold"],
                [None, "($/bbl)", "($/mt)", "($/troy oz)"],
                ["2026M08", 80.5, "…", 2400.0],
                ["2026M09", 81.0, 95.25, None],
                ["Note: footnote line"],
            ]
        }
    )


def file_response(content: bytes, content_type: str, **headers: str) -> Handler:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, content=content, headers={"content-type": content_type} | headers
        )

    return handler


def wpp_file() -> bytes:
    rows = []
    for loc, iso3, kind, name in (
        ("380", "ITA", "Country/Area", "Italy"),
        ("900", "", "World", "World"),
        ("5507", "", "", "ADB region"),
    ):
        for year in (2023, 2024):
            rows.append(
                {
                    "SortOrder": "",
                    "LocID": loc,
                    "ISO3_code": iso3,
                    "LocTypeName": kind,
                    "Location": name,
                    "Time": str(year),
                    "TPopulation1July": "59000.5" if year == 2023 else "",
                    "TFR": "1.2",
                }
            )
    return gzip.compress(("﻿" + csv_text(rows)).encode(), mtime=0)


def dataverse_json(filename: str, file_id: int, minor: int = 0) -> Handler:
    body = {
        "data": {
            "latestVersion": {
                "versionNumber": 2,
                "versionMinorNumber": minor,
                "files": [
                    {"dataFile": {"id": 1, "filename": "other.pdf"}},
                    {
                        "dataFile": {
                            "id": file_id,
                            "filename": filename,
                            "checksum": {"type": "MD5", "value": f"md5-{minor}"},
                        }
                    },
                ],
            }
        }
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, content=json.dumps(body).encode(), headers={"content-type": "application/json"}
        )

    return handler


def pwt_file() -> bytes:
    return xlsx(
        {
            "Info": [["Penn World Table"]],
            "Legend": [
                ["Variable name", "Variable definition"],
                ["Identifier variables", None],
                ["countrycode", "3-letter ISO country code"],
                ["rgdpe", "Expenditure-side real GDP at chained PPPs (in mil. 2021US$)"],
                ["hc", "Human capital index"],
                ["i_xr", "0/1: the exchange rate is market-based (0) or estimated (1)"],
            ],
            "Data": [
                ["countrycode", "country", "currency_unit", "year", "rgdpe", "hc", "i_xr"],
                ["ITA", "Italy", "Euro", 2019, 2854516.0, 3.1, 0],
                ["ITA", "Italy", "Euro", 2020, None, 3.2, 0],
            ],
        }
    )


def maddison_file() -> bytes:
    return xlsx(
        {
            "Full data": [
                ["countrycode", "country", "region", "year", "gdppc", "pop"],
                ["SUN", "Former USSR", "Eastern Europe", 1950, 4529, 179571],
                ["ALB", "Albania", "Eastern Europe", 1, None, 200],
            ]
        }
    )
