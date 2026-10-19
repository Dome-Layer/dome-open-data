import json
from dataclasses import dataclass, field
from typing import Any

import httpx

from dome_open_data.registry import Source, parse_registry

ENDPOINT = "https://api.worldbank.test/v2/"

INDICATORS = {
    "NY.GDP.MKTP.CD": "GDP (current US$)",
    "SP.POP.TOTL": "Population, total",
}


def make_source(**overrides: Any) -> Source:
    entry: dict[str, Any] = {
        "id": "worldbank_wdi",
        "name": "World Bank World Development Indicators",
        "publisher": "worldbank",
        "dataset": "wdi",
        "domain": "economy",
        "connector": "rest",
        "dialect": "worldbank",
        "endpoint": ENDPOINT,
        "cadence": "monthly",
        "change_signal": "catalogue_date",
        "rate_limit": {"per_second": 1000},
        "keep_raw": True,
        "expected_staleness_days": 120,
        "licence": {
            "name": "CC BY 4.0",
            "commercial_ok": "yes",
            "share_alike": False,
            "attribution": "Source: World Bank, World Development Indicators.",
        },
        "options": {"source_id": 2, "start_year": 2020, "indicators": list(INDICATORS)},
    }
    entry.update(overrides)
    return parse_registry([entry])["worldbank_wdi"]


def record(code: str, iso3: str, year: str, value: float | None) -> dict[str, Any]:
    return {
        "indicator": {"id": code, "value": INDICATORS.get(code, code)},
        "country": {"id": iso3[:2], "value": iso3},
        "countryiso3code": iso3,
        "date": year,
        "value": value,
        "unit": "",
        "obs_status": "",
        "decimal": 0,
    }


def default_rows(code: str) -> list[dict[str, Any]]:
    return [
        record(code, "ITA", "2021", 2.1),
        record(code, "ITA", "2020", 1.9),
        record(code, "FRA", "2021", 3.0),
        record(code, "FRA", "2020", None),  # null value: dropped
        record(code, "", "2021", 9.9),  # no geo: dropped
    ]


@dataclass
class FakeWorldBank:
    lastupdated: str = "2026-10-08"
    page_size: int = 3
    rows: dict[str, list[dict[str, Any]]] = field(
        default_factory=lambda: {code: default_rows(code) for code in INDICATORS}
    )
    fail_first: set[str] = field(default_factory=set)
    html_for: set[str] = field(default_factory=set)
    truncate: set[str] = field(default_factory=set)
    requests: list[str] = field(default_factory=list)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/v2/")
        self.requests.append(path)
        if path in self.fail_first:
            self.fail_first.discard(path)
            return httpx.Response(503)
        if path in self.html_for:
            return httpx.Response(
                200, text="<html>challenge</html>", headers={"content-type": "text/html"}
            )
        if path == "sources/2":
            return self._json(
                [
                    {"page": "1", "pages": "1", "total": "1"},
                    [{"id": "2", "lastupdated": self.lastupdated}],
                ]
            )
        if path == "source/2/indicator":
            meta = [
                {"id": c, "name": n, "unit": "", "sourceNote": f"{n} note."}
                for c, n in INDICATORS.items()
            ]
            return self._json([{"page": 1, "pages": 1, "total": len(meta)}, meta])
        if path.startswith("country/all/indicator/"):
            code = path.rsplit("/", 1)[1]
            rows = self.rows[code]
            page = int(request.url.params["page"])
            pages = max(1, -(-len(rows) // self.page_size))
            chunk = rows[(page - 1) * self.page_size : page * self.page_size]
            if code in self.truncate:
                chunk = chunk[:-1]
            header = {
                "page": page,
                "pages": pages,
                "per_page": self.page_size,
                "total": len(rows),
                "lastupdated": self.lastupdated,
            }
            return self._json([header, chunk])
        return httpx.Response(404)

    @staticmethod
    def _json(body: Any) -> httpx.Response:
        return httpx.Response(
            200,
            content=json.dumps(body).encode(),
            headers={"content-type": "application/json;charset=utf-8"},
        )
