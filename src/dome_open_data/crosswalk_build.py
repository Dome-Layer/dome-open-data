"""Rebuild `crosswalk/countries.csv` and `crosswalk/aggregates.csv` from their sources.

Inputs: the UN WPP location list (ISO alpha-3, alpha-2, M49 for every country and area), the
World Bank country API (its codes and aggregates), the IMF codes that WEO and ER actually
use, and two hand-kept files: `manual.csv` (entities the UN list lacks: Kosovo, the Channel
Islands, historical states with their successors) and `overrides.csv` (publisher codes that
differ from ISO). A code that matches nothing fails the build, so a new one cannot slip in.
"""

import csv
import gzip
import io
import logging
import re
from pathlib import Path

import httpx

from dome_open_data.crosswalk import AGGREGATE_COLUMNS, COUNTRY_COLUMNS, ROOT, read_csv
from dome_open_data.http import CSV_ACCEPT_IMF, RateLimiter, SourceError, get, require_json
from dome_open_data.registry import Source

log = logging.getLogger(__name__)

WB_COUNTRIES = "https://api.worldbank.org/v2/country"
IMF = "https://api.imf.org/external/sdmx/2.1/"
IMF_CODE_QUERIES = (("IMF.RES,WEO", ".NGDPD.A"), ("IMF.STA,ER", ".XDC_USD.PA_RT.A"))
_IMF_CODE = re.compile(
    r'<str:Code\b[^>]*\bid="([^"]+)"[^>]*>\s*(?:<com:Annotations>.*?</com:Annotations>\s*)?'
    r'<com:Name xml:lang="en">([^<]*)</com:Name>',
    re.S,
)
# WPP location types whose rows the un_wpp source keeps as aggregates.
UN_AGGREGATE_TYPES = {"World", "Geographic region", "Subregion"}


async def build(client: httpx.AsyncClient, wpp: Source, root: Path = ROOT) -> tuple[int, int]:
    limiter = RateLimiter(2)

    response = await get(client, str(wpp.options["url"]), limiter)
    text = gzip.decompress(response.content).decode("utf-8-sig")
    locations: dict[str, dict[str, str]] = {}
    for row in csv.DictReader(io.StringIO(text)):
        locations.setdefault(row["LocID"], row)
    by_iso3 = {r["ISO3_code"]: r for r in locations.values() if r["ISO3_code"]}

    response = await get(client, WB_COUNTRIES, limiter, params={"format": "json", "per_page": 400})
    worldbank = require_json(response)[1]

    imf_codes: set[str] = set()
    for flow, key in IMF_CODE_QUERIES:
        response = await get(
            client,
            f"{IMF}data/{flow}/{key}",
            limiter,
            params={"lastNObservations": 1, "detail": "dataonly"},
            headers=CSV_ACCEPT_IMF,
        )
        imf_codes |= {row["COUNTRY"] for row in csv.DictReader(io.StringIO(response.text))}
    response = await get(
        client, f"{IMF}dataflow/IMF.RES/WEO/latest", limiter, params={"references": "descendants"}
    )
    imf_names = dict(_IMF_CODE.findall(response.text))

    manual = {row["iso3"]: row for row in read_csv(root / "manual.csv")}
    overrides = {
        (row["publisher"], row["code"]): row["iso3"] for row in read_csv(root / "overrides.csv")
    }

    rows: dict[str, dict[str, str]] = {}
    for iso3, loc in by_iso3.items():
        rows[iso3] = {
            "iso3": iso3,
            "iso2": loc["ISO2_code"],
            "m49": loc["LocID"],
            "name": loc["Location"],
            "kind": "current",
            "worldbank": "",
            "imf": "",
            "successors": "",
            "note": "",
        }
    for iso3, entry in manual.items():
        rows[iso3] = {column: entry.get(column, "") for column in COUNTRY_COLUMNS}

    aggregates: list[dict[str, str]] = []
    for country in worldbank:
        code = country["id"]
        if country["region"]["id"] == "NA":
            aggregates.append({"publisher": "worldbank", "code": code, "name": country["name"]})
        elif code in rows:
            rows[code]["worldbank"] = code
        else:
            raise SourceError(f"World Bank code {code} ({country['name']}) is in no crosswalk row")

    for code in sorted(imf_codes):
        iso3 = overrides.get(("imf", code), code)
        if iso3 in rows:
            rows[iso3]["imf"] = code
        elif code.startswith("G") or code in {"CWX"}:
            aggregates.append({"publisher": "imf", "code": code, "name": imf_names.get(code, code)})
        else:
            raise SourceError(
                f"IMF code {code} ({imf_names.get(code, '?')}) is in no crosswalk row"
            )

    for loc in locations.values():
        if loc["LocTypeName"] in UN_AGGREGATE_TYPES:
            aggregates.append(
                {"publisher": "un", "code": f"UN{loc['LocID']}", "name": loc["Location"]}
            )

    for row in rows.values():
        for successor in filter(None, row["successors"].split(";")):
            if successor not in rows:
                raise SourceError(f"{row['iso3']}: successor {successor} is in no crosswalk row")

    _write(root / "countries.csv", COUNTRY_COLUMNS, sorted(rows.values(), key=lambda r: r["iso3"]))
    _write(
        root / "aggregates.csv",
        AGGREGATE_COLUMNS,
        sorted(aggregates, key=lambda r: (r["publisher"], r["code"])),
    )
    log.info("crosswalk built", extra={"countries": len(rows), "aggregates": len(aggregates)})
    return len(rows), len(aggregates)


def _write(path: Path, columns: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows({k: " ".join(v.split()) for k, v in row.items()} for row in rows)
