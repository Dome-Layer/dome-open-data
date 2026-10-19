import re
from dataclasses import dataclass
from datetime import date
from typing import Any

import pyarrow as pa


def schema_of(*fields: "pa.Field[Any]") -> pa.Schema:
    return pa.schema(fields)


TIMESERIES_SCHEMA = schema_of(
    pa.field("series_id", pa.string(), nullable=False),
    pa.field("geo", pa.string(), nullable=False),
    pa.field("period", pa.date32(), nullable=False),
    pa.field("freq", pa.string(), nullable=False),
    pa.field("value", pa.float64(), nullable=False),
    pa.field("unit", pa.string(), nullable=False),
    pa.field("obs_status", pa.string(), nullable=True),
    pa.field("vintage", pa.date32(), nullable=False),
)

SERIES_SCHEMA = schema_of(
    pa.field("series_id", pa.string(), nullable=False),
    pa.field("source_id", pa.string(), nullable=False),
    pa.field("code", pa.string(), nullable=False),
    pa.field("geo", pa.string(), nullable=False),
    pa.field("freq", pa.string(), nullable=False),
    pa.field("title", pa.string(), nullable=False),
    pa.field("definition", pa.string(), nullable=True),
    pa.field("unit", pa.string(), nullable=False),
    pa.field("seasonal_adjustment", pa.string(), nullable=True),
    pa.field("source_url", pa.string(), nullable=False),
    pa.field("licence", pa.string(), nullable=False),
    pa.field("attribution", pa.string(), nullable=False),
    pa.field("vintage", pa.date32(), nullable=False),
)

_ANNUAL = re.compile(r"^(\d{4})$")
_QUARTERLY = re.compile(r"^(\d{4})Q([1-4])$")
_MONTHLY = re.compile(r"^(\d{4})M(0[1-9]|1[0-2])$")


@dataclass(frozen=True, slots=True)
class Observation:
    series_id: str
    geo: str
    period: date
    freq: str
    value: float
    unit: str
    obs_status: str | None


def parse_period(text: str) -> tuple[date, str]:
    if match := _ANNUAL.match(text):
        return date(int(match[1]), 1, 1), "A"
    if match := _QUARTERLY.match(text):
        return date(int(match[1]), 3 * int(match[2]) - 2, 1), "Q"
    if match := _MONTHLY.match(text):
        return date(int(match[1]), int(match[2]), 1), "M"
    raise ValueError(f"unrecognised period {text!r}")


def observations_table(observations: list[Observation], vintage: date) -> pa.Table:
    return pa.table(
        {
            "series_id": [o.series_id for o in observations],
            "geo": [o.geo for o in observations],
            "period": [o.period for o in observations],
            "freq": [o.freq for o in observations],
            "value": [o.value for o in observations],
            "unit": [o.unit for o in observations],
            "obs_status": [o.obs_status for o in observations],
            "vintage": [vintage] * len(observations),
        },
        schema=TIMESERIES_SCHEMA,
    )
