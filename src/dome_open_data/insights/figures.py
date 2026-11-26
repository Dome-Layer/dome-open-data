"""Figures: the only numbers and dates an insight may state, each with the recipe that proves it.

A figure carries its value, how it is shown, and a `calc`: a small, explicit recipe over named
observations (`ref`s) that the checker evaluates again from the store, with its own code. The
detectors compute the value their own way while scanning; the two must agree or the candidate is
dropped.

A `ref` names one observation exactly:
`{"source_id", "series_id", "period", "vintage"}`, where `vintage` is the collection vintage for
a snapshot source and the publication date for ALFRED.
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Any

MONTHS = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)

Ref = dict[str, str]


def ref(source_id: str, series_id: str, period: date, vintage: date) -> Ref:
    return {
        "source_id": source_id,
        "series_id": series_id,
        "period": period.isoformat(),
        "vintage": vintage.isoformat(),
    }


@dataclass(frozen=True, slots=True)
class Figure:
    """One stated fact. `value` is a number, or an ISO date for a date figure."""

    id: str
    meaning: str
    value: float | str
    display: str
    calc: dict[str, Any]
    percent: bool = False
    granularity: str | None = None  # "day", "month" or "quarter" for a date figure

    @property
    def is_date(self) -> bool:
        return isinstance(self.value, str)

    def as_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "id": self.id,
            "meaning": self.meaning,
            "display": self.display,
            "calc": self.calc,
        }
        if self.is_date:
            out["date"] = self.value
            out["granularity"] = self.granularity
        else:
            out["number"] = self.value
            out["percent"] = self.percent
        return out


@dataclass(slots=True)
class Figures:
    """Collects a candidate's figures under unique ids, in the order they are added."""

    items: list[Figure] = field(default_factory=list)

    def add(self, figure: Figure) -> Figure:
        if any(f.id == figure.id for f in self.items):
            raise ValueError(f"duplicate figure id {figure.id}")
        self.items.append(figure)
        return figure


# --- Display -------------------------------------------------------------------------------


def number(value: float, decimals: int) -> str:
    return f"{value:,.{decimals}f}"


def signed_pct(value: float, decimals: int = 1) -> str:
    return f"{value:+.{decimals}f}%"


def day(when: date) -> str:
    return f"{when.day} {MONTHS[when.month - 1]} {when.year}"


def month(when: date) -> str:
    return f"{MONTHS[when.month - 1]} {when.year}"


def quarter(when: date) -> str:
    return f"Q{(when.month - 1) // 3 + 1} {when.year}"


def show_date(when: date, granularity: str) -> str:
    return {"day": day, "month": month, "quarter": quarter}[granularity](when)


def granularity_of(freq: str) -> str:
    return {"D": "day", "M": "month", "Q": "quarter"}.get(freq, "day")


def months_between(earlier: date, later: date) -> int:
    return (later.year - earlier.year) * 12 + later.month - earlier.month


def pct(start: float, end: float) -> float:
    return 100.0 * (end / start - 1.0)
