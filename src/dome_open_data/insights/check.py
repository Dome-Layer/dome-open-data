"""Stage 3, check: every number in an insight is matched to a figure and recomputed from the data.

Two things are proved, and either failing drops the insight:

1. **The figures are true.** Each figure of the candidate is recomputed from its recipe by the
   code below, reading the store again through a `Reader` of its own (never the scan's), and every
   object the candidate read must still hash the same. The arithmetic here is written separately
   from the detectors on purpose: two pieces of code have to agree.
2. **The text says only those figures.** Every number, date, month, quarter and year in the
   headline and body is found, and each must match a figure of the candidate at the precision the
   text shows. A number the text states that no figure supports is a mismatch, however plausible.

One mismatch and the insight is dropped and logged, never republished by hand. What passes is
published with "figures checked: n of n".
"""

import gzip
import hashlib
import math
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from dome_open_data.insights.data import DataError, Point, Reader
from dome_open_data.insights.figures import MONTHS, Figure
from dome_open_data.insights.scan import Candidate

MAX_HEADLINE = 120
MAX_BODY = 600
DASHES = ("\u2013", "\u2014")
# Wording that claims a cause. The data can show that one thing followed another, never why.
CAUSAL = re.compile(
    r"\b(because|caused|causing|cause[sd]? by|due to|drove|driven by|led to|leading to|"
    r"triggered|in response to|as a result|resulted in|thanks to|owing to|prompted)\b",
    re.IGNORECASE,
)
NUMBER_WORDS = {
    w: n
    for n, w in enumerate(
        "zero one two three four five six seven eight nine ten eleven twelve".split()
    )
    if n >= 2  # "one" is too common as a pronoun to be read as a figure
}

_MONTH = "(" + "|".join(MONTHS) + ")"
_DAY_PAIR = re.compile(rf"\b(\d{{1,2}}) (?:and|to) (\d{{1,2}}) {_MONTH}(?: (\d{{4}}))?\b")
_DAY = re.compile(rf"\b(\d{{1,2}}) {_MONTH}(?: (\d{{4}}))?\b")
_MONTH_YEAR = re.compile(rf"\b{_MONTH} (\d{{4}})\b")
_QUARTER = re.compile(r"\bQ([1-4])(?: (\d{4}))?\b")
_BARE_MONTH = re.compile(rf"\b{_MONTH}\b")
_YEAR = re.compile(r"\b((?:19|20)\d{2})\b")
_NUMBER = re.compile(
    r"(?<![\w.,])([+\-\u2212])?[$€£]?(\d{1,3}(?:,\d{3})+|\d+)(?:\.(\d+))?(\s?%|\s?per ?cent)?"
    r"(?![\w])"
)
_WORD = re.compile(r"\b(" + "|".join(NUMBER_WORDS) + r")\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class Draft:
    """What the writer returns for one candidate."""

    candidate_id: str
    tag: str
    headline: str
    body: str
    figures_used: list[str]


@dataclass(frozen=True, slots=True)
class Mention:
    text: str
    kind: str  # number, day, month, quarter, year
    number: float | None = None
    decimals: int = 0
    signed: bool = False
    percent: bool = False
    day: int | None = None
    month: int | None = None
    year: int | None = None
    quarter: int | None = None
    word: bool = False  # written as a word ("five weeks")


@dataclass(slots=True)
class Verdict:
    candidate_id: str
    checked: int = 0
    total: int = 0
    problems: list[str] = field(default_factory=list)
    matches: list[dict[str, Any]] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.problems and self.total > 0 and self.checked == self.total

    @property
    def label(self) -> str:
        return f"figures checked: {self.checked} of {self.total}"


# --- Finding what the text states ------------------------------------------------------------


def _blank(text: str, match: re.Match[str]) -> str:
    return text[: match.start()] + " " * (match.end() - match.start()) + text[match.end() :]


def mentions(text: str) -> list[Mention]:
    """Every number and date the text states, dates first so their digits are not read twice."""
    found: list[Mention] = []
    rest = text

    def take(pattern: re.Pattern[str], build: Any) -> None:
        nonlocal rest
        for match in list(pattern.finditer(rest)):
            found.append(build(match))
            rest = _blank(rest, match)

    def month_no(name: str) -> int:
        return MONTHS.index(name) + 1

    for match in list(_DAY_PAIR.finditer(rest)):
        year = int(match[4]) if match[4] else None
        for group in (1, 2):
            found.append(
                Mention(
                    f"{match[group]} {match[3]}",
                    "day",
                    day=int(match[group]),
                    month=month_no(match[3]),
                    year=year,
                )
            )
        rest = _blank(rest, match)
    take(
        _DAY,
        lambda m: Mention(
            m[0], "day", day=int(m[1]), month=month_no(m[2]), year=int(m[3]) if m[3] else None
        ),
    )
    take(_MONTH_YEAR, lambda m: Mention(m[0], "month", month=month_no(m[1]), year=int(m[2])))
    take(
        _QUARTER,
        lambda m: Mention(m[0], "quarter", quarter=int(m[1]), year=int(m[2]) if m[2] else None),
    )
    take(_BARE_MONTH, lambda m: Mention(m[0], "month", month=month_no(m[1])))
    take(_YEAR, lambda m: Mention(m[0], "year", year=int(m[1])))

    def number(m: re.Match[str]) -> Mention:
        whole = m[2].replace(",", "")
        decimals = len(m[3]) if m[3] else 0
        value = float(f"{whole}.{m[3]}" if m[3] else whole)
        if m[1] in ("-", "\u2212"):
            value = -value
        return Mention(
            m[0].strip(),
            "number",
            number=value,
            decimals=decimals,
            signed=bool(m[1]),
            percent=bool(m[4]),
        )

    take(_NUMBER, number)
    take(
        _WORD,
        lambda m: Mention(m[0], "number", number=float(NUMBER_WORDS[m[1].lower()]), word=True),
    )
    return found


def _rounded(value: float, decimals: int) -> set[float]:
    """The ways a figure may be shown at a given precision: Python's and half-up rounding."""
    exact = Decimal(repr(abs(value)))
    quantum = Decimal(1).scaleb(-decimals)
    return {
        float(f"{abs(value):.{decimals}f}"),
        float(exact.quantize(quantum, rounding=ROUND_HALF_UP)),
    }


def _supports(figure: Figure, mention: Mention) -> bool:
    if mention.kind == "number":
        if figure.is_date or mention.number is None:
            return False
        value = float(figure.value)
        if mention.percent and not figure.percent:
            return False
        if (
            mention.signed
            and value != 0
            and math.copysign(1, value) != math.copysign(1, mention.number)
        ):
            return False
        if mention.decimals == 0 and not value.is_integer():
            # Rounding to a whole number is allowed only where it keeps the meaning: a
            # percentage, or a figure in the hundreds or more. "six" is not 5.6339.
            if mention.word or not (mention.percent or abs(value) >= 100):
                return False
        return abs(mention.number) in _rounded(value, mention.decimals)
    if not figure.is_date:
        return False
    when = date.fromisoformat(str(figure.value))
    if mention.year is not None and when.year != mention.year:
        return False
    if mention.kind == "day":
        return figure.granularity == "day" and (when.day, when.month) == (
            mention.day,
            mention.month,
        )
    if mention.kind == "month":
        return figure.granularity in ("day", "month") and when.month == mention.month
    if mention.kind == "quarter":
        return figure.granularity == "quarter" and (when.month - 1) // 3 + 1 == mention.quarter
    return mention.kind == "year"


def match(candidate: Candidate, draft: Draft, verdict: Verdict) -> None:
    """Map every mention in the text to a figure of the candidate, or record a problem."""
    if draft.candidate_id != candidate.id:
        verdict.problems.append(f"draft is for {draft.candidate_id}, not {candidate.id}")
    if draft.tag != candidate.tag:
        verdict.problems.append(f"tag {draft.tag!r}, expected {candidate.tag!r}")
    unknown = sorted(set(draft.figures_used) - {f.id for f in candidate.figures})
    if unknown:
        verdict.problems.append(f"figures_used names unknown figures: {', '.join(unknown)}")
    if not draft.headline.strip() or len(draft.headline) > MAX_HEADLINE:
        verdict.problems.append(f"headline empty or over {MAX_HEADLINE} characters")
    if not draft.body.strip() or len(draft.body) > MAX_BODY:
        verdict.problems.append(f"body empty or over {MAX_BODY} characters")
    for part in (draft.headline, draft.body):
        if any(dash in part for dash in DASHES):
            verdict.problems.append("uses an em or en dash (house style)")
        if found := CAUSAL.search(part):
            verdict.problems.append(f"claims a cause: {found[0]!r}")
    for part in (draft.headline, draft.body):
        for mention in mentions(part):
            verdict.total += 1
            supporting = [f for f in candidate.figures if _supports(f, mention)]
            if supporting:
                verdict.checked += 1
                verdict.matches.append({"text": mention.text, "figure": supporting[0].id})
            else:
                verdict.problems.append(f"{mention.text!r} matches no figure of the candidate")


# --- Recomputing every figure from the store -------------------------------------------------


class Recompute:
    """Evaluates figure recipes against the store, with its own reader and its own arithmetic."""

    def __init__(self, reader: Reader) -> None:
        self.reader = reader

    async def series(self, ref: dict[str, str]) -> list[Point]:
        source = self.reader.source(ref["source_id"])
        vintage = date.fromisoformat(ref["vintage"])
        if source.connector == "alfred":
            code = ref["series_id"].split(".")[2]
            held = await self.reader.versions(source.id, code)
            if vintage not in held.versions:
                raise DataError(f"{code} has no publication dated {vintage}")
            return held.versions[vintage]
        snap = await self.reader.snapshot(source.id, vintage)
        if ref["series_id"] not in snap.series:
            raise DataError(f"{ref['series_id']} is not in {source.id} {vintage}")
        return snap.series[ref["series_id"]].points

    async def point(self, ref: dict[str, str]) -> tuple[list[Point], int]:
        points = await self.series(ref)
        period = date.fromisoformat(ref["period"])
        for index, (when, _) in enumerate(points):
            if when == period:
                return points, index
        raise DataError(f"no observation of {ref['series_id']} at {period}")

    async def value(self, ref: dict[str, str]) -> float:
        points, index = await self.point(ref)
        return points[index][1]

    async def measure(self, how: str, ref: dict[str, str]) -> float:
        points, index = await self.point(ref)
        value = points[index][1]
        if how == "level":
            return value
        if index == 0:
            raise DataError("no earlier period to measure from")
        before = points[index - 1][1]
        if how == "change":
            return value - before
        if how == "monthly_growth":
            return (value - before) / before * 100.0
        if how == "annualised_growth":
            return (math.pow(value / before, 4) - 1.0) * 100.0
        raise DataError(f"unknown measure {how}")

    async def window(self, calc: dict[str, Any]) -> dict[str, float]:
        """Percent move of each series still quoted over the window, as the recipe defines it."""
        snap = await self.reader.snapshot(calc["source_id"], date.fromisoformat(calc["vintage"]))
        since = date.fromisoformat(calc["since"])
        until = date.fromisoformat(calc["until"])
        cutoff = until - timedelta(days=int(calc["quoted_within_days"]))
        moves = {}
        for sid, series in snap.series.items():
            if series.points[-1][0] < cutoff:
                continue
            inside = [v for when, v in series.points if since <= when <= until]
            if len(inside) >= 2:
                moves[sid] = (inside[-1] - inside[0]) / inside[0] * 100.0
        return moves

    async def evaluate(self, calc: dict[str, Any]) -> float | str:
        op = calc["op"]
        if op == "obs":
            return await self.value(calc["ref"])
        if op in ("date", "published"):
            await self.point(calc["ref"])
            return str(calc["ref"]["period" if op == "date" else "vintage"])
        if op == "pct":
            a, b = await self.value(calc["from"]), await self.value(calc["to"])
            return (b - a) / a * 100.0
        if op in ("weeks", "months", "days"):
            await self.point(calc["from"])
            await self.point(calc["to"])
            start = date.fromisoformat(calc["from"]["period"])
            end = date.fromisoformat(calc["to"]["period"])
            if op == "weeks":
                return float(round((end - start).days / 7))
            if op == "days":
                return float((end - start).days)
            return float((end.year - start.year) * 12 + (end.month - start.month))
        if op == "quoted":
            return float(len(await self.window(calc)))
        if op == "rank":
            moves = await self.window(calc)
            order = sorted(moves, key=lambda sid: (-abs(moves[sid]), sid))
            return float(order.index(calc["series_id"]) + 1)
        if op == "since":
            points, index = await self.point(calc["ref"])
            value = points[index][1]
            low = calc["direction"] == "low"
            for when, earlier in reversed(points[:index]):
                if (earlier <= value) if low else (earlier >= value):
                    return when.isoformat()
            raise DataError("no earlier observation as extreme")
        if op == "held_since":
            points, index = await self.point(calc["ref"])
            if index == 0 or points[index - 1][1] == points[index][1]:
                raise DataError("the follower did not change at the stated date")
            return points[index - 1][0].isoformat()
        if op == "step_ratio":
            points, index = await self.point(calc["ref"])
            start = points[index][0] - timedelta(days=int(calc["history_days"]))
            steps = sorted(
                abs(points[m][1] / points[m - 1][1] - 1.0) * 100.0
                for m in range(1, index)
                if points[m][0] >= start and points[m - 1][1] != 0
            )
            usual = steps[max(0, math.ceil(0.99 * len(steps)) - 1)]
            return abs(points[index][1] / points[index - 1][1] - 1.0) * 100.0 / usual
        if op == "measure":
            return await self.measure(calc["measure"], calc["ref"])
        if op == "revision":
            return await self.measure(calc["measure"], calc["to"]) - await self.measure(
                calc["measure"], calc["from"]
            )
        raise DataError(f"unknown recipe {op}")


def _same(expected: float | str, got: float | str) -> bool:
    if isinstance(expected, str) or isinstance(got, str):
        return expected == got
    return math.isclose(float(expected), float(got), rel_tol=1e-9, abs_tol=1e-9)


async def _hash(reader: Reader, key: str) -> str | None:
    data = await reader.storage.get(key)
    if data is None:
        return None
    if key.endswith(".gz"):
        data = gzip.decompress(data)  # ALFRED manifests hash the bytes as received
    return hashlib.sha256(data).hexdigest()


async def recompute(candidate: Candidate, reader: Reader, verdict: Verdict) -> None:
    for read in candidate.reads:
        found = await _hash(reader, read.key)
        if found != read.sha256:
            verdict.problems.append(f"{read.key} no longer hashes as it did when scanned")
    engine = Recompute(reader)
    for figure in candidate.figures:
        try:
            got = await engine.evaluate(figure.calc)
        except (DataError, ValueError, ZeroDivisionError, IndexError) as exc:
            verdict.problems.append(f"{figure.id}: could not recompute ({exc})")
            continue
        if not _same(figure.value, got):
            verdict.problems.append(f"{figure.id}: scanned {figure.value}, recomputed {got}")


async def check(candidate: Candidate, draft: Draft, reader: Reader) -> Verdict:
    """The whole check for one insight. `reader` must be the checker's own, not the scan's."""
    verdict = Verdict(candidate.id)
    await recompute(candidate, reader, verdict)
    match(candidate, draft, verdict)
    return verdict
