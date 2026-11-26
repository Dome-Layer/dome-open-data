"""Stage 1, scan: deterministic detectors over the clean zone.

Each detector returns candidates: what it found, every figure it measured (with the recipe that
proves it, see `figures`), the exact objects it read, and a score for ranking. Nothing here is
written by a model, and nothing a model writes is ever fed back in.

The five detectors, all over sources whose licence allows values:

- **move**: the largest move in a family over a window (the currencies, the commodities).
- **record**: an observation more extreme than any for at least `min_days`.
- **jump**: one step far outside the series' own range of steps.
- **revision**: the largest revision between the two newest ALFRED publications of a series.
- **lead_lag**: a large move in a leader, then a follower's first change after a long hold,
  always worded as a pattern in the data and never as a cause.
"""

import hashlib
import json
import logging
import math
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
from statistics import median
from typing import Any

from dome_open_data import licence
from dome_open_data.insights import figures as fig
from dome_open_data.insights.config import Family, InsightsConfig, LeadLag, RevisionSeries
from dome_open_data.insights.data import DataError, Point, Read, Reader, Series, Snapshot, Versions
from dome_open_data.insights.figures import Figure, Figures, ref

log = logging.getLogger(__name__)

PATTERN_CAVEAT = "A pattern in the data, not proof of cause."
TAGS = {
    "move": "Outlier",
    "jump": "Outlier",
    "record": "Milestone",
    "revision": "Revision",
    "lead_lag": "Pattern",
}
# A series counts as still quoted if its newest observation is this close to the newest of its
# source: a currency the ECB stopped fixing in 2022 is held, but it is not "one of the movers".
QUOTED_WITHIN_DAYS = 10


@dataclass(slots=True)
class Candidate:
    id: str
    detector: str
    subject: str
    summary: str
    figures: list[Figure]
    reads: list[Read]
    score: float
    series: set[str] = field(default_factory=set)
    # The newest period the finding is about, for ranking by freshness. None: never discounted.
    latest: date | None = None
    # Candidates of one group compete for one place: the euro falling against twenty
    # currencies is one story, not twenty.
    group: str = ""
    charts: list[dict[str, Any]] = field(default_factory=list)
    source_ids: list[str] = field(default_factory=list)

    @property
    def tag(self) -> str:
        return TAGS[self.detector]

    @property
    def caveat(self) -> str | None:
        return PATTERN_CAVEAT if self.detector == "lead_lag" else None

    def figure(self, figure_id: str) -> Figure | None:
        return next((f for f in self.figures if f.id == figure_id), None)

    @property
    def fingerprint(self) -> str:
        """Same id and the same figures means the same finding, so its checked text may be kept."""
        body = json.dumps(
            [self.id, [[f.id, f.value] for f in self.figures]], sort_keys=True, default=str
        )
        return hashlib.sha256(body.encode()).hexdigest()[:16]

    def for_model(self) -> dict[str, Any]:
        """The only thing the writer sees: no store access, no raw series, nothing to add up."""
        return {
            "id": self.id,
            "tag": self.tag,
            "subject": self.subject,
            "finding": self.summary,
            "figures": [
                {"id": f.id, "meaning": f.meaning, "display": f.display} for f in self.figures
            ],
            **({"caveat": self.caveat} if self.caveat else {}),
        }


# --- Helpers -------------------------------------------------------------------------------


def decimals_of(values: list[float], cap: int = 4) -> int:
    """How many decimals the publisher used, read off the values themselves."""
    best = 0
    for value in values[-50:]:
        text = f"{value:.{cap}f}".rstrip("0")
        best = max(best, len(text.split(".")[1]) if "." in text else 0)
    return best


# Titles that read badly as names in a sentence.
NAMES = {
    "ecb.policy_rates.DFR.LEV.EMU.D": "ECB deposit facility rate",
    "ecb.policy_rates.MRR_FR.LEV.EMU.D": "ECB main refinancing rate",
    "ecb.policy_rates.MLFR.LEV.EMU.D": "ECB marginal lending rate",
}


def name(series: Series) -> str:
    """How a reader knows the series: EUR/BRL for a currency, the title otherwise."""
    parts = series.series_id.split(".")
    if parts[:2] == ["ecb", "fx"]:
        return f"EUR/{parts[2]}"
    return NAMES.get(series.series_id, series.title)


def on(when: date, granularity: str) -> str:
    """'on 23 November 2026', 'in September 2026', 'in Q1 2008'."""
    return ("on " if granularity == "day" else "in ") + fig.show_date(when, granularity)


def long_name(series: Series) -> str:
    parts = series.series_id.split(".")
    if parts[:2] == ["ecb", "fx"]:
        currency = series.title.split("/")[0].strip()
        return f"EUR/{parts[2]} ({currency} per euro)"
    if series.series_id in NAMES:
        return NAMES[series.series_id]
    return f"{series.title} ({series.unit})"


def quoted(snapshot: Snapshot) -> tuple[date, list[Series]]:
    latest = max(s.last[0] for s in snapshot.series.values())
    cutoff = latest - timedelta(days=QUOTED_WITHIN_DAYS)
    return latest, [s for s in snapshot.series.values() if s.last[0] >= cutoff]


def chart(snapshot: Snapshot, series: Series, points: list[Point], label: str) -> dict[str, Any]:
    return licence.value_block(
        snapshot.source,
        [[p.isoformat(), v] for p, v in points],
        series_id=series.series_id,
        label=label,
        unit=series.unit,
    )


class _Builder:
    """Figures for observations of one series in one snapshot, with their recipes."""

    def __init__(self, snapshot: Snapshot, series: Series, figures: Figures) -> None:
        self.snapshot = snapshot
        self.series = series
        self.figures = figures
        self.decimals = decimals_of([v for _, v in series.points])
        self.granularity = fig.granularity_of(series.freq)

    def ref(self, period: date) -> fig.Ref:
        return ref(self.snapshot.source.id, self.series.series_id, period, self.snapshot.vintage)

    def value(self, fid: str, point: Point, meaning: str, percent: bool = False) -> Figure:
        return self.figures.add(
            Figure(
                fid,
                meaning,
                point[1],
                fig.number(point[1], self.decimals) + ("%" if percent else ""),
                {"op": "obs", "ref": self.ref(point[0])},
                percent=percent,
            )
        )

    def when(self, fid: str, point: Point, meaning: str) -> Figure:
        return self.figures.add(
            Figure(
                fid,
                meaning,
                point[0].isoformat(),
                fig.show_date(point[0], self.granularity),
                {"op": "date", "ref": self.ref(point[0])},
                granularity=self.granularity,
            )
        )

    def change(self, fid: str, start: Point, end: Point, meaning: str) -> Figure:
        value = fig.pct(start[1], end[1])
        return self.figures.add(
            Figure(
                fid,
                meaning,
                value,
                fig.signed_pct(value),
                {"op": "pct", "from": self.ref(start[0]), "to": self.ref(end[0])},
                percent=True,
            )
        )


# --- Detectors -----------------------------------------------------------------------------


async def moves(reader: Reader, family: Family) -> list[Candidate]:
    snapshot = await reader.snapshot(family.source_id)
    latest, live = quoted(snapshot)
    since = latest - timedelta(days=family.window_days)
    found: list[tuple[Series, list[Point], float]] = []
    for series in live:
        window = [p for p in series.points if since <= p[0] <= latest]
        if len(window) >= 2:
            found.append((series, window, fig.pct(window[0][1], window[-1][1])))
    if len(found) < 5:
        return []
    found.sort(key=lambda m: (-abs(m[2]), m[0].series_id))
    top, window, change = found[0]
    typical = median(abs(m[2]) for m in found)
    ratio = abs(change) / typical if typical else math.inf
    if ratio < 2:
        return []

    figures = Figures()
    b = _Builder(snapshot, top, figures)
    b.value("start_value", window[0], f"{name(top)} at the start of the window")
    b.when("start_date", window[0], "start of the window")
    b.value("end_value", window[-1], f"{name(top)} at the end of the window")
    b.when("end_date", window[-1], "end of the window")
    b.change("change_pct", window[0], window[-1], f"change in {name(top)} over the window")
    window_calc = {
        "source_id": snapshot.source.id,
        "vintage": snapshot.vintage.isoformat(),
        "since": since.isoformat(),
        "until": latest.isoformat(),
        "quoted_within_days": QUOTED_WITHIN_DAYS,
    }
    figures.add(
        Figure(
            "compared",
            f"{family.label} quoted over the window and compared",
            float(len(found)),
            str(len(found)),
            {"op": "quoted", **window_calc},
        )
    )
    figures.add(
        Figure(
            "rank",
            f"rank of {name(top)} by size of move (1 = largest)",
            1.0,
            "1",
            {"op": "rank", "series_id": top.series_id, **window_calc},
        )
    )
    if top.freq == "D":
        weeks = round((window[-1][0] - window[0][0]).days / 7)
        figures.add(
            Figure(
                "weeks",
                "length of the window in weeks",
                float(weeks),
                f"{weeks} weeks",
                {"op": "weeks", "from": b.ref(window[0][0]), "to": b.ref(window[-1][0])},
            )
        )
    else:
        months = fig.months_between(window[0][0], window[-1][0])
        figures.add(
            Figure(
                "months",
                "length of the window in months",
                float(months),
                f"{months} months",
                {"op": "months", "from": b.ref(window[0][0]), "to": b.ref(window[-1][0])},
            )
        )
    runner_up, runner_window, _ = found[1]
    b2 = _Builder(snapshot, runner_up, figures)
    b2.change(
        "runner_up_pct",
        runner_window[0],
        runner_window[-1],
        f"change in {name(runner_up)}, the second largest move",
    )

    summary = (
        f"Of {len(found)} {family.label} still quoted, {long_name(top)} moved the most between "
        f"{fig.day(window[0][0])} and {fig.day(window[-1][0])}: from {figures.items[0].display} to "
        f"{figures.items[2].display}, {figures.items[4].display}. The second largest move was "
        f"{name(runner_up)}, {figures.items[-1].display}."
    )
    steps = [(window[i - 1], window[i]) for i in range(1, len(window))]
    step_from, step_to = max(steps, key=lambda s: abs(fig.pct(s[0][1], s[1][1])))
    step = fig.pct(step_from[1], step_to[1])
    if abs(step) >= 0.5 * abs(change) and len(steps) > 1:
        b.value("step_from_value", step_from, f"{name(top)} just before its largest single step")
        b.when("step_from_date", step_from, "observation before the largest single step")
        b.value("step_to_value", step_to, f"{name(top)} just after its largest single step")
        b.when("step_to_date", step_to, "observation after the largest single step")
        b.change(
            "step_pct", step_from, step_to, f"the largest single step in {name(top)}, in one go"
        )
        summary += (
            f" Most of it came in one step, between the observations of "
            f"{fig.show_date(step_from[0], b.granularity)} and "
            f"{fig.show_date(step_to[0], b.granularity)}: {fig.signed_pct(step)}."
        )
    return [
        Candidate(
            id=f"move:{family.id}",
            detector="move",
            subject=f"{long_name(top)}, the largest move of {len(found)} {family.label}",
            summary=summary,
            figures=figures.items,
            reads=[snapshot.read],
            score=min(ratio / 2, 3.0),
            series={top.series_id},
            latest=window[-1][0],
            charts=[chart(snapshot, top, window, name(top))],
            source_ids=[snapshot.source.id],
        )
    ]


def _since(points: list[Point], index: int, low: bool) -> int | None:
    """Index of the newest earlier observation at least as extreme as `points[index]`."""
    value = points[index][1]
    for j in range(index - 1, -1, -1):
        if (points[j][1] <= value) if low else (points[j][1] >= value):
            return j
    return None


async def records(reader: Reader, family: Family, config: InsightsConfig) -> list[Candidate]:
    snapshot = await reader.snapshot(family.source_id)
    _, live = quoted(snapshot)
    out: list[Candidate] = []
    for series in live:
        pts = series.points
        recent = config.records.recent_observations.get(series.freq, 1)
        tail = range(max(1, len(pts) - recent), len(pts))
        best: tuple[int, int, int, bool] | None = None  # gap days, index, since index, low
        for low in (True, False):
            # The most extreme of the newest observations, so the claim is about the record
            # itself and not an earlier, less extreme step on the way to it.
            i = (min if low else max)(tail, key=lambda k: (pts[k][1] if low else -pts[k][1], -k))
            j = _since(pts, i, low)
            if j is None:
                continue  # the extreme of everything held: not claimed, the store starts late
            gap = (pts[i][0] - pts[j][0]).days
            if gap >= config.records.min_days and (best is None or gap > best[0]):
                best = (gap, i, j, low)
        if best is None:
            continue
        gap, i, j, low = best
        word = "lowest" if low else "highest"
        figures = Figures()
        b = _Builder(snapshot, series, figures)
        b.value("value", pts[i], f"{name(series)}, the {word} observation in a long time")
        b.when("date", pts[i], f"date of the {word} observation")
        figures.add(
            Figure(
                "since_date",
                f"the last earlier observation at least as {'low' if low else 'high'}",
                pts[j][0].isoformat(),
                fig.show_date(pts[j][0], b.granularity),
                {"op": "since", "ref": b.ref(pts[i][0]), "direction": "low" if low else "high"},
                granularity=b.granularity,
            )
        )
        b.value("since_value", pts[j], f"{name(series)} on that earlier date")
        before = [p for p in pts[:i] if p[0] >= pts[i][0] - timedelta(days=30)]
        if before:
            other = (max if low else min)(before, key=lambda p: p[1])
            other_word = "highest" if low else "lowest"
            b.value("recent_other_value", other, f"the {other_word} value in the 30 days before")
            b.when("recent_other_date", other, f"date of that {other_word} value")
        summary = (
            f"{long_name(series)} was {figures.items[0].display} "
            f"{on(pts[i][0], b.granularity)}, the {word} since {figures.items[2].display} "
            f"({gap} days)."
        )
        if before:
            summary += (
                f" In the 30 days before, its {other_word} was {figures.items[4].display} "
                f"{on(other[0], b.granularity)}."
            )
        recent_points = [p for p in pts if p[0] >= pts[i][0] - timedelta(days=45)]
        out.append(
            Candidate(
                id=f"record:{series.series_id}",
                detector="record",
                subject=f"{long_name(series)}, {word} since {figures.items[2].display}",
                summary=summary,
                figures=figures.items,
                reads=[snapshot.read],
                score=min(gap / 365 * (2 if series.series_id in config.prominent else 1), 3.0),
                series={series.series_id},
                latest=pts[i][0],
                group=f"record:{family.id}",
                charts=[chart(snapshot, series, recent_points, name(series))],
                source_ids=[snapshot.source.id],
            )
        )
    return out


def nearest_rank(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


async def jumps(reader: Reader, family: Family, config: InsightsConfig) -> list[Candidate]:
    snapshot = await reader.snapshot(family.source_id)
    _, live = quoted(snapshot)
    out: list[Candidate] = []
    for series in live:
        pts = series.points
        best: tuple[float, int, float] | None = None
        recent = config.jumps.recent_observations.get(series.freq, 1)
        for k in range(max(1, len(pts) - recent), len(pts)):
            start = pts[k][0] - timedelta(days=config.jumps.history_days)
            history = [
                abs(fig.pct(pts[m - 1][1], pts[m][1]))
                for m in range(1, k)
                if pts[m][0] >= start and pts[m - 1][1] != 0
            ]
            if len(history) < 20 or pts[k - 1][1] == 0:
                continue
            usual = nearest_rank(history, 0.99)
            step = fig.pct(pts[k - 1][1], pts[k][1])
            if usual == 0 or step == 0:
                continue  # a series that hardly ever moves has no range to step outside
            ratio = abs(step) / usual
            if ratio >= config.jumps.ratio and (best is None or ratio > best[0]):
                best = (ratio, k, step)
        if best is None:
            continue
        ratio, k, step = best
        figures = Figures()
        b = _Builder(snapshot, series, figures)
        b.value("from_value", pts[k - 1], f"{name(series)} before the step")
        b.when("from_date", pts[k - 1], "observation before the step")
        b.value("to_value", pts[k], f"{name(series)} after the step")
        b.when("to_date", pts[k], "observation after the step")
        b.change("step_pct", pts[k - 1], pts[k], "the step, in percent")
        figures.add(
            Figure(
                "ratio",
                f"the step as a multiple of the usual largest step over {config.jumps.history_days} days (99th percentile)",
                ratio,
                f"{ratio:.1f} times",
                {
                    "op": "step_ratio",
                    "ref": b.ref(pts[k][0]),
                    "history_days": config.jumps.history_days,
                },
            )
        )
        around = [p for p in pts if p[0] >= pts[k][0] - timedelta(days=60)]
        out.append(
            Candidate(
                id=f"jump:{series.series_id}",
                detector="jump",
                subject=f"{long_name(series)}, one step far outside its usual range",
                summary=(
                    f"{long_name(series)} went from {figures.items[0].display} "
                    f"{on(pts[k - 1][0], b.granularity)} to {figures.items[2].display} "
                    f"{on(pts[k][0], b.granularity)}, {figures.items[4].display}: "
                    f"{figures.items[5].display} its usual largest step."
                ),
                figures=figures.items,
                reads=[snapshot.read],
                score=min(1.5 * ratio / config.jumps.ratio, 3.0),
                series={series.series_id},
                latest=pts[k][0],
                group=f"jump:{family.id}",
                charts=[chart(snapshot, series, around, name(series))],
                source_ids=[snapshot.source.id],
            )
        )
    return out


def measure(points: list[Point], how: str) -> dict[date, float]:
    """A series in the measure a reader knows it by, period by period, within one publication."""
    out: dict[date, float] = {}
    for (_, before), (period, value) in zip(points, points[1:], strict=False):
        if how == "level":
            out[period] = value
        elif how == "change":
            out[period] = value - before
        elif how == "monthly_growth":
            out[period] = fig.pct(before, value)
        elif how == "annualised_growth":
            out[period] = 100.0 * ((value / before) ** 4 - 1.0)
        else:
            raise ValueError(f"unknown measure {how}")
    if how == "level" and points:
        out[points[0][0]] = points[0][1]
    return out


def measure_display(value: float, how: str) -> str:
    """Two decimals for rates and growth, so a revision never looks like bad arithmetic: "+0.8%
    to +0.5%, -0.4 points" is true of 0.78 and 0.45 but reads wrong; "+0.78% to +0.45%" does not."""
    if how == "change":
        return f"{value:+,.0f} thousand"
    if how == "level":
        return f"{value:.2f}%"
    return fig.signed_pct(value, 2)


def _granularity(versions: Versions) -> str:
    return "quarter" if versions.series_id.endswith(".Q") else "month"


async def revisions(reader: Reader, config: InsightsConfig, as_of: date) -> list[Candidate]:
    out: list[Candidate] = []
    rev_config = config.revisions
    for entry in rev_config.series:
        versions = await reader.versions(rev_config.source_id, entry.code)
        candidate = _revision(versions, entry, rev_config.recent_periods, as_of)
        if candidate:
            out.append(candidate)
    return out


def _revision(
    versions: Versions, entry: RevisionSeries, recent_periods: int, as_of: date
) -> Candidate | None:
    dates = versions.dates
    if len(dates) < 2:
        return None
    older, newer = dates[-2], dates[-1]
    if newer < as_of - timedelta(days=60):
        return None
    was = measure(versions.versions[older], entry.measure)
    now = measure(versions.versions[newer], entry.measure)
    periods = [p for p in sorted(now) if p in was][-recent_periods:]
    if not periods:
        return None
    period = max(periods, key=lambda p: (abs(now[p] - was[p]), p))
    revision = now[period] - was[period]
    if abs(revision) < entry.threshold:
        return None
    source = versions.source
    granularity = _granularity(versions)
    how = entry.measure

    def mref(published: date) -> dict[str, Any]:
        return {
            "op": "measure",
            "measure": how,
            "ref": ref(source.id, versions.series_id, period, published),
        }

    unit = "points" if how != "change" else "thousand"
    rev_display = f"{revision:+.2f} points" if how != "change" else f"{revision:+,.0f} thousand"
    figures = [
        Figure(
            "period",
            f"the {granularity} revised",
            period.isoformat(),
            fig.show_date(period, granularity),
            {"op": "date", "ref": ref(source.id, versions.series_id, period, newer)},
            granularity=granularity,
        ),
        Figure(
            "was",
            f"{entry.label}, {how.replace('_', ' ')}, as published on {fig.day(older)}",
            was[period],
            measure_display(was[period], how),
            mref(older),
            percent=how != "change",
        ),
        Figure(
            "now",
            f"{entry.label}, {how.replace('_', ' ')}, as published on {fig.day(newer)}",
            now[period],
            measure_display(now[period], how),
            mref(newer),
            percent=how != "change",
        ),
        Figure(
            "revision",
            f"the revision, in {unit}",
            revision,
            rev_display,
            {
                "op": "revision",
                "measure": how,
                "from": ref(source.id, versions.series_id, period, older),
                "to": ref(source.id, versions.series_id, period, newer),
            },
        ),
        Figure(
            "older_published",
            "publication date of the earlier version",
            older.isoformat(),
            fig.day(older),
            {"op": "published", "ref": ref(source.id, versions.series_id, period, older)},
            granularity="day",
        ),
        Figure(
            "newer_published",
            "publication date of the newer version",
            newer.isoformat(),
            fig.day(newer),
            {"op": "published", "ref": ref(source.id, versions.series_id, period, newer)},
            granularity="day",
        ),
    ]
    reads = [versions.raw[d] for d in (older, newer) if d in versions.raw]
    shown = [p for p in sorted(now) if p in was][-recent_periods:]
    return Candidate(
        id=f"revision:{entry.code}",
        detector="revision",
        subject=f"{entry.label}, {fig.show_date(period, granularity)}, revised",
        summary=(
            f"{entry.label} for {figures[0].display} ({how.replace('_', ' ')}) was "
            f"{figures[1].display} as published on {figures[4].display} and "
            f"{figures[2].display} as published on {figures[5].display}: a revision of "
            f"{figures[3].display}."
        ),
        figures=figures,
        reads=reads,
        score=min(abs(revision) / entry.threshold, 3.0),
        series={versions.series_id},
        latest=newer,
        charts=[
            licence.value_block(
                source,
                {
                    older.isoformat(): [[p.isoformat(), was[p]] for p in shown],
                    newer.isoformat(): [[p.isoformat(), now[p]] for p in shown],
                },
                series_id=versions.series_id,
                label=f"{entry.label}, {how.replace('_', ' ')}",
            )
        ],
        source_ids=[source.id],
    )


async def lead_lags(reader: Reader, pair: LeadLag) -> list[Candidate]:
    fsnap = await reader.snapshot(pair.follower.source_id)
    follower = fsnap.series.get(pair.follower.series_id)
    if follower is None or len(follower.points) < 3:
        return []
    pts = follower.points
    change = next(
        (
            i
            for i in range(len(pts) - 1, 0, -1)
            if pts[i][1] != pts[i - 1][1] and (pts[i][0] - pts[i - 1][0]).days >= pair.min_hold_days
        ),
        None,
    )
    if change is None:
        return []
    when = pts[change][0]
    figures = Figures()
    leaders: list[tuple[Snapshot, Series, Point, Point]] = []
    for leader_ref in pair.leaders:
        lsnap = await reader.snapshot(leader_ref.source_id)
        leader = lsnap.series.get(leader_ref.series_id)
        if leader is None:
            continue
        lp = leader.points
        steps = [
            (lp[k - 1], lp[k])
            for k in range(1, len(lp))
            if 1 <= fig.months_between(lp[k][0], when) <= pair.max_lag_months
        ]
        if not steps:
            continue
        start, end = max(steps, key=lambda s: abs(fig.pct(s[0][1], s[1][1])))
        if abs(fig.pct(start[1], end[1])) >= pair.min_move_pct:
            leaders.append((lsnap, leader, start, end))
    if not leaders:
        return []

    fb = _Builder(fsnap, follower, figures)
    fb.decimals = 2
    sentences = []
    reads = [fsnap.read]
    charts = []
    for lsnap, leader, start, end in leaders:
        lb = _Builder(lsnap, leader, figures)
        key = leader.series_id.split(".")[2].lower()
        lb.value(f"{key}_from_value", start, f"{name(leader)} the month before its move")
        lb.when(f"{key}_from_date", start, f"the month before the move in {name(leader)}")
        lb.value(f"{key}_to_value", end, f"{name(leader)} after its move")
        lb.when(f"{key}_to_date", end, f"the month of the move in {name(leader)}")
        lb.change(f"{key}_pct", start, end, f"the move in {name(leader)} in one month")
        lag = fig.months_between(end[0], when)
        figures.add(
            Figure(
                f"{key}_lag_months",
                f"months from the move in {name(leader)} to the follower's change",
                float(lag),
                f"{lag} months",
                {"op": "months", "from": lb.ref(end[0]), "to": fb.ref(when)},
            )
        )
        sentences.append(
            f"{name(leader)} ({leader.unit}) went from {fig.number(start[1], lb.decimals)} in "
            f"{fig.month(start[0])} to {fig.number(end[1], lb.decimals)} in {fig.month(end[0])}, "
            f"{fig.signed_pct(fig.pct(start[1], end[1]))}, {lag} months before the change."
        )
        if lsnap.read not in reads:
            reads.append(lsnap.read)
        start_at = when - timedelta(days=270)
        charts.append(
            chart(lsnap, leader, [p for p in leader.points if p[0] >= start_at], name(leader))
        )
    held_since = pts[change - 1]
    fb.value("held_value", held_since, f"{name(follower)} during the hold", percent=True)
    figures.add(
        Figure(
            "held_since",
            f"when {name(follower)} was last changed before the hold",
            held_since[0].isoformat(),
            fig.day(held_since[0]),
            {"op": "held_since", "ref": fb.ref(when)},
            granularity="day",
        )
    )
    figures.add(
        Figure(
            "held_days",
            "days the follower was held unchanged",
            float((when - held_since[0]).days),
            f"{(when - held_since[0]).days} days",
            {"op": "days", "from": fb.ref(held_since[0]), "to": fb.ref(when)},
        )
    )
    if change >= 2:
        fb.value("before_hold_value", pts[change - 2], f"{name(follower)} before the hold", True)
    later = pts[change:]
    for n, point in enumerate(later, 1):
        fb.value(f"change_{n}_value", point, f"{name(follower)} after change {n}", percent=True)
        fb.when(f"change_{n}_date", point, f"date of change {n} after the hold")
    moves = ", then ".join(f"{v:.2f}% on {fig.day(p)}" for p, v in later)
    sentences.append(
        f"{long_name(follower)} had been {held_since[1]:.2f}% since {fig.day(held_since[0])}"
        + (f" (before that {pts[change - 2][1]:.2f}%)" if change >= 2 else "")
        + f", and then went to {moves}."
    )
    charts.append(
        licence.value_block(
            fsnap.source,
            [[p.isoformat(), v] for p, v in pts if p >= when - timedelta(days=720)],
            series_id=follower.series_id,
            label=name(follower),
            unit=follower.unit,
            step=True,
        )
    )
    return [
        Candidate(
            id=f"lead_lag:{pair.id}",
            detector="lead_lag",
            subject=f"{', '.join(name(s) for _, s, _, _ in leaders)} moved, then {name(follower)} changed",
            summary=" ".join(sentences),
            figures=figures.items,
            reads=reads,
            score=2.0,
            series={follower.series_id, *(s.series_id for _, s, _, _ in leaders)},
            charts=charts,
            source_ids=sorted({fsnap.source.id, *(s.source.id for s, _, _, _ in leaders)}),
        )
    ]


# --- Scan and rank -------------------------------------------------------------------------


async def scan(reader: Reader, config: InsightsConfig, as_of: date) -> list[Candidate]:
    """Every detector, every configured input. A detector that cannot read its data is logged
    and skipped: one missing source must not cost the dashboard its other insights."""
    found: list[Candidate] = []

    async def run(label: str, coro: Any) -> None:
        try:
            found.extend(await coro)
        except DataError as exc:
            log.warning("detector skipped", extra={"detector": label, "reason": str(exc)})

    for family in config.families:
        await run(f"move {family.id}", moves(reader, family))
        await run(f"record {family.id}", records(reader, family, config))
        await run(f"jump {family.id}", jumps(reader, family, config))
    await run("revision", revisions(reader, config, as_of))
    for pair in config.lead_lag:
        await run(f"lead_lag {pair.id}", lead_lags(reader, pair))
    return found


def freshness(candidate: Candidate, as_of: date) -> float:
    """News first: a finding about last week outranks an equally strong one from last quarter.
    A pattern over months (lead and lag) is not discounted."""
    if candidate.latest is None:
        return 1.0
    age = (as_of - candidate.latest).days
    return 1.0 if age <= 14 else 0.75 if age <= 60 else 0.5


def ranked(candidate: Candidate, as_of: date) -> float:
    return round(candidate.score * freshness(candidate, as_of), 6)


def shortlist(
    candidates: list[Candidate],
    size: int,
    per_detector: int,
    as_of: date,
    prominent: tuple[str, ...] = (),
) -> list[Candidate]:
    """Highest ranked first, at most `per_detector` of a kind and one per group, never the same
    series twice. Ties go to the series a reader knows best (earliest in `prominent`), then by
    id, so the order is the same on every run."""
    chosen: list[Candidate] = []
    seen: set[str] = set()
    groups: set[str] = set()
    per: Counter[str] = Counter()

    def known(c: Candidate) -> int:
        return min((prominent.index(s) for s in c.series if s in prominent), default=len(prominent))

    def order(c: Candidate) -> tuple[float, int, str]:
        return (-ranked(c, as_of), known(c), c.id)

    for candidate in sorted(candidates, key=order):
        if candidate.series & seen or per[candidate.detector] >= per_detector:
            continue
        if candidate.group and candidate.group in groups:
            continue
        chosen.append(candidate)
        seen |= candidate.series
        per[candidate.detector] += 1
        if candidate.group:
            groups.add(candidate.group)
        if len(chosen) == size:
            break
    return chosen
