"""`public/dashboard.json`: everything the Open Data dashboard shows, in one object.

Assembled after each collection: scan, write, check (the `insights` package), then the key
figures with their sparklines, the flagship revision panel, the store totals and the run history
(the ledger's own assembly), all through the licence gate. Values travel only in value blocks
naming a source whose licence allows them; the IMF sources appear in the counts and nowhere else.

**Honesty rules, as for the ledger.** An insight that fails its check is not published and is
listed under `dropped` (id, tag and why, never its text), and the run history shows the failed
runs too. If the writer cannot be reached, the dashboard still publishes, with the insights that
could be kept and the error said out loud, and the command then exits non-zero so the job is red.

Each published or dropped insight also leaves a governance event (agent `open-data`), in the
canonical `governance_events` row shape, under `catalog/governance/` in the private bucket.
Forwarding those rows to the production table is a separate decision (TOOL_CONTEXT.md).
"""

import hashlib
import json
import logging
import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from dome_open_data import ledger, licence
from dome_open_data.insights import check as checker
from dome_open_data.insights import scan, write
from dome_open_data.insights.config import Flagship, InsightsConfig, KeyFigure
from dome_open_data.insights.data import DataError, Reader
from dome_open_data.insights.figures import Figure
from dome_open_data.registry import Source
from dome_open_data.storage import Storage

log = logging.getLogger(__name__)

DASHBOARD_KEY = "public/dashboard.json"
GOVERNANCE_PREFIX = "catalog/governance/agent=open-data/"
AGENT_ID = "open-data"
RUN_HISTORY_DAYS = 14
RULES = [
    "figures_recomputed_from_store",
    "every_number_matches_a_figure",
    "no_causal_claim",
    "house_style_no_dashes",
    "licence_gate_values",
]


@dataclass(slots=True)
class Outcome:
    """What one run of the stage did, for the log line and the governance events."""

    shortlisted: int = 0
    kept: int = 0
    written: int = 0
    published: int = 0
    dropped: int = 0
    model: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    writer_error: str | None = None
    events: list[dict[str, Any]] = field(default_factory=list)


# --- Key figures and the flagship -----------------------------------------------------------


def _month_end(day: date) -> date:
    following = (day.replace(day=28) + timedelta(days=4)).replace(day=1)
    return following - timedelta(days=1)


async def key_figure(reader: Reader, item: KeyFigure, as_of: date) -> dict[str, Any]:
    snapshot = await reader.snapshot(item.source_id)
    series = snapshot.series.get(item.series_id)
    if series is None:
        raise DataError(f"{item.series_id} is not in {item.source_id}")
    if item.step == "monthly":
        # A rate set on the days it changes, shown as its level at each month end.
        months: list[date] = []
        cursor = as_of.replace(day=1)
        for _ in range(item.points):
            months.append(_month_end(cursor))
            cursor = (cursor - timedelta(days=1)).replace(day=1)
        points: list[list[Any]] = []
        for end in reversed(months):
            held = series.until(end)
            if held:
                points.append([end.isoformat(), held[-1][1]])
        since = series.last[0]
    else:
        points = [[p.isoformat(), v] for p, v in series.points[-item.points :]]
        since = None
    first, last = float(points[0][1]), float(points[-1][1])
    return licence.value_block(
        snapshot.source,
        points,
        label=item.label,
        series_id=item.series_id,
        unit=series.unit,
        decimals=item.decimals,
        latest_period=series.last[0].isoformat(),
        latest_reading=series.last[1],
        **({"unchanged_since": since.isoformat()} if since else {}),
        change_pct=round(100.0 * (last / first - 1.0), 4) if first else None,
        change_from=points[0][0],
        vintage=snapshot.vintage.isoformat(),
    )


async def flagship(reader: Reader, item: Flagship) -> dict[str, Any]:
    """Each quarter of 2008 as every publication stated it: annualised growth over the quarter
    before, so versions in different base years compare (levels never do)."""
    versions = await reader.versions(item.source_id, item.code)
    quarters = []
    for text in item.quarters:
        quarter = date.fromisoformat(text)
        before = (quarter.replace(day=1) - timedelta(days=1)).replace(day=1)
        before = before.replace(month=((before.month - 1) // 3) * 3 + 1)
        line = []
        for published in versions.dates:
            values = dict(versions.versions[published])
            if quarter in values and before in values and values[before]:
                growth = 100.0 * ((values[quarter] / values[before]) ** 4 - 1.0)
                line.append([published.isoformat(), round(growth, 4)])
        quarters.append({"quarter": text, "growth_by_publication": line})
    return licence.value_block(
        versions.source,
        quarters,
        series_id=versions.series_id,
        measure="annualised growth over the previous quarter, percent",
        publications=len(versions.dates),
    )


# --- Insights -------------------------------------------------------------------------------


def _figure_source(figure: Figure) -> str:
    calc = figure.calc
    for key in ("ref", "from", "to"):
        if isinstance(calc.get(key), dict):
            return str(calc[key]["source_id"])
    return str(calc["source_id"])


def _evidence(
    candidate: scan.Candidate, verdict: checker.Verdict, sources: Mapping[str, Source]
) -> dict[str, Any]:
    by_source: dict[str, list[dict[str, Any]]] = {}
    for figure in candidate.figures:
        by_source.setdefault(_figure_source(figure), []).append(figure.as_json())
    return {
        "reads": [read.as_json() for read in candidate.reads],
        "figures": [
            licence.value_block(sources[source_id], figures)
            for source_id, figures in sorted(by_source.items())
        ],
        "matches": verdict.matches,
    }


def _input_hash(candidate: scan.Candidate) -> str:
    body = json.dumps(candidate.for_model(), sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(body.encode()).hexdigest()


def _event(
    candidate: scan.Candidate,
    verdict: checker.Verdict,
    published: bool,
    summary: str,
    model: str | None,
    at: datetime,
) -> dict[str, Any]:
    """One `governance_events` row, in the canonical schema's columns."""
    return {
        "agent_id": AGENT_ID,
        "action_type": "insight_published" if published else "insight_dropped",
        "timestamp": at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "input_hash": _input_hash(candidate),
        "input_type": "insight_candidate",
        "output_summary": summary[:500],
        "rules_applied": RULES,
        "rules_triggered": [] if published else verdict.problems[:20],
        "confidence": (verdict.checked / verdict.total) if verdict.total else 0.0,
        "human_in_loop": None,
        "user_id": None,
        "workflow_run_id": os.environ.get("GITHUB_RUN_ID") or None,
        "metadata": {
            "candidate_id": candidate.id,
            "fingerprint": candidate.fingerprint,
            "detector": candidate.detector,
            "model": model,
            "figures_checked": verdict.label,
            "reads": [read.as_json() for read in candidate.reads],
        },
    }


async def previous(storage: Storage) -> dict[str, dict[str, Any]]:
    """Published insights of the last dashboard, by fingerprint, so their text can be kept."""
    data = await storage.get(DASHBOARD_KEY)
    if data is None:
        return {}
    try:
        old = json.loads(data)
        return {item["fingerprint"]: item for item in old.get("insights", [])}
    except (ValueError, KeyError, TypeError):
        log.warning("previous dashboard unreadable; every insight is written afresh")
        return {}


async def insights(
    storage: Storage,
    sources: Mapping[str, Source],
    config: InsightsConfig,
    as_of: datetime,
    call: write.Call | None = None,
    use_model: bool = True,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Outcome]:
    outcome = Outcome()
    today = as_of.date()
    found = await scan.scan(Reader(storage, sources), config, today)
    chosen = scan.shortlist(
        found, config.shortlist.size, config.shortlist.per_detector, today, config.prominent
    )
    outcome.shortlisted = len(chosen)

    kept = await previous(storage)
    drafts: dict[str, checker.Draft] = {}
    model_of: dict[str, str | None] = {}
    to_write = []
    for candidate in chosen:
        old = kept.get(candidate.fingerprint)
        if old and old.get("id") == candidate.id:
            drafts[candidate.id] = checker.Draft(
                candidate.id, old["tag"], old["headline"], old["body"], old["figures_used"]
            )
            model_of[candidate.id] = old.get("written_by")
            outcome.kept += 1
        else:
            to_write.append(candidate)
    if to_write and use_model:
        try:
            written = await write.write(to_write, config.writer, call)
        except write.WriterError as exc:
            outcome.writer_error = str(exc)
            log.error("the writer failed", extra={"reason": str(exc)})
        else:
            outcome.model = written.model
            outcome.input_tokens = written.input_tokens
            outcome.output_tokens = written.output_tokens
            for answer in written.drafts:
                if answer.candidate_id in {c.id for c in to_write}:
                    drafts.setdefault(answer.candidate_id, answer)
                    model_of[answer.candidate_id] = written.model
                    outcome.written += 1

    check_reader = Reader(storage, sources)  # never the scan's: the check reads everything again
    published: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    for candidate in chosen:
        draft = drafts.get(candidate.id)
        if draft is None:
            verdict = checker.Verdict(candidate.id, problems=["no text was written for it"])
        else:
            verdict = await checker.check(candidate, draft, check_reader)
        if draft is not None and verdict.passed:
            published.append(
                {
                    "id": candidate.id,
                    "fingerprint": candidate.fingerprint,
                    "detector": candidate.detector,
                    "tag": candidate.tag,
                    "headline": draft.headline,
                    "body": draft.body,
                    "caveat": candidate.caveat,
                    "figures_used": draft.figures_used,
                    "figures_checked": verdict.label,
                    "checked": verdict.checked,
                    "total": verdict.total,
                    "written_by": model_of.get(candidate.id),
                    "sources": [
                        {
                            "source_id": sid,
                            "name": sources[sid].name,
                            "attribution": sources[sid].licence.attribution,
                        }
                        for sid in candidate.source_ids
                    ],
                    "charts": candidate.charts,
                    "evidence": _evidence(candidate, verdict, sources),
                }
            )
            outcome.events.append(
                _event(candidate, verdict, True, draft.headline, model_of.get(candidate.id), as_of)
            )
        else:
            log.warning(
                "insight dropped",
                extra={"candidate": candidate.id, "problems": verdict.problems[:10]},
            )
            # Public: which one and why in kind, never the text that failed.
            dropped.append(
                {
                    "id": candidate.id,
                    "tag": candidate.tag,
                    "problems": len(verdict.problems),
                    "figures_checked": verdict.label,
                }
            )
            outcome.events.append(
                _event(
                    candidate,
                    verdict,
                    False,
                    "dropped: " + "; ".join(verdict.problems[:3]),
                    model_of.get(candidate.id),
                    as_of,
                )
            )
    outcome.published = len(published)
    outcome.dropped = len(dropped)
    return published, dropped, outcome


# --- The whole object -----------------------------------------------------------------------


async def build(
    storage: Storage,
    sources: Mapping[str, Source],
    config: InsightsConfig,
    as_of: datetime | None = None,
    call: write.Call | None = None,
    use_model: bool = True,
) -> tuple[dict[str, Any], Outcome]:
    moment = (as_of or datetime.now(UTC)).astimezone(UTC)
    today = moment.date()
    published, dropped, outcome = await insights(storage, sources, config, moment, call, use_model)
    reader = Reader(storage, sources)
    figures = []
    for item in config.key_figures:
        try:
            figures.append(await key_figure(reader, item, today))
        except DataError as exc:
            log.warning("key figure skipped", extra={"label": item.label, "reason": str(exc)})
    try:
        flagship_panel: dict[str, Any] | None = await flagship(reader, config.flagship)
    except DataError as exc:
        log.warning("flagship skipped", extra={"reason": str(exc)})
        flagship_panel = None

    registry = list(sources.values())
    collector = await ledger.build(storage, registry, today, moment)
    history = await ledger.run_history(storage, registry, today - timedelta(days=RUN_HISTORY_DAYS))
    used = sorted(
        {sid for item in published for sid in (s["source_id"] for s in item["sources"])}
        | {f["source_id"] for f in figures}
        | ({config.flagship.source_id} if flagship_panel else set())
    )
    dashboard = {
        "as_of": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "how": (
            "Code shortlists what changed, Claude writes it, and code proves every figure "
            "against the store or the insight is not published."
        ),
        "generated": {
            "shortlisted": outcome.shortlisted,
            "published": outcome.published,
            "dropped": outcome.dropped,
            "kept_from_previous": outcome.kept,
            "written_now": outcome.written,
            "model": outcome.model,
            "writer_error": outcome.writer_error,
        },
        "key_figures": figures,
        "insights": published,
        "dropped": dropped,
        "flagship": flagship_panel,
        "collector": {
            "store": collector["store"],
            "totals": collector["totals"],
            "stale": [s["id"] for s in collector["sources"] if s["stale"]],
            "never_collected": [s["id"] for s in collector["sources"] if s["never_collected"]],
            "run_history_complete_from": collector["run_history_complete_from"],
        },
        "runs": history,
        "attribution": [
            {"source_id": sid, "attribution": sources[sid].licence.attribution} for sid in used
        ],
    }
    return dashboard, outcome


async def write_events(storage: Storage, outcome: Outcome, at: datetime) -> str | None:
    if not outcome.events:
        return None
    key = f"{GOVERNANCE_PREFIX}run={at:%Y%m%dT%H%M%S%fZ}-{uuid.uuid4().hex[:8]}.json"
    await storage.put_new(key, json.dumps(outcome.events, ensure_ascii=False, indent=1).encode())
    return key


async def publish(
    storage: Storage, dashboard: dict[str, Any], sources: Mapping[str, Source]
) -> int:
    return await licence.publish(storage, DASHBOARD_KEY, dashboard, sources)
