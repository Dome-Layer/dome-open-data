"""The store's own record, assembled into one small JSON object for the public ledger page.

What the page shows is metadata, counts and provenance: which sources are held, under which
licence, how many dated vintages exist, how fresh each one is, and how the last few runs went.
**It never carries an observation value.** That keeps the bucket private and sidesteps the
licences whose `commercial_ok` is `ask`, because what those restrict is redistributing the data,
not the fact that we collect it.

It is assembled here, by the collector, rather than read live by the website: a Worker has a
10 ms CPU budget and the catalog is Parquet. One object, written at the end of every run, is the
whole data path (decided 2026-11-17).

The honesty rule from the brief applies to this file above all: failures, stale sources and
sources that have never collected are published as they are. A ledger that showed only the good
weeks would be marketing, and worthless as the evidence the market test piece claims it is.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from dome_open_data import catalog, licence, ops
from dome_open_data.registry import Source
from dome_open_data.storage import Storage

LEDGER_KEY = "public/ledger.json"

# How many of the most recent runs each source publishes. Enough to show a failure and its
# recovery next to each other, small enough that the whole object stays a few tens of KiB.
RECENT_RUNS = 5

# Runs whose change token was unchanged wrote no run row until dome-open-data #12 landed on this
# date, so a gap in the history before it means "nothing to collect", not "no run". The page says
# so rather than letting a reader count the gaps as missed runs.
RUN_HISTORY_COMPLETE_FROM = date(2026, 11, 16)

# Query-string values that must never be published, in case an error message quotes the URL that
# produced it. No source needs a key today; FRED does, and Phase 2 adds it.
_SECRET_PARAM = re.compile(
    r"([?&](?:api_?key|key|token|secret|password|auth|signature)=)[^&\s]+",
    re.IGNORECASE,
)
MAX_ERROR_CHARS = 300


def public_error(error: str | None) -> str | None:
    """An error message fit to publish: secrets out of any URL it quotes, and bounded in length."""
    if not error:
        return None
    cleaned = _SECRET_PARAM.sub(r"\1REDACTED", " ".join(error.split()))
    if len(cleaned) > MAX_ERROR_CHARS:
        cleaned = cleaned[: MAX_ERROR_CHARS - 1].rstrip() + "…"
    return cleaned


def _timestamp(value: Any) -> str | None:
    if isinstance(value, datetime):
        at = value if value.tzinfo else value.replace(tzinfo=UTC)
        return at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return None


def _run_entry(row: dict[str, Any]) -> dict[str, Any]:
    vintage = row.get("vintage")
    return {
        "status": row["status"],
        "started_at": _timestamp(row.get("started_at")),
        "finished_at": _timestamp(row.get("finished_at")),
        "vintage": vintage.isoformat() if isinstance(vintage, date) else None,
        "rows": int(row.get("rows_clean") or 0),
        "error": public_error(row.get("error")),
    }


@dataclass(frozen=True, slots=True)
class SourceLedger:
    """One row of the published table, with nothing in it that a licence could object to."""

    source: Source
    vintages: list[date]
    freshness: ops.Freshness
    series: int | None
    rows: int | None
    runs: list[dict[str, Any]]

    def as_json(self) -> dict[str, Any]:
        newest = self.vintages[-1] if self.vintages else None
        return {
            "id": self.source.id,
            "name": self.source.name,
            "publisher": self.source.publisher,
            "dataset": self.source.dataset,
            "domain": self.source.domain,
            "endpoint": self.source.endpoint,
            "cadence": self.source.cadence,
            "newest_vintage": newest.isoformat() if newest else None,
            "vintages_held": len(self.vintages),
            "first_vintage": self.vintages[0].isoformat() if self.vintages else None,
            "age_days": self.freshness.age_days,
            "staleness_limit_days": self.freshness.limit_days,
            "stale": self.freshness.stale,
            "never_collected": newest is None,
            "series": self.series,
            "rows": self.rows,
            "licence": {
                "name": self.source.licence.name,
                "commercial_ok": self.source.licence.commercial_ok,
                "share_alike": self.source.licence.share_alike,
                "attribution": self.source.licence.attribution,
            },
            "recent_runs": self.runs,
        }


async def source_ledger(storage: Storage, source: Source, today: date) -> SourceLedger:
    vintages = await catalog.vintages(storage, source)
    newest = vintages[-1] if vintages else None
    freshness = ops.Freshness(
        source_id=source.id,
        cadence=source.cadence,
        vintage=newest,
        age_days=(today - newest).days if newest else None,
        limit_days=source.expected_staleness_days,
    )
    runs = await catalog.recent_runs(storage, source.id, limit=RECENT_RUNS)
    rows = next(
        (int(row["rows_clean"]) for row in runs if row["status"] == "collected"),
        None,
    )
    return SourceLedger(
        source=source,
        vintages=vintages,
        freshness=freshness,
        series=await catalog.series_count(storage, source, newest) if newest else None,
        rows=rows,
        runs=[_run_entry(row) for row in runs],
    )


async def build(
    storage: Storage, sources: list[Source], today: date, as_of: datetime | None = None
) -> dict[str, Any]:
    """Assemble the whole ledger. One pass over the catalog, no observation values read."""
    entries = [await source_ledger(storage, source, today) for source in sources]
    usage = await ops.usage(storage)
    moment = (as_of or datetime.now(UTC)).astimezone(UTC)
    return {
        "as_of": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "store": {
            "objects": usage.objects,
            "bytes": usage.total_bytes,
            "free_tier_bytes": 10 * 1000**3,
            "over_alert": usage.over,
        },
        "totals": {
            "sources": len(entries),
            "stale": sum(1 for e in entries if e.freshness.stale),
            "vintages": sum(len(e.vintages) for e in entries),
            "series": sum(e.series or 0 for e in entries),
            "rows_newest_vintage": sum(e.rows or 0 for e in entries),
        },
        # Said out loud because a reader would otherwise count the gaps as missed runs.
        "run_history_complete_from": RUN_HISTORY_COMPLETE_FROM.isoformat(),
        "sources": [entry.as_json() for entry in sorted(entries, key=lambda e: e.source.id)],
    }


def serialise(ledger: dict[str, Any]) -> bytes:
    return licence.serialise(ledger)


async def write(storage: Storage, ledger: dict[str, Any], sources: Mapping[str, Source]) -> int:
    """Overwrite `public/ledger.json` through the licence gate.

    The ledger carries no value block today, so the gate passes it; going through it anyway means
    a value added to the ledger later is checked like any other public value.
    """
    return await licence.publish(storage, LEDGER_KEY, ledger, sources)
