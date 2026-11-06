"""The checks that run while nobody is watching.

Collection failures are loud on their own: the job fails and the workflow opens an issue. These
two are the quiet failures. A source can stop refreshing without anyone noticing, because the
last good vintage still answers every query, and the free bucket can fill up until the first
write fails. Both are caught here on a weekly schedule.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from dome_open_data import catalog
from dome_open_data.registry import Source
from dome_open_data.storage import Storage

GIB = 1024**3
# The R2 free tier is 10 GB. Alerting at 8 leaves room to decide what to prune.
STORAGE_ALERT_BYTES = 8 * GIB


@dataclass(frozen=True, slots=True)
class Freshness:
    source_id: str
    cadence: str
    vintage: date | None
    age_days: int | None
    limit_days: int

    @property
    def stale(self) -> bool:
        return self.age_days is None or self.age_days > self.limit_days

    @property
    def line(self) -> str:
        state = "STALE" if self.stale else "ok"
        if self.vintage is None:
            return f"{state:<5} {self.source_id:<24} never collected (limit {self.limit_days} d)"
        return (
            f"{state:<5} {self.source_id:<24} newest {self.vintage.isoformat()}, "
            f"{self.age_days} d old (limit {self.limit_days} d)"
        )


@dataclass(frozen=True, slots=True)
class Usage:
    objects: int
    total_bytes: int

    @property
    def over(self) -> bool:
        return self.total_bytes >= STORAGE_ALERT_BYTES

    @property
    def line(self) -> str:
        state = "OVER " if self.over else "ok"
        return (
            f"{state} {self.objects} objects, {self.total_bytes / GIB:.2f} GiB "
            f"(alert at {STORAGE_ALERT_BYTES / GIB:.0f} GiB, free tier 10 GB)"
        )


async def freshness(storage: Storage, sources: Iterable[Source], today: date) -> list[Freshness]:
    """How old each source's newest clean vintage is, against its own expectation.

    A source that has never been collected counts as stale: on a registry entry that has been
    sitting unused for a week, that is exactly the thing worth saying out loud.
    """
    report = []
    for source in sources:
        found = await catalog.vintages(storage, source)
        newest = found[-1] if found else None
        report.append(
            Freshness(
                source_id=source.id,
                cadence=source.cadence,
                vintage=newest,
                age_days=(today - newest).days if newest else None,
                limit_days=source.expected_staleness_days,
            )
        )
    return report


async def usage(storage: Storage) -> Usage:
    objects, total = await storage.usage()
    return Usage(objects=objects, total_bytes=total)
