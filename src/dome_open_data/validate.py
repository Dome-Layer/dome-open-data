from dataclasses import dataclass
from datetime import date

import pyarrow as pa
import pyarrow.compute as pc

from dome_open_data.model import TIMESERIES_SCHEMA

# A new vintage may not lose more than half of the previous vintage's rows or series.
MAX_SHRINK = 0.5


class ValidationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class VintageStats:
    """What the previous vintage of a source held, read from its manifest and series file."""

    vintage: date
    rows: int
    series: int | None


def validate_timeseries(table: pa.Table, expected_rows: int) -> None:
    """Gate before anything reaches clean/."""
    if not table.schema.equals(TIMESERIES_SCHEMA):
        raise ValidationError(f"schema mismatch: {table.schema}")
    if table.num_rows == 0:
        raise ValidationError("no rows")
    if table.num_rows != expected_rows:
        raise ValidationError(f"{table.num_rows} rows, raw files hold {expected_rows}")
    for column in ("series_id", "geo", "freq", "unit"):
        if table[column].null_count:
            raise ValidationError(f"nulls in {column}")
    if pc.any(pc.is_nan(table["value"])).as_py():
        raise ValidationError("NaN in value")
    keys = table.group_by(["series_id", "period"]).aggregate([([], "count_all")])
    if keys.num_rows != table.num_rows:
        raise ValidationError(
            f"{table.num_rows - keys.num_rows} duplicate (series_id, period) rows"
        )


def validate_against_previous(rows: int, series: int, previous: VintageStats) -> None:
    """Compare a new vintage with the one before it, and block the clean write if it collapsed.

    Columns and dtypes need no comparison here: every clean file is written against the one
    `TIMESERIES_SCHEMA`, which `validate_timeseries` has already enforced, so a connector cannot
    quietly change them. In a long-format store the equivalent of a column disappearing is a
    series disappearing, so the same rule is applied to the series count.

    What this catches is the failure the schema check cannot see: a source that answers with a
    fraction of its data, which is how a truncated download, a silently changed query or a
    publisher mid-revision arrives. A smaller drop is left alone: revisions legitimately remove
    rows, and an alert that fires on every revision would be ignored within a month.
    """
    if rows * 2 < previous.rows:
        raise ValidationError(
            f"{rows} rows against {previous.rows} in vintage {previous.vintage}: "
            f"a drop of more than {MAX_SHRINK:.0%} blocks the write"
        )
    if previous.series is not None and series * 2 < previous.series:
        raise ValidationError(
            f"{series} series against {previous.series} in vintage {previous.vintage}: "
            f"a drop of more than {MAX_SHRINK:.0%} blocks the write"
        )
