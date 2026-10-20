import pyarrow as pa
import pyarrow.compute as pc

from dome_open_data.model import TIMESERIES_SCHEMA


class ValidationError(RuntimeError):
    pass


def validate_timeseries(table: pa.Table, expected_rows: int) -> None:
    """Gate before anything reaches clean/. Session 3 adds the vintage-to-vintage comparison."""
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
