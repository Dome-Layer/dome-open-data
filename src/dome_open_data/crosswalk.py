"""Country code crosswalk (`crosswalk/` at the repository root).

- `countries.csv`: one row per country, territory or historical entity: ISO alpha-3, alpha-2,
  UN M49, name, kind (`current` or `historical`), the World Bank and IMF codes where they
  exist, and successors for historical entities. Built by `python -m dome_open_data crosswalk`
  from the UN WPP location list, the World Bank country API, the IMF WEO country list and
  the hand-kept `historical.csv`.
- `aggregates.csv`: region and group codes per publisher (World Bank `WLD`, IMF `G001`, UN
  M49 `UN900`...). Clean data keeps these codes in `geo`; this file says what they are.

In clean data `geo` is the ISO alpha-3 code for countries and territories (publisher codes
that differ are mapped, for example IMF `UVK` to `XKX`), the ISO 3166-3 former code for
historical entities (`SUN`, `YUG`, `CSK`), and the publisher's own code for aggregates.
"""

import csv
from functools import cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "crosswalk"
COUNTRY_COLUMNS = ["iso3", "iso2", "m49", "name", "kind", "worldbank", "imf", "successors", "note"]
AGGREGATE_COLUMNS = ["publisher", "code", "name"]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


@cache
def countries(root: Path = ROOT) -> list[dict[str, str]]:
    return read_csv(root / "countries.csv")


def publisher_codes(publisher: str, root: Path = ROOT) -> dict[str, str]:
    """Publisher code -> ISO alpha-3, only where the two differ."""
    return {
        row[publisher]: row["iso3"]
        for row in countries(root)
        if row.get(publisher) and row[publisher] != row["iso3"]
    }
