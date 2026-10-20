from datetime import date

import pytest

from dome_open_data.connectors.worldbank import unit_from_title
from dome_open_data.model import parse_period


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2024", (date(2024, 1, 1), "A")),
        ("2024Q3", (date(2024, 7, 1), "Q")),
        ("2024M11", (date(2024, 11, 1), "M")),
    ],
)
def test_parse_period(text: str, expected: tuple[date, str]) -> None:
    assert parse_period(text) == expected


@pytest.mark.parametrize("text", ["24", "2024Q5", "2024M13", "2024-01"])
def test_parse_period_rejects_unknown_formats(text: str) -> None:
    with pytest.raises(ValueError):
        parse_period(text)


@pytest.mark.parametrize(
    ("title", "unit"),
    [
        ("GDP (current US$)", "current US$"),
        (
            "Unemployment, total (% of total labor force) (modeled ILO estimate)",
            "% of total labor force",
        ),
        ("Population, total", ""),
    ],
)
def test_unit_from_title(title: str, unit: str) -> None:
    assert unit_from_title(title) == unit
