from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Protocol

import httpx

from dome_open_data.model import Observation
from dome_open_data.registry import Source


@dataclass(frozen=True, slots=True)
class RawFile:
    name: str
    url: str
    data: bytes


@dataclass(frozen=True, slots=True)
class SeriesMeta:
    series_id: str
    code: str
    geo: str
    freq: str
    title: str
    definition: str | None
    unit: str
    seasonal_adjustment: str | None
    source_url: str


@dataclass
class Collected:
    raw_files: list[RawFile] = field(default_factory=list)
    observations: list[Observation] = field(default_factory=list)
    series: list[SeriesMeta] = field(default_factory=list)
    rows_raw: int = 0
    dropped: dict[str, int] = field(default_factory=dict)


class ConnectorImpl(Protocol):
    async def change_token(self, client: httpx.AsyncClient, source: Source) -> str: ...

    async def collect(
        self, client: httpx.AsyncClient, source: Source, vintage: date
    ) -> Collected: ...

    def count_clean_rows(self, files: Mapping[str, bytes]) -> int:
        """Recount, straight from the raw files, the rows the clean file must hold."""
        ...


def connector_for(source: Source) -> ConnectorImpl:
    from dome_open_data.connectors.worldbank import WorldBank

    implementations: dict[tuple[str, str | None], ConnectorImpl] = {
        ("rest", "worldbank"): WorldBank(),
    }
    try:
        return implementations[(source.connector, source.dialect)]
    except KeyError:
        raise NotImplementedError(
            f"no connector for {source.connector}/{source.dialect} ({source.id})"
        ) from None
