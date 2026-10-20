from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Domain = Literal["economy", "trade", "climate", "demographics", "longrun"]
Connector = Literal["sdmx", "rest", "bulk_file"]
Cadence = Literal["daily", "weekly", "monthly", "yearly"]
ChangeSignal = Literal["etag", "last_modified", "sdmx_updated_after", "catalogue_date", "none"]


class RegistryError(ValueError):
    pass


class Licence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1)
    commercial_ok: Literal["yes", "ask", "no"]
    share_alike: bool
    attribution: str = Field(min_length=1)

    @field_validator("commercial_ok", mode="before")
    @classmethod
    def _yaml_booleans(cls, value: Any) -> Any:
        # YAML 1.1 reads an unquoted yes/no as a boolean.
        if isinstance(value, bool):
            return "yes" if value else "no"
        return value


class RateLimit(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    per_second: float = Field(gt=0)


class Source(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(pattern=r"^[a-z0-9]+(_[a-z0-9]+)*$")
    name: str = Field(min_length=1)
    publisher: str = Field(pattern=r"^[a-z0-9]+$")
    dataset: str = Field(pattern=r"^[a-z0-9]+(_[a-z0-9]+)*$")
    domain: Domain
    connector: Connector
    dialect: str | None = None
    endpoint: str = Field(pattern=r"^https://")
    cadence: Cadence
    change_signal: ChangeSignal
    rate_limit: RateLimit
    auth_secret: str | None = None
    keep_raw: bool
    expected_staleness_days: int = Field(gt=0)
    licence: Licence
    options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _id_matches_publisher_dataset(self) -> "Source":
        if self.id != f"{self.publisher}_{self.dataset}":
            raise ValueError(f"id must be '{self.publisher}_{self.dataset}', got '{self.id}'")
        return self

    @property
    def raw_prefix(self) -> str:
        return f"raw/{self.publisher}/{self.dataset}"

    @property
    def clean_prefix(self) -> str:
        return f"clean/{self.domain}/{self.publisher}__{self.dataset}"


def parse_registry(entries: Any) -> dict[str, Source]:
    if not isinstance(entries, list):
        raise RegistryError("registry must be a YAML list of sources")
    sources: dict[str, Source] = {}
    for index, entry in enumerate(entries):
        try:
            source = Source.model_validate(entry)
        except ValueError as exc:
            label = entry.get("id", f"#{index}") if isinstance(entry, dict) else f"#{index}"
            raise RegistryError(f"invalid registry entry {label}: {exc}") from exc
        if source.id in sources:
            raise RegistryError(f"duplicate registry id {source.id}")
        sources[source.id] = source
    return sources


def load_registry(path: Path) -> dict[str, Source]:
    return parse_registry(yaml.safe_load(path.read_text(encoding="utf-8")))
