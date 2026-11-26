"""`insights.yaml`, validated. Every source it names must be in the registry and allow values."""

from collections.abc import Mapping
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

from dome_open_data import licence
from dome_open_data.registry import Source


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Writer(_Strict):
    model: str = Field(min_length=1)
    effort: Literal["low", "medium", "high", "xhigh", "max"] = "medium"
    max_tokens: int = Field(default=16000, gt=0)
    fallbacks: Literal["default", "none"] = "default"


class Shortlist(_Strict):
    size: int = Field(default=6, gt=0)
    per_detector: int = Field(default=2, gt=0)


class Family(_Strict):
    id: str
    source_id: str
    label: str
    window_days: int = Field(gt=0)


class Records(_Strict):
    min_days: int = Field(gt=0)
    # How many of the newest observations may hold the record, by frequency (D, M, Q).
    recent_observations: dict[str, int]


class Jumps(_Strict):
    ratio: float = Field(gt=1)
    history_days: int = Field(gt=0)
    recent_observations: dict[str, int]


class RevisionSeries(_Strict):
    code: str
    measure: Literal["level", "change", "monthly_growth", "annualised_growth"]
    threshold: float = Field(gt=0)
    label: str


class Revisions(_Strict):
    source_id: str
    recent_periods: int = Field(gt=0)
    series: list[RevisionSeries]


class SeriesRef(_Strict):
    source_id: str
    series_id: str


class LeadLag(_Strict):
    id: str
    leaders: list[SeriesRef] = Field(min_length=1)
    follower: SeriesRef
    min_move_pct: float = Field(gt=0)
    min_hold_days: int = Field(gt=0)
    max_lag_months: int = Field(gt=0)


class KeyFigure(_Strict):
    label: str
    source_id: str
    series_id: str
    points: int = Field(gt=1)
    decimals: int = Field(ge=0)
    step: Literal["observed", "monthly"] = "observed"


class Flagship(_Strict):
    source_id: str
    code: str
    quarters: list[str]


class InsightsConfig(_Strict):
    writer: Writer
    shortlist: Shortlist = Shortlist()
    families: list[Family] = []
    records: Records
    jumps: Jumps
    revisions: Revisions
    lead_lag: list[LeadLag] = []
    key_figures: list[KeyFigure] = []
    flagship: Flagship
    # Series a general reader knows by name, most known first. A record in one counts double, and
    # a tie goes to the one listed first.
    prominent: tuple[str, ...] = ()

    def source_ids(self) -> set[str]:
        ids = {f.source_id for f in self.families}
        ids.add(self.revisions.source_id)
        ids.add(self.flagship.source_id)
        for pair in self.lead_lag:
            ids.update(leader.source_id for leader in pair.leaders)
            ids.add(pair.follower.source_id)
        ids.update(k.source_id for k in self.key_figures)
        return ids

    def check_sources(self, sources: Mapping[str, Source]) -> None:
        for source_id in sorted(self.source_ids()):
            if source_id not in sources:
                raise ValueError(f"insights.yaml names {source_id}, which is not in the registry")
            if not licence.values_allowed(sources[source_id]):
                raise licence.LicenceError(
                    f"insights.yaml names {source_id}, whose licence does not allow values"
                )


def load(path: Path, sources: Mapping[str, Source]) -> InsightsConfig:
    config = InsightsConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
    config.check_sources(sources)
    return config
