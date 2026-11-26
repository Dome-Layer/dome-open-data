"""The insights stage on real data: the three insights on the signed-off canvas, reproduced.

`tests/fixtures/store/` is a small cut of the real bucket as it stood when the canvas was made:
the ECB reference rates of vintage 2026-11-24 (every currency still quoted, from 2023, plus the
last five fixes of each one the ECB no longer quotes), the World Bank Pink Sheet of vintage
2026-10-21 (from 1990) and the ECB key rates. Both publishers allow reuse with attribution
(ECB statistics reuse policy; World Bank, CC BY 4.0), which is why their bytes may sit in a
public repository. The model is never called here: every draft is written by the test.
"""

import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from dome_open_data import dashboard, licence
from dome_open_data.insights import check as checker
from dome_open_data.insights import config as insights_config
from dome_open_data.insights import scan, write
from dome_open_data.insights.data import Reader
from dome_open_data.insights.figures import Figure
from dome_open_data.registry import Source, load_registry
from dome_open_data.storage import Storage
from tests.test_runner import sync

ROOT = Path(__file__).resolve().parents[1]
STORE = Path(__file__).parent / "fixtures" / "store"
AS_OF = date(2026, 11, 24)
MOMENT = datetime(2026, 11, 24, 18, tzinfo=UTC)
USD = "ecb.fx.USD.EUR.SP00.A.EMU.D"
BRL = "move:ecb_currencies"
EURUSD = f"record:{USD}"
ENERGY = "lead_lag:energy_then_ecb"

# The canvas text, word for word except where the canvas was wrong (see the 44 test below).
CANVAS = {
    BRL: (
        "Outlier",
        "The Brazilian real is the biggest mover of 29 currencies against the euro.",
        "EUR/BRL went from 5.6339 to 6.4349 in five weeks, +14.2%. Most of it came at once: "
        "between the fixes of 23 and 26 October the rate jumped 10.7%, and after a short "
        "pullback the climb resumed.",
    ),
    ENERGY: (
        "Pattern",
        "Energy moved first. The ECB turned from cutting to raising three months later.",
        "Brent went from $71.1 to $103.7 a barrel between February and March 2026. The ECB, "
        "which had held its deposit rate at 2.00% for a year, raised it in June and again in "
        "September.",
    ),
    EURUSD: (
        "Milestone",
        "The euro fixed lower against the dollar than on any day since April 2025.",
        "1.0939 on 23 November 2026, down from 1.1267 on 4 November. No reference fix was that "
        "low since 2 April 2025.",
    ),
}


@pytest.fixture
def registry() -> dict[str, Source]:
    return load_registry(ROOT / "sources.yaml")


@pytest.fixture
def config(registry: dict[str, Source]) -> insights_config.InsightsConfig:
    return insights_config.load(ROOT / "insights.yaml", registry)


@pytest.fixture
def storage(tmp_path: Path) -> Storage:
    shutil.copytree(STORE, tmp_path / "bucket")
    return Storage.local(tmp_path / "bucket")


def candidates(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> dict[str, scan.Candidate]:
    found = sync(scan.scan(Reader(storage, registry), config, AS_OF))
    return {c.id: c for c in found}


def values(candidate: scan.Candidate) -> dict[str, float | str]:
    return {f.id: f.value for f in candidate.figures}


def draft(candidate_id: str, headline: str | None = None, body: str | None = None) -> checker.Draft:
    tag, canvas_headline, canvas_body = CANVAS[candidate_id]
    return checker.Draft(candidate_id, tag, headline or canvas_headline, body or canvas_body, [])


def verdict(
    storage: Storage, registry: dict[str, Source], candidate: scan.Candidate, d: checker.Draft
) -> checker.Verdict:
    return sync(checker.check(candidate, d, Reader(storage, registry)))


# --- Scan ----------------------------------------------------------------------------------


def test_the_scan_finds_the_three_canvas_findings_with_their_figures(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    found = candidates(storage, registry, config)

    brl = values(found[BRL])
    assert found[BRL].series == {"ecb.fx.BRL.EUR.SP00.A.EMU.D"}
    assert (brl["start_value"], brl["start_date"]) == (5.6339, "2026-10-20")
    assert (brl["end_value"], brl["end_date"]) == (6.4349, "2026-11-24")
    assert round(float(brl["change_pct"]), 1) == 14.2
    assert (brl["step_from_date"], brl["step_to_date"]) == ("2026-10-23", "2026-10-26")
    assert round(float(brl["step_pct"]), 1) == 10.7
    assert brl["weeks"] == 5
    assert brl["rank"] == 1

    usd = values(found[EURUSD])
    assert (usd["value"], usd["date"], usd["since_date"]) == (1.0939, "2026-11-23", "2025-04-02")
    assert (usd["recent_other_value"], usd["recent_other_date"]) == (1.1267, "2026-11-04")

    energy = values(found[ENERGY])
    assert (energy["crude_oil_brent_from_value"], energy["crude_oil_brent_to_value"]) == (
        71.1,
        103.7,
    )
    assert energy["crude_oil_brent_to_date"] == "2026-03-01"
    assert (energy["held_value"], energy["held_since"]) == (2.0, "2025-06-11")
    assert (energy["change_1_value"], energy["change_1_date"]) == (2.25, "2026-06-17")
    assert (energy["change_2_value"], energy["change_2_date"]) == (2.5, "2026-09-16")
    assert energy["crude_oil_brent_lag_months"] == 3
    assert found[ENERGY].caveat == scan.PATTERN_CAVEAT


def test_only_currencies_still_quoted_are_compared(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    """The store holds 44 ECB currencies, but 15 are no longer fixed (the lira, the rouble since
    March 2022, the lev since 2026), so the move is the largest of 29, not of 44."""
    snapshot = sync(Reader(storage, registry).snapshot("ecb_fx"))
    assert len(snapshot.series) == 44
    assert values(candidates(storage, registry, config)[BRL])["compared"] == 29


def test_the_shortlist_holds_all_three_canvas_findings(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    found = list(candidates(storage, registry, config).values())
    chosen = scan.shortlist(
        found, config.shortlist.size, config.shortlist.per_detector, AS_OF, config.prominent
    )
    ids = [c.id for c in chosen]
    assert {BRL, EURUSD, ENERGY} <= set(ids)
    assert len(ids) <= config.shortlist.size
    # One record per family: the euro's low against the dollar stands for the others.
    assert sum(1 for c in chosen if c.group == "record:ecb_currencies") == 1
    assert len({s for c in chosen for s in c.series}) == sum(len(c.series) for c in chosen)


# --- Check ---------------------------------------------------------------------------------


@pytest.mark.parametrize(("candidate_id", "total"), [(BRL, 8), (ENERGY, 8), (EURUSD, 6)])
def test_the_checker_reproduces_each_canvas_insight(
    storage: Storage,
    registry: dict[str, Source],
    config: insights_config.InsightsConfig,
    candidate_id: str,
    total: int,
) -> None:
    candidate = candidates(storage, registry, config)[candidate_id]
    result = verdict(storage, registry, candidate, draft(candidate_id))
    assert result.problems == []
    assert result.passed
    assert result.label == f"figures checked: {total} of {total}"


def test_the_canvas_headline_with_44_currencies_is_dropped(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    """The signed-off canvas said "of 44 currencies". The checker refuses it: 44 is what the store
    holds, not what moved, and no figure of the candidate says 44."""
    candidate = candidates(storage, registry, config)[BRL]
    headline = "The Brazilian real is the biggest mover of 44 currencies against the euro."
    result = verdict(storage, registry, candidate, draft(BRL, headline=headline))
    assert not result.passed
    assert result.problems == ["'44' matches no figure of the candidate"]
    assert result.label == "figures checked: 7 of 8"


def test_a_deliberately_wrong_figure_drops_the_insight(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    candidate = candidates(storage, registry, config)[BRL]
    body = CANVAS[BRL][2].replace("+14.2%", "+14.5%")
    result = verdict(storage, registry, candidate, draft(BRL, body=body))
    assert not result.passed
    assert "'+14.5%' matches no figure of the candidate" in result.problems


@pytest.mark.parametrize(
    ("right", "wrong"),
    [
        ("23 and 26 October", "23 and 27 October"),  # a date that is not a figure
        ("in five weeks", "in six weeks"),  # a number word
        ("+14.2%", "-14.2%"),  # the right size, the wrong sign
        ("in five weeks", "in five weeks of 2025"),  # a year no figure has
        ("6.4349", "6.4394"),  # transposed digits
    ],
)
def test_every_kind_of_wrong_mention_is_caught(
    storage: Storage,
    registry: dict[str, Source],
    config: insights_config.InsightsConfig,
    right: str,
    wrong: str,
) -> None:
    candidate = candidates(storage, registry, config)[BRL]
    body = CANVAS[BRL][2].replace(right, wrong)
    assert body != CANVAS[BRL][2]
    result = verdict(storage, registry, candidate, draft(BRL, body=body))
    assert not result.passed
    assert len(result.problems) == 1


def test_a_claimed_cause_drops_the_insight(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    candidate = candidates(storage, registry, config)[ENERGY]
    headline = "The ECB raised rates three months later because energy moved first."
    result = verdict(storage, registry, candidate, draft(ENERGY, headline=headline))
    assert result.problems == ["claims a cause: 'because'"]


def test_an_em_dash_drops_the_insight(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    candidate = candidates(storage, registry, config)[EURUSD]
    body = CANVAS[EURUSD][2].replace(", down from", " \u2014 down from")
    assert (
        "uses an em or en dash (house style)"
        in verdict(storage, registry, candidate, draft(EURUSD, body=body)).problems
    )


def test_a_figure_that_does_not_recompute_drops_the_insight(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    """A detector bug: the scan claims a value the store does not hold."""
    candidate = candidates(storage, registry, config)[EURUSD]
    candidate.figures = [
        Figure(f.id, f.meaning, 1.0839, f.display, f.calc) if f.id == "value" else f
        for f in candidate.figures
    ]
    result = verdict(storage, registry, candidate, draft(EURUSD))
    assert "value: scanned 1.0839, recomputed 1.0939" in result.problems


def test_a_store_that_changed_after_the_scan_drops_the_insight(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    candidate = candidates(storage, registry, config)[EURUSD]
    key = candidate.reads[0].key
    data = sync(storage.get(key))
    assert data is not None
    sync(storage.put(key, data + b"tampered"))
    result = verdict(storage, registry, candidate, draft(EURUSD))
    assert f"{key} no longer hashes as it did when scanned" in result.problems


def test_mentions_reads_dates_before_numbers() -> None:
    found = checker.mentions(
        "From 5.6339 on 20 October to 6.4349 on 24 November 2026, +14.2%, in Q3 2026, "
        "between 23 and 26 October, three months, $1,460.23, in March 2026 and June, since 2025."
    )
    assert [(m.kind, m.text) for m in found] == [
        ("day", "23 October"),
        ("day", "26 October"),
        ("day", "20 October"),
        ("day", "24 November 2026"),
        ("month", "March 2026"),
        ("quarter", "Q3 2026"),
        ("month", "June"),
        ("year", "2025"),
        ("number", "5.6339"),
        ("number", "6.4349"),
        ("number", "+14.2%"),
        ("number", "$1,460.23"),
        ("number", "three"),
    ]
    plus = next(m for m in found if m.text == "+14.2%")
    assert (plus.number, plus.decimals, plus.signed, plus.percent) == (14.2, 1, True, True)


def test_a_text_with_no_figure_at_all_is_not_published(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    candidate = candidates(storage, registry, config)[EURUSD]
    d = draft(EURUSD, headline="The euro is weak.", body="It fixed low against the dollar.")
    result = verdict(storage, registry, candidate, d)
    assert result.problems == [] and not result.passed


# --- Licence -------------------------------------------------------------------------------


def test_an_ask_source_never_reaches_a_detector(
    storage: Storage, registry: dict[str, Source]
) -> None:
    with pytest.raises(licence.LicenceError):
        sync(Reader(storage, registry).snapshot("imf_weo"))


def test_the_config_may_not_name_an_ask_source(registry: dict[str, Source], tmp_path: Path) -> None:
    text = (
        (ROOT / "insights.yaml")
        .read_text()
        .replace("source_id: worldbank_pinksheet\n    label", "source_id: imf_weo\n    label")
    )
    (tmp_path / "insights.yaml").write_text(text)
    with pytest.raises(licence.LicenceError):
        insights_config.load(tmp_path / "insights.yaml", registry)


# --- Writer --------------------------------------------------------------------------------


def test_the_writer_sees_only_the_shortlist_and_the_configured_model(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    chosen = [candidates(storage, registry, config)[BRL]]
    body = write.request(chosen, config.writer)
    assert body["model"] == config.writer.model == "claude-opus-5-5"
    assert body["output_config"]["format"]["type"] == "json_schema"
    assert body["output_config"]["effort"] == "medium"
    assert body["fallbacks"] == "default" and body["betas"] == [write.FALLBACK_BETA]
    sent = json.loads(body["messages"][0]["content"].split("\n", 1)[1])
    assert sent == [chosen[0].for_model()]
    # No series, no store keys, nothing to add up: only meanings and display strings.
    assert set(sent[0]) == {"id", "tag", "subject", "finding", "figures"}
    assert all(set(f) == {"id", "meaning", "display"} for f in sent[0]["figures"])


def test_an_answer_in_another_shape_is_an_error() -> None:
    with pytest.raises(write.WriterError):
        write.parse('{"cards": []}')


# --- The dashboard -------------------------------------------------------------------------


class Model:
    """Stands in for Claude: answers with the canvas text for every candidate it knows."""

    def __init__(self, override: dict[str, tuple[str, str]] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.override = override or {}

    async def __call__(self, body: dict[str, Any]) -> tuple[str, str, int, int]:
        self.calls.append(body)
        sent = json.loads(body["messages"][0]["content"].split("\n", 1)[1])
        out = []
        for item in sent:
            if item["id"] in CANVAS:
                tag, headline, text = CANVAS[item["id"]]
                headline, text = self.override.get(item["id"], (headline, text))
                out.append(
                    {
                        "candidate_id": item["id"],
                        "tag": tag,
                        "headline": headline,
                        "body": text,
                        "figures_used": [f["id"] for f in item["figures"]][:3],
                    }
                )
        return json.dumps({"insights": out}), "claude-opus-5-5", 1200, 400


def build(storage: Storage, registry: dict[str, Source], config: Any, model: Any) -> Any:
    return sync(dashboard.build(storage, registry, config, MOMENT, call=model))


def test_the_dashboard_publishes_the_three_canvas_insights_through_the_gate(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    model = Model()
    built, outcome = build(storage, registry, config, model)
    assert len(model.calls) == 1
    published = {item["id"]: item for item in built["insights"]}
    assert set(published) == {BRL, EURUSD, ENERGY}
    for item in published.values():
        assert item["figures_checked"] == f"figures checked: {item['total']} of {item['total']}"
        assert item["written_by"] == "claude-opus-5-5"
    assert published[ENERGY]["caveat"] == scan.PATTERN_CAVEAT
    # Everything else on the shortlist had no text, so it is listed as dropped, without text.
    assert built["generated"]["dropped"] == outcome.dropped == len(built["dropped"])
    assert all(set(d) == {"id", "tag", "problems", "figures_checked"} for d in built["dropped"])
    assert [k["label"] for k in built["key_figures"]] == [k.label for k in config.key_figures]

    written = sync(dashboard.publish(storage, built, registry))
    assert written > 0
    stored = json.loads(sync(storage.get(dashboard.DASHBOARD_KEY)) or b"{}")
    assert licence.check_public(stored, registry) > 0
    assert {s["source_id"] for s in stored["attribution"]} >= {"ecb_fx", "worldbank_pinksheet"}

    key = sync(dashboard.write_events(storage, outcome, MOMENT))
    assert key is not None and key.startswith(dashboard.GOVERNANCE_PREFIX)
    events = json.loads(sync(storage.get(key)) or b"[]")
    assert {e["action_type"] for e in events} == {"insight_published", "insight_dropped"}
    assert all(e["agent_id"] == "open-data" for e in events)
    assert sum(e["action_type"] == "insight_published" for e in events) == 3


def test_a_wrong_figure_from_the_model_is_dropped_and_the_rest_publish(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    wrong = (CANVAS[EURUSD][1], CANVAS[EURUSD][2].replace("1.0939", "1.0893"))
    built, outcome = build(storage, registry, config, Model({EURUSD: wrong}))
    assert {item["id"] for item in built["insights"]} == {BRL, ENERGY}
    dropped = {d["id"]: d for d in built["dropped"]}
    assert dropped[EURUSD]["figures_checked"] == "figures checked: 5 of 6"
    event = next(e for e in outcome.events if e["metadata"]["candidate_id"] == EURUSD)
    assert event["action_type"] == "insight_dropped"
    assert "'1.0893' matches no figure of the candidate" in event["rules_triggered"]


def test_unchanged_findings_keep_their_checked_text_without_a_model_call(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    first, _ = build(storage, registry, config, Model())
    sync(dashboard.publish(storage, first, registry))

    model = Model()
    again, outcome = build(storage, registry, config, model)
    assert outcome.kept == 3
    # Only the candidates that had no text last time go to the model; the three are kept.
    sent = {
        c["id"]
        for call in model.calls
        for c in json.loads(call["messages"][0]["content"].split("\n", 1)[1])
    }
    assert sent.isdisjoint({BRL, EURUSD, ENERGY})
    assert [i["headline"] for i in again["insights"]] == [i["headline"] for i in first["insights"]]


def test_a_writer_failure_still_publishes_and_says_so(
    storage: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    async def down(body: dict[str, Any]) -> tuple[str, str, int, int]:
        raise write.WriterError("the writer request failed: HTTP 529")

    built, outcome = build(storage, registry, config, down)
    assert built["insights"] == []
    assert built["generated"]["writer_error"] == outcome.writer_error
    assert built["key_figures"]
    assert sync(dashboard.publish(storage, built, registry)) > 0


# --- ALFRED: revisions and the flagship panel ----------------------------------------------


@pytest.fixture
def with_gdp(storage: Storage, registry: dict[str, Source]) -> Storage:
    """The real GDPC1 versions of 2008-04-30, 2009-07-31 and 2026-11-25 (2007 and 2008 rows)."""
    from tests import test_alfred

    source = registry["fred_alfred"].model_copy(
        update={
            "endpoint": "https://alfred.test/",
            "rate_limit": registry["fred_alfred"].rate_limit.model_copy(
                update={"per_second": 1000}
            ),
            "options": {"series": ["GDPC1"]},
        }
    )
    test_alfred.collect(source, storage, test_alfred.FakeAlfred(test_alfred.DATES))
    return storage


def test_the_flagship_panel_tells_the_2008_story(
    with_gdp: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    panel = sync(dashboard.flagship(Reader(with_gdp, registry), config.flagship))
    assert panel["source_id"] == "fred_alfred"
    q1 = panel["values"][0]
    assert q1["quarter"] == "2008-01-01"
    assert [(d, round(g, 2)) for d, g in q1["growth_by_publication"]] == [
        ("2008-04-30", 0.60),
        ("2009-07-31", -0.72),
        ("2026-11-25", -1.70),
    ]


def test_a_revision_between_the_two_newest_versions_is_found_and_checks(
    with_gdp: Storage, registry: dict[str, Source], config: insights_config.InsightsConfig
) -> None:
    found = sync(scan.revisions(Reader(with_gdp, registry), config, date(2026, 11, 26)))
    assert [c.id for c in found] == ["revision:GDPC1"]
    revision = found[0]
    v = values(revision)
    assert (v["older_published"], v["newer_published"]) == ("2009-07-31", "2026-11-25")
    assert round(float(v["now"]) - float(v["was"]), 9) == round(float(v["revision"]), 9)
    text = checker.Draft(
        revision.id,
        "Revision",
        f"US growth in {revision.figure('period').display} looks different today.",  # type: ignore[union-attr]
        f"Published on 31 July 2009 as {revision.figure('was').display}, it reads "  # type: ignore[union-attr]
        f"{revision.figure('now').display} in the version of 25 November 2026.",  # type: ignore[union-attr]
        ["period", "was", "now"],
    )
    result = sync(checker.check(revision, text, Reader(with_gdp, registry)))
    assert result.problems == [] and result.passed


def test_the_run_history_holds_the_last_14_days_failures_included(
    storage: Storage, registry: dict[str, Source]
) -> None:
    from dome_open_data import catalog, ledger

    def row(day: date, status: str) -> catalog.RunRecord:
        at = datetime(day.year, day.month, day.day, 12, tzinfo=UTC)
        return catalog.RunRecord("ecb_fx", at, at, status, day, None, 1, 1, 1, None)

    for day, status in [(date(2026, 11, 1), "collected"), (date(2026, 11, 20), "failed")]:
        sync(catalog.write_run(storage, row(day, status)))
    history = sync(ledger.run_history(storage, [registry["ecb_fx"]], date(2026, 11, 10)))
    assert [(r["source_id"], r["status"]) for r in history] == [("ecb_fx", "failed")]
