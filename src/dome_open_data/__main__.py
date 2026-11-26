import argparse
import asyncio
import json
import logging
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from dome_open_data import alfred, catalog, dashboard, ledger, logs, ops
from dome_open_data.config import load_env_file
from dome_open_data.http import new_client
from dome_open_data.registry import Source, load_registry
from dome_open_data.runner import run_source, verify_source
from dome_open_data.storage import R2Credentials, Storage

log = logging.getLogger("dome_open_data")


def _storage(args: argparse.Namespace) -> Storage:
    if args.local_dir:
        return Storage.local(Path(args.local_dir))
    return Storage.r2(R2Credentials.from_env())


def _source(args: argparse.Namespace) -> Source:
    sources = load_registry(Path(args.registry))
    if args.source not in sources:
        raise SystemExit(f"unknown source {args.source!r}; known: {', '.join(sorted(sources))}")
    return sources[args.source]


async def _run(args: argparse.Namespace) -> None:
    source = _source(args)
    if source.connector == "alfred":
        # Dated by the publisher, so --force means nothing here: a run fetches what is new.
        async with new_client() as client:
            result = await alfred.run_alfred(
                source,
                _storage(args),
                client,
                only=args.series or None,
                max_new=args.max_new,
                recheck=args.recheck,
            )
        print(f"{source.id}: {result.status}, {result.new} new publication dates")
        return
    async with new_client() as client:
        await run_source(
            source,
            _storage(args),
            client,
            vintage=datetime.now(UTC).date(),
            force=args.force,
        )


async def _crosswalk(args: argparse.Namespace) -> None:
    from dome_open_data.crosswalk_build import build

    sources = load_registry(Path(args.registry))
    async with new_client() as client:
        countries, aggregates = await build(client, sources["un_wpp"])
    print(f"crosswalk: {countries} countries, {aggregates} aggregates")


async def _freshness(args: argparse.Namespace) -> None:
    """Weekly: say which sources have stopped refreshing. Exit 2 means at least one has."""
    sources = load_registry(Path(args.registry)).values()
    report = await ops.freshness(_storage(args), sources, datetime.now(UTC).date())
    for row in sorted(report, key=lambda r: (not r.stale, r.source_id)):
        print(row.line)
    stale = [row.source_id for row in report if row.stale]
    if stale:
        print(f"\n{len(stale)} stale: {', '.join(sorted(stale))}")
        raise SystemExit(2)
    print(f"\nall {len(report)} sources are within their expected staleness")


async def _usage(args: argparse.Namespace) -> None:
    """Weekly: how full the bucket is. Exit 2 means it is time to prune."""
    report = await ops.usage(_storage(args))
    print(report.line)
    if report.over:
        raise SystemExit(2)


async def _alert_check(args: argparse.Namespace) -> None:
    """After a failed run: exit 2 if a human should be told, 0 if the schedule will sort it out.

    Anything unexpected here exits 2 as well. A monitor that goes quiet when it cannot read its
    own history is worse than one that cries wolf.
    """
    source = _source(args)
    storage = _storage(args)
    try:
        runs = await catalog.recent_runs(storage, source.id, limit=5)
        statuses = [str(row["status"]) for row in runs]
        report = await ops.freshness(storage, [source], datetime.now(UTC).date())
        decision = ops.should_alert(statuses, source.cadence, report[0].stale)
    except Exception as exc:  # noqa: BLE001
        print(f"alert: could not read the run history ({type(exc).__name__}: {exc})")
        raise SystemExit(2) from None
    print(f"{'alert' if decision.alert else 'quiet'}: {decision.reason}")
    print(f"recent runs, newest first: {', '.join(statuses) or 'none'}")
    if decision.alert:
        raise SystemExit(2)


async def _ledger(args: argparse.Namespace) -> None:
    """Assemble the public ledger and write it to `public/ledger.json`.

    Called at the end of the collect workflows and the ops weekly workflow, so the published page
    is never more than one run behind. It reads the catalog only: no observation value is touched.
    """
    storage = _storage(args)
    registry = load_registry(Path(args.registry))
    built = await ledger.build(storage, list(registry.values()), datetime.now(UTC).date())
    if args.print:
        print(ledger.serialise(built).decode("utf-8"))
        return
    written = await ledger.write(storage, built, registry)
    totals = built["totals"]
    print(
        f"{ledger.LEDGER_KEY}: {written:,} bytes, {totals['sources']} sources, "
        f"{totals['stale']} stale, {totals['vintages']} vintages, "
        f"{totals['series']:,} series, as of {built['as_of']}"
    )


async def _dashboard(args: argparse.Namespace) -> None:
    """Scan, write, check, then publish `public/dashboard.json` (Phase 2A step 3).

    Called after `ledger` at the end of the collect workflows. The dashboard is written even when
    the writer fails, with the insights it could keep; the command then exits non-zero.
    """
    from dome_open_data.insights import config as insights_config

    storage = _storage(args)
    registry = load_registry(Path(args.registry))
    config = insights_config.load(Path(args.config), registry)
    at = datetime.now(UTC)
    built, outcome = await dashboard.build(
        storage, registry, config, at, use_model=not args.no_model
    )
    if args.print:
        print(ledger.serialise(built).decode("utf-8"))
    else:
        written = await dashboard.publish(storage, built, registry)
        events = await dashboard.write_events(storage, outcome, at)
        print(f"{dashboard.DASHBOARD_KEY}: {written:,} bytes; governance events: {events}")
    print(
        f"insights: {outcome.shortlisted} shortlisted, {outcome.kept} kept, "
        f"{outcome.written} written by {outcome.model or 'no model'} "
        f"({outcome.input_tokens} in, {outcome.output_tokens} out), "
        f"{outcome.published} published, {outcome.dropped} dropped"
    )
    for item in built["insights"]:
        print(f"  {item['tag']}: {item['headline']} [{item['figures_checked']}]")
    for item in built["dropped"]:
        print(f"  dropped {item['id']} [{item['figures_checked']}]")
    if outcome.writer_error:
        raise RuntimeError(f"the writer failed: {outcome.writer_error}")


async def _verify(args: argparse.Namespace) -> None:
    source = _source(args)
    if source.connector == "alfred":
        checked = await alfred.verify_alfred(source, _storage(args), args.series or None)
        print(
            f"{source.id}: {checked.series} series, {checked.publication_dates} publication "
            f"dates, {checked.rows:,} rows, every hash and count matches"
        )
        return
    vintage = date.fromisoformat(args.vintage) if args.vintage else None
    await verify_source(source, _storage(args), vintage)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dome_open_data")
    parser.add_argument("--registry", default="sources.yaml")
    parser.add_argument("--env-file", default=".env")
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("list", help="validate the registry and list its sources")

    ids = commands.add_parser("ids", help="registry ids as JSON, for the workflow matrices")
    ids.add_argument("--cadence", choices=["daily", "weekly", "monthly", "yearly"])

    fresh = commands.add_parser("freshness", help="which sources have stopped refreshing")
    fresh.add_argument("--local-dir", help="read a local folder instead of R2")

    usage = commands.add_parser("usage", help="objects and bytes in the bucket")
    usage.add_argument("--local-dir", help="read a local folder instead of R2")

    alert = commands.add_parser(
        "alert-check", help="after a failed run: does it need a human? exit 2 means yes"
    )
    alert.add_argument("source")
    alert.add_argument("--local-dir", help="read a local folder instead of R2")

    run = commands.add_parser("run", help="collect one source")
    run.add_argument("source")
    run.add_argument("--force", action="store_true", help="ignore the change signal")
    run.add_argument("--local-dir", help="write to a local folder instead of R2")
    run.add_argument(
        "--series", action="append", help="ALFRED only: collect just this series (repeatable)"
    )
    run.add_argument(
        "--max-new", type=int, help="ALFRED only: cap on new publication dates this run"
    )
    run.add_argument(
        "--recheck",
        choices=["none", "latest", "all"],
        default="latest",
        help="ALFRED only: which held publication dates to fetch again and hash-check",
    )

    led = commands.add_parser("ledger", help="assemble public/ledger.json for the ledger page")
    led.add_argument("--local-dir", help="read and write a local folder instead of R2")
    led.add_argument(
        "--print", action="store_true", help="print the JSON instead of writing it to the store"
    )

    dash = commands.add_parser(
        "dashboard", help="scan, write and check insights, then publish public/dashboard.json"
    )
    dash.add_argument("--config", default="insights.yaml")
    dash.add_argument("--local-dir", help="read and write a local folder instead of R2")
    dash.add_argument(
        "--print", action="store_true", help="print the JSON instead of writing it to the store"
    )
    dash.add_argument("--no-model", action="store_true", help="publish without calling the writer")

    verify = commands.add_parser("verify", help="recount a vintage from raw and via DuckDB")
    verify.add_argument("source")
    verify.add_argument("--vintage", help="YYYY-MM-DD, default the newest")
    verify.add_argument("--local-dir", help="read a local folder instead of R2")
    verify.add_argument("--series", action="append", help="ALFRED only: just this series")

    commands.add_parser("crosswalk", help="rebuild crosswalk/countries.csv and aggregates.csv")

    args = parser.parse_args(argv)
    logs.configure()
    load_env_file(Path(args.env_file))

    if args.command == "list":
        for source in load_registry(Path(args.registry)).values():
            print(f"{source.id}\t{source.connector}\t{source.cadence}\t{source.licence.name}")
        return 0
    if args.command == "ids":
        sources = load_registry(Path(args.registry)).values()
        selected = [s.id for s in sources if not args.cadence or s.cadence == args.cadence]
        print(json.dumps(sorted(selected)))
        return 0
    commands_run = {
        "run": _run,
        "verify": _verify,
        "crosswalk": _crosswalk,
        "freshness": _freshness,
        "usage": _usage,
        "alert-check": _alert_check,
        "ledger": _ledger,
        "dashboard": _dashboard,
    }
    try:
        asyncio.run(commands_run[args.command](args))
    except Exception:
        log.exception(
            "command failed", extra={"command": args.command, "source": getattr(args, "source", "")}
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
