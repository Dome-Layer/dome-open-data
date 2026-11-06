import argparse
import asyncio
import json
import logging
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from dome_open_data import logs, ops
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
    async with new_client() as client:
        await run_source(
            _source(args),
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


async def _verify(args: argparse.Namespace) -> None:
    vintage = date.fromisoformat(args.vintage) if args.vintage else None
    await verify_source(_source(args), _storage(args), vintage)


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

    run = commands.add_parser("run", help="collect one source")
    run.add_argument("source")
    run.add_argument("--force", action="store_true", help="ignore the change signal")
    run.add_argument("--local-dir", help="write to a local folder instead of R2")

    verify = commands.add_parser("verify", help="recount a vintage from raw and via DuckDB")
    verify.add_argument("source")
    verify.add_argument("--vintage", help="YYYY-MM-DD, default the newest")
    verify.add_argument("--local-dir", help="read a local folder instead of R2")

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
