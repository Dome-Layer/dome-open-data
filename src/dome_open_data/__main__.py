import argparse
import asyncio
import logging
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from dome_open_data import logs
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


async def _verify(args: argparse.Namespace) -> None:
    vintage = date.fromisoformat(args.vintage) if args.vintage else None
    await verify_source(_source(args), _storage(args), vintage)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dome_open_data")
    parser.add_argument("--registry", default="sources.yaml")
    parser.add_argument("--env-file", default=".env")
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("list", help="validate the registry and list its sources")

    run = commands.add_parser("run", help="collect one source")
    run.add_argument("source")
    run.add_argument("--force", action="store_true", help="ignore the change signal")
    run.add_argument("--local-dir", help="write to a local folder instead of R2")

    verify = commands.add_parser("verify", help="recount a vintage from raw and via DuckDB")
    verify.add_argument("source")
    verify.add_argument("--vintage", help="YYYY-MM-DD, default the newest")
    verify.add_argument("--local-dir", help="read a local folder instead of R2")

    args = parser.parse_args(argv)
    logs.configure()
    load_env_file(Path(args.env_file))

    if args.command == "list":
        for source in load_registry(Path(args.registry)).values():
            print(f"{source.id}\t{source.connector}\t{source.cadence}\t{source.licence.name}")
        return 0
    try:
        asyncio.run(_run(args) if args.command == "run" else _verify(args))
    except Exception:
        log.exception("command failed", extra={"command": args.command, "source": args.source})
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
