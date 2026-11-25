"""The licence gate: which sources may put **values** in front of the public.

A source contributes observation values to anything public (the ledger, the dashboard JSON, any
future `public/` object) only if its registry entry allows reuse: `commercial_ok: "yes"`, which is
also how a public-domain source is entered. Every other source (`ask`, `no`) contributes counts
and provenance only. Decided 2026-11-25 (Phase 2A step 2).

How it is enforced, so it does not rest on anyone remembering it:

- Every object under `public/` is written by `publish`, which runs `check_public` first and
  refuses the write if it fails.
- In a public payload, values travel only inside a **value block**: a mapping with a `value` or
  `values` key. `check_public` finds every such mapping however deep it sits and requires that it
  names a registry `source_id` whose licence allows values, and that its `attribution` is that
  source's registry string, character for character. A value block with no source, an unknown
  source, a refused source or a retyped attribution fails the write.
- `value_block` builds a block with the attribution taken from the registry, so the app never
  types one.
"""

import json
from collections.abc import Iterator, Mapping
from typing import Any

from dome_open_data.registry import Source
from dome_open_data.storage import Storage

PUBLIC_PREFIX = "public/"
VALUE_KEYS = ("value", "values")


class LicenceError(RuntimeError):
    pass


def values_allowed(source: Source) -> bool:
    """True only where the licence allows republishing the data itself."""
    return source.licence.commercial_ok == "yes"


def value_block(source: Source, values: Any, **fields: Any) -> dict[str, Any]:
    """A value block for a public payload, refused at once for a source that may not publish."""
    if not values_allowed(source):
        raise LicenceError(
            f"{source.id} is {source.licence.commercial_ok!r} under {source.licence.name}: "
            "it may contribute counts to a public object, never values"
        )
    return {
        **fields,
        "source_id": source.id,
        "licence": source.licence.name,
        "attribution": source.licence.attribution,
        "values": values,
    }


def _value_blocks(node: Any, path: str = "$") -> Iterator[tuple[str, Mapping[str, Any]]]:
    if isinstance(node, Mapping):
        if any(key in node for key in VALUE_KEYS):
            yield path, node
        for key, child in node.items():
            yield from _value_blocks(child, f"{path}.{key}")
    elif isinstance(node, list | tuple):
        for index, child in enumerate(node):
            yield from _value_blocks(child, f"{path}[{index}]")


def check_public(payload: Any, sources: Mapping[str, Source]) -> int:
    """Refuse a payload in which any value could reach the public without a licence for it.

    Returns how many value blocks it checked.
    """
    checked = 0
    for path, block in _value_blocks(payload):
        source_id = block.get("source_id")
        if not isinstance(source_id, str) or not source_id:
            raise LicenceError(f"{path}: a value with no source_id cannot be published")
        source = sources.get(source_id)
        if source is None:
            raise LicenceError(f"{path}: {source_id!r} is not in the registry")
        if not values_allowed(source):
            raise LicenceError(
                f"{path}: {source_id} is {source.licence.commercial_ok!r} under "
                f"{source.licence.name}, so its values may not be published"
            )
        if block.get("attribution") != source.licence.attribution:
            raise LicenceError(
                f"{path}: the attribution must be the registry's for {source_id}, "
                f"{source.licence.attribution!r}"
            )
        checked += 1
    return checked


def serialise(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=1).encode("utf-8")


async def publish(storage: Storage, key: str, payload: Any, sources: Mapping[str, Source]) -> int:
    """The only way an object reaches `public/`: checked first, then overwritten in place.

    A public object is a snapshot, not a vintage, so it is not create-only.
    """
    if not key.startswith(PUBLIC_PREFIX):
        raise LicenceError(f"{key} is not under {PUBLIC_PREFIX}")
    check_public(payload, sources)
    body = serialise(payload)
    await storage.put(key, body)
    return len(body)
