import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import obstore
from obstore.exceptions import AlreadyExistsError, NotFoundError
from obstore.store import LocalStore, MemoryStore, ObjectStore, S3Store


class VintageExistsError(RuntimeError):
    pass


# Expected shapes, checked before any request so a value pasted into the wrong variable
# fails here, without ever being echoed into a log (the account id ends up in every URL).
_R2_SHAPES = {
    "R2_ACCOUNT_ID": (re.compile(r"^[0-9a-f]{32}$"), "32 hex characters"),
    "R2_ACCESS_KEY_ID": (re.compile(r"^[0-9a-f]{32}$"), "32 hex characters"),
    "R2_SECRET_ACCESS_KEY": (re.compile(r"^[0-9a-f]{64}$"), "64 hex characters"),
    "R2_BUCKET": (re.compile(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$"), "a bucket name"),
}


@dataclass(frozen=True)
class R2Credentials:
    account_id: str = field(repr=False)
    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)
    bucket: str

    @classmethod
    def from_env(cls) -> "R2Credentials":
        missing = [name for name in _R2_SHAPES if not os.environ.get(name, "").strip()]
        if missing:
            raise RuntimeError(f"missing environment variables: {', '.join(missing)}")
        values = {name: os.environ[name].strip() for name in _R2_SHAPES}
        wrong = [
            f"{name} should be {expected} (got {len(values[name])} characters)"
            for name, (pattern, expected) in _R2_SHAPES.items()
            if not pattern.match(values[name])
        ]
        if wrong:
            raise RuntimeError("; ".join(wrong))
        return cls(*values.values())

    @property
    def endpoint(self) -> str:
        return f"https://{self.account_id}.r2.cloudflarestorage.com"


def _sql_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


class Storage:
    """Object storage for raw/, clean/ and catalog/, plus DuckDB access to the same keys."""

    def __init__(
        self,
        store: ObjectStore,
        duckdb_root: str,
        r2: R2Credentials | None = None,
    ) -> None:
        self._store = store
        self._duckdb_root = duckdb_root.rstrip("/")
        self._r2 = r2

    @classmethod
    def r2(cls, credentials: R2Credentials) -> "Storage":
        store = S3Store(
            credentials.bucket,
            endpoint=credentials.endpoint,
            region="auto",
            access_key_id=credentials.access_key_id,
            secret_access_key=credentials.secret_access_key,
        )
        return cls(store, f"r2://{credentials.bucket}", credentials)

    @classmethod
    def local(cls, root: Path) -> "Storage":
        root = root.resolve()
        root.mkdir(parents=True, exist_ok=True)
        return cls(LocalStore(root), str(root))

    @classmethod
    def memory(cls) -> "Storage":
        return cls(MemoryStore(), "memory://")

    async def put_new(self, key: str, data: bytes) -> None:
        try:
            await obstore.put_async(self._store, key, data, mode="create")
        except AlreadyExistsError as exc:
            raise VintageExistsError(
                f"{key} already exists; vintages are never overwritten"
            ) from exc

    async def put(self, key: str, data: bytes) -> None:
        await obstore.put_async(self._store, key, data)

    async def get(self, key: str) -> bytes | None:
        try:
            result = await obstore.get_async(self._store, key)
        except (NotFoundError, FileNotFoundError):
            return None
        return bytes(await result.bytes_async())

    async def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        async for chunk in obstore.list(self._store, prefix):
            keys.extend(meta["path"] for meta in chunk)
        return sorted(keys)

    def duckdb_path(self, key: str) -> str:
        return f"{self._duckdb_root}/{key}"

    def duckdb(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect()
        if self._r2 is not None:
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute(
                "CREATE SECRET r2 (TYPE r2, "
                f"KEY_ID {_sql_literal(self._r2.access_key_id)}, "
                f"SECRET {_sql_literal(self._r2.secret_access_key)}, "
                f"ACCOUNT_ID {_sql_literal(self._r2.account_id)})"
            )
        return con
