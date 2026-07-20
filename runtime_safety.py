"""Fail-closed checks for persistent production storage."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path


class StorageSafetyError(RuntimeError):
    """Raised when persistent storage is missing, empty, or corrupt."""


def env_truthy(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"", "0", "false", "no", "off"}


def resolve_db_path() -> Path:
    return Path(os.getenv("DB_PATH", "contracts.db")).expanduser().resolve()


def validate_existing_sqlite(db_path: Path | None = None) -> dict[str, object]:
    """Validate an existing SQLite file without creating or migrating it."""
    path = (db_path or resolve_db_path()).resolve()
    minimum_size = int(os.getenv("PRODUCTION_DB_MIN_BYTES", "4096"))
    if not path.is_file():
        raise StorageSafetyError(f"Required SQLite database is missing: {path}")
    size = path.stat().st_size
    if size < minimum_size:
        raise StorageSafetyError(
            f"Required SQLite database is unexpectedly small: {path} ({size} bytes)"
        )

    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10) as connection:
            check = connection.execute("PRAGMA quick_check").fetchone()
            result = str(check[0] if check else "")
            if result != "ok":
                raise StorageSafetyError(f"SQLite quick_check failed for {path}: {result}")
            table_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM sqlite_master "
                    "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                ).fetchone()[0]
            )
            required_table = os.getenv("PRODUCTION_DB_REQUIRED_TABLE", "web_users").strip()
            if required_table:
                exists = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (required_table,),
                ).fetchone()
                if exists is None:
                    raise StorageSafetyError(
                        f"Required table {required_table!r} is missing in {path}"
                    )
    except sqlite3.Error as exc:
        raise StorageSafetyError(f"Cannot validate SQLite database {path}: {exc}") from exc
    return {"path": str(path), "size_bytes": size, "quick_check": "ok", "tables": table_count}


def validate_runtime_storage() -> dict[str, object]:
    """Apply fail-closed production checks when explicitly enabled."""
    if not env_truthy("REQUIRE_EXISTING_DB", False):
        return {"required": False, "path": str(resolve_db_path())}
    report = validate_existing_sqlite()
    report["required"] = True

    if env_truthy("REQUIRE_EXISTING_UPLOAD_ROOT", False):
        upload_root = Path(
            os.getenv("UPLOAD_DIR")
            or os.getenv("FILE_STORAGE_LOCAL_ROOT")
            or resolve_db_path().parent / "uploads"
        ).expanduser().resolve()
        if not upload_root.is_dir():
            raise StorageSafetyError(f"Required upload directory is missing: {upload_root}")
        report["upload_root"] = str(upload_root)
    return report
