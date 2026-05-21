#!/usr/bin/env python3
"""Create a consistent SQLite backup and upload it to configured file storage."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sqlite3
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from file_storage import checksum_sha256, create_file_storage


def env_truthy(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"", "0", "false", "no", "off"}


def resolve_db_path() -> Path:
    return Path(os.getenv("DB_PATH", "contracts.db")).expanduser().resolve()


def resolve_storage_prefix() -> str:
    return os.getenv("DB_BACKUP_STORAGE_PREFIX", "backups/db").strip().strip("/") or "backups/db"


def now_local() -> datetime:
    tz_name = os.getenv("BOT_TIMEZONE", "Asia/Vladivostok").strip() or "Asia/Vladivostok"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("Asia/Vladivostok")
    return datetime.now(tz)


def create_sqlite_backup(db_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    source = sqlite3.connect(str(db_path))
    try:
        destination = sqlite3.connect(str(target_path))
        try:
            source.backup(destination)
        finally:
            destination.close()
    finally:
        source.close()


def gzip_file(source_path: Path, target_path: Path) -> None:
    with source_path.open("rb") as source, gzip.open(target_path, "wb", compresslevel=6, mtime=0) as target:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            target.write(chunk)


def backup_sqlite_to_storage(kind: str = "hourly") -> dict:
    load_dotenv()
    db_path = resolve_db_path()
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite DB not found: {db_path}")
    timestamp = now_local()
    stamp = timestamp.strftime("%Y%m%d-%H%M%S")
    date_path = timestamp.strftime("%Y/%m/%d")
    backup_prefix = resolve_storage_prefix()
    object_prefix = f"{backup_prefix}/{kind}/{date_path}"
    base_name = f"{db_path.stem}-{stamp}.sqlite"
    with tempfile.TemporaryDirectory(prefix="crm-db-backup-") as temp_dir:
        temp_root = Path(temp_dir)
        sqlite_copy = temp_root / base_name
        gzip_copy = temp_root / f"{base_name}.gz"
        create_sqlite_backup(db_path, sqlite_copy)
        gzip_file(sqlite_copy, gzip_copy)
        payload = gzip_copy.read_bytes()
        sha256 = checksum_sha256(payload)
        storage = create_file_storage(db_path.parent / "uploads")
        backup_key = f"{object_prefix}/{gzip_copy.name}"
        manifest = {
            "kind": kind,
            "created_at": timestamp.isoformat(),
            "db_path": str(db_path),
            "backup_key": backup_key,
            "sqlite_size_bytes": sqlite_copy.stat().st_size,
            "gzip_size_bytes": len(payload),
            "sha256": sha256,
        }
        manifest_bytes = (json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
        manifest_key = f"{object_prefix}/{base_name}.manifest.json"
        storage.save_bytes(backup_key, payload, original_filename=gzip_copy.name, content_type="application/gzip")
        storage.save_bytes(
            manifest_key,
            manifest_bytes,
            original_filename=Path(manifest_key).name,
            content_type="application/json",
        )
        return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Back up CRM SQLite DB to configured file storage.")
    parser.add_argument("--kind", default="hourly", choices=("hourly", "daily", "manual"))
    parser.add_argument("--force", action="store_true", help="Run even when DB_BACKUP_ENABLED is false.")
    args = parser.parse_args()
    load_dotenv()
    if not args.force and not env_truthy("DB_BACKUP_ENABLED", False):
        print("DB backup is disabled")
        return 0
    manifest = backup_sqlite_to_storage(args.kind)
    print(
        "backup uploaded "
        f"kind={manifest['kind']} key={manifest['backup_key']} "
        f"bytes={manifest['gzip_size_bytes']} sha256={manifest['sha256']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
