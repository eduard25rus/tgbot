#!/usr/bin/env python3
"""Create and verify a consistent SQLite backup with a JSON manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def table_counts(connection: sqlite3.Connection) -> dict[str, int]:
    names = [
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    return {name: int(connection.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]) for name in names}


def quick_check(connection: sqlite3.Connection) -> None:
    result = connection.execute("PRAGMA quick_check").fetchone()
    if not result or result[0] != "ok":
        raise RuntimeError(f"SQLite quick_check failed: {result[0] if result else 'no result'}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--target-dir", required=True, type=Path)
    parser.add_argument("--kind", default="manual")
    args = parser.parse_args()

    source = args.source.resolve()
    if not source.is_file() or source.stat().st_size < 4096:
        raise SystemExit(f"Refusing to back up missing or empty database: {source}")
    args.target_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = args.target_dir / f"contracts-{args.kind}-{stamp}.db"

    with sqlite3.connect(f"file:{source}?mode=ro", uri=True) as src:
        quick_check(src)
        source_counts = table_counts(src)
        with sqlite3.connect(target) as dst:
            src.backup(dst)

    with sqlite3.connect(f"file:{target}?mode=ro", uri=True) as copied:
        quick_check(copied)
        copied_counts = table_counts(copied)
    if copied_counts != source_counts:
        target.unlink(missing_ok=True)
        raise SystemExit("Backup table counts do not match source")

    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    manifest = {
        "kind": args.kind,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": str(source),
        "backup": str(target),
        "size_bytes": target.stat().st_size,
        "sha256": digest,
        "quick_check": "ok",
        "table_counts": copied_counts,
    }
    target.with_suffix(".manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({k: manifest[k] for k in ("backup", "size_bytes", "sha256", "quick_check")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
