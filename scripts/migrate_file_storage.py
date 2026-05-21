from __future__ import annotations

import argparse
import mimetypes
import os
import sqlite3
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from file_storage import checksum_sha256, configured_storage_provider, create_file_storage
from storage import Storage


MEDIA_MIME = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".mp4": "video/mp4",
    ".mov": "video/quicktime",
    ".m4v": "video/x-m4v",
    ".webm": "video/webm",
}


def upload_root_for_db(db_path: Path) -> Path:
    explicit = os.getenv("UPLOAD_DIR", "").strip() or os.getenv("FILE_STORAGE_LOCAL_ROOT", "").strip()
    if explicit:
        root = Path(explicit).expanduser()
        if not root.is_absolute():
            root = db_path.parent / root
    else:
        root = db_path.parent / "uploads"
    return root.resolve()


def content_type_for_name(filename: str) -> str:
    suffix = Path(filename).suffix.lower()
    return MEDIA_MIME.get(suffix) or mimetypes.guess_type(filename)[0] or "application/octet-stream"


def iter_media_rows(conn: sqlite3.Connection):
    queries = [
        (
            "construction_report_photos",
            """
            SELECT p.id, p.file_name, p.file_path, p.storage_provider, p.storage_key,
                   p.original_filename, p.content_type
            FROM construction_report_photos p
            ORDER BY p.id ASC
            """,
        ),
        (
            "mobile_work_report_files",
            """
            SELECT f.id, f.file_name, f.file_path, f.storage_provider, f.storage_key,
                   f.original_filename, f.content_type
            FROM mobile_work_report_files f
            ORDER BY f.id ASC
            """,
        ),
    ]
    for table_name, query in queries:
        for row in conn.execute(query).fetchall():
            yield table_name, row


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Move construction/workforce media metadata to configured file storage.")
    parser.add_argument("--db-path", default=os.getenv("DB_PATH", "contracts.db"))
    parser.add_argument("--provider", default=configured_storage_provider())
    parser.add_argument("--apply", action="store_true", help="Upload files and update DB. Without this flag, runs dry-run only.")
    args = parser.parse_args()

    db_path = Path(args.db_path).expanduser().resolve()
    Storage(str(db_path))
    upload_root = upload_root_for_db(db_path)
    target_provider = args.provider.strip().lower()
    if target_provider in {"", "local"}:
        print("Target provider is local; nothing to upload.")
        return 0

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    target_storage = create_file_storage(upload_root, provider=target_provider) if args.apply else None

    total = 0
    ready = 0
    missing = 0
    skipped = 0
    uploaded = 0
    for table_name, row in iter_media_rows(conn):
        total += 1
        current_provider = (row["storage_provider"] or "local").strip().lower()
        if current_provider not in {"", "local"}:
            skipped += 1
            continue
        key = (row["storage_key"] or row["file_path"] or "").strip()
        if not key:
            missing += 1
            print(f"MISSING_KEY {table_name}#{row['id']}")
            continue
        local_path = (upload_root / key).resolve()
        if upload_root not in local_path.parents and local_path != upload_root:
            missing += 1
            print(f"FORBIDDEN_PATH {table_name}#{row['id']} {key}")
            continue
        if not local_path.is_file():
            missing += 1
            print(f"MISSING_FILE {table_name}#{row['id']} {key}")
            continue
        ready += 1
        data = local_path.read_bytes() if args.apply else b""
        filename = row["original_filename"] or row["file_name"] or local_path.name
        content_type = row["content_type"] or content_type_for_name(filename)
        if not args.apply:
            continue
        assert target_storage is not None
        stored_file = target_storage.save_bytes(key, data, original_filename=filename, content_type=content_type)
        conn.execute(
            f"""
            UPDATE {table_name}
            SET storage_provider = ?,
                storage_key = ?,
                original_filename = ?,
                content_type = ?,
                size_bytes = ?,
                checksum_sha256 = ?
            WHERE id = ?
            """,
            (
                stored_file.provider,
                stored_file.key,
                stored_file.original_filename,
                stored_file.content_type,
                stored_file.size_bytes,
                stored_file.checksum_sha256 or checksum_sha256(data),
                int(row["id"]),
            ),
        )
        uploaded += 1
    if args.apply:
        conn.commit()
    conn.close()
    mode = "APPLY" if args.apply else "DRY_RUN"
    print(f"{mode} total={total} ready={ready} uploaded={uploaded} skipped={skipped} missing={missing}")
    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
