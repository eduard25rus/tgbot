#!/usr/bin/env python3
"""Render an allow-listed Timeweb env file from Railway-provided environment."""

from __future__ import annotations

import os


EXACT_NAMES = {
    "BOT_TOKEN",
    "BOT_TIMEZONE",
    "BANK_ACCOUNT_LABELS",
    "PUBLIC_BASE_URL",
    "SOFTWARE_DIGEST_TOKEN",
    "SOFTWARE_DIGEST_OWNER",
    "CRON_TOKEN",
}
PREFIXES = ("BANK_MAIL_", "S3_", "CASH_PUSH_", "VAPID_", "DB_BACKUP_")


def dotenv_quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")
    return f'"{escaped}"'


def main() -> int:
    selected = {
        key: value
        for key, value in os.environ.items()
        if key in EXACT_NAMES or key.startswith(PREFIXES)
    }
    selected.update(
        {
            "BOT_ENABLED": "0",
            "BANK_MAIL_AUTO_IMPORT_ENABLED": "0",
            "DB_BACKUP_ENABLED": "0",
            "FILE_STORAGE_PROVIDER": os.getenv("FILE_STORAGE_PROVIDER", "local"),
            "FILE_STORAGE_REDIRECT_SIGNED_URL": os.getenv("FILE_STORAGE_REDIRECT_SIGNED_URL", "1"),
            "PUBLIC_BASE_URL": "https://felis-test-72056842.72-56-8-42.sslip.io",
        }
    )
    for key in sorted(selected):
        if selected[key] != "":
            print(f"{key}={dotenv_quote(selected[key])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
