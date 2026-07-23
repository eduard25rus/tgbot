from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from datetime import datetime
from datetime import timedelta
from socketserver import ThreadingMixIn
from zoneinfo import ZoneInfo
from wsgiref.simple_server import WSGIServer
from wsgiref.simple_server import make_server

from telegram import Update

from bot import build_application
from scripts.backup_sqlite_to_s3 import backup_sqlite_to_storage
from scripts.import_bank_mail import run_bank_mail_import
from webapp import app as web_app
from runtime_safety import validate_runtime_storage


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.INFO,
)
LOGGER = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


def resolve_web_bind() -> tuple[str, int]:
    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT") or os.getenv("PORT", "8000"))
    return host, port


def env_truthy(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"", "0", "false", "no", "off"}


def bank_mail_import_configured() -> bool:
    required = ("BANK_MAIL_OWNER_CHAT_ID", "BANK_MAIL_LOGIN", "BANK_MAIL_PASSWORD")
    return all(os.getenv(name, "").strip() for name in required)


def resolve_daily_import_time() -> tuple[int, int]:
    raw = os.getenv("BANK_MAIL_DAILY_TIME", "08:30").strip() or "08:30"
    try:
        hour_raw, minute_raw = raw.split(":", 1)
        hour = int(hour_raw)
        minute = int(minute_raw)
    except ValueError:
        LOGGER.warning("Invalid BANK_MAIL_DAILY_TIME=%r, using 08:30", raw)
        return 8, 30
    if hour not in range(24) or minute not in range(60):
        LOGGER.warning("Invalid BANK_MAIL_DAILY_TIME=%r, using 08:30", raw)
        return 8, 30
    return hour, minute


def resolve_import_interval_seconds() -> int:
    raw = os.getenv("BANK_MAIL_INTERVAL_MINUTES", "30").strip()
    if not raw:
        return 0
    try:
        minutes = int(raw)
    except ValueError:
        LOGGER.warning("Invalid BANK_MAIL_INTERVAL_MINUTES=%r, using daily schedule", raw)
        return 0
    if minutes <= 0:
        return 0
    return max(300, minutes * 60)


def seconds_until_next_daily_run() -> float:
    tz_name = os.getenv("BOT_TIMEZONE", "Asia/Vladivostok").strip() or "Asia/Vladivostok"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        LOGGER.warning("Invalid BOT_TIMEZONE=%r, using Asia/Vladivostok", tz_name)
        tz = ZoneInfo("Asia/Vladivostok")
    hour, minute = resolve_daily_import_time()
    now = datetime.now(tz)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


def run_bank_mail_import_once(reason: str) -> None:
    try:
        imported, skipped, errors = run_bank_mail_import()
        LOGGER.info(
            "Bank statement mail import %s finished: processed=%s, already_processed=%s, errors=%s",
            reason,
            imported,
            skipped,
            errors,
        )
    except Exception:
        LOGGER.exception("Bank statement mail import %s failed", reason)


def bank_mail_import_loop() -> None:
    if env_truthy("BANK_MAIL_RUN_ON_STARTUP", True):
        time.sleep(int(os.getenv("BANK_MAIL_STARTUP_DELAY_SECONDS", "30")))
        run_bank_mail_import_once("startup")
    interval_seconds = resolve_import_interval_seconds()
    while True:
        delay = interval_seconds or seconds_until_next_daily_run()
        if interval_seconds:
            LOGGER.info("Next bank statement mail import in %.0f seconds by interval", delay)
        else:
            LOGGER.info("Next bank statement mail import in %.0f seconds", delay)
        time.sleep(delay)
        run_bank_mail_import_once("interval" if interval_seconds else "scheduled")


def start_bank_mail_import_thread() -> None:
    auto_enabled = env_truthy("BANK_MAIL_AUTO_IMPORT_ENABLED", bank_mail_import_configured())
    if not auto_enabled:
        LOGGER.info("Bank statement mail auto-import is disabled")
        return
    if not bank_mail_import_configured():
        LOGGER.warning("Bank statement mail auto-import is enabled but IMAP env vars are incomplete")
        return
    thread = threading.Thread(target=bank_mail_import_loop, name="bank-mail-import", daemon=True)
    thread.start()
    LOGGER.info("Bank statement mail auto-import thread started")


def resolve_db_backup_interval_seconds() -> int:
    raw = os.getenv("DB_BACKUP_INTERVAL_HOURS", "2").strip() or "2"
    try:
        hours = float(raw)
    except ValueError:
        LOGGER.warning("Invalid DB_BACKUP_INTERVAL_HOURS=%r, using 2 hours", raw)
        hours = 2.0
    return max(900, int(hours * 3600))


def run_db_backup_once(reason: str) -> None:
    try:
        manifest = backup_sqlite_to_storage("hourly")
        LOGGER.info(
            "SQLite backup %s uploaded: key=%s bytes=%s sha256=%s",
            reason,
            manifest["backup_key"],
            manifest["gzip_size_bytes"],
            manifest["sha256"],
        )
    except Exception:
        LOGGER.exception("SQLite backup %s failed", reason)


def db_backup_loop() -> None:
    if env_truthy("DB_BACKUP_RUN_ON_STARTUP", True):
        time.sleep(int(os.getenv("DB_BACKUP_STARTUP_DELAY_SECONDS", "120")))
        run_db_backup_once("startup")
    interval_seconds = resolve_db_backup_interval_seconds()
    while True:
        LOGGER.info("Next SQLite backup in %.0f seconds", interval_seconds)
        time.sleep(interval_seconds)
        run_db_backup_once("interval")


def start_db_backup_thread() -> None:
    if not env_truthy("DB_BACKUP_ENABLED", False):
        LOGGER.info("SQLite backup thread is disabled")
        return
    if os.getenv("FILE_STORAGE_PROVIDER", "local").strip().lower() not in {"s3", "yandex"}:
        LOGGER.warning("SQLite backup is enabled but FILE_STORAGE_PROVIDER is not s3/yandex")
        return
    thread = threading.Thread(target=db_backup_loop, name="sqlite-backup", daemon=True)
    thread.start()
    LOGGER.info("SQLite backup thread started")


def telegram_polling_loop() -> None:
    retry_seconds = max(10, int(os.getenv("BOT_RETRY_SECONDS", "30")))
    while True:
        application = None
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            application = build_application()
            LOGGER.info("Telegram bot polling is starting")
            application.run_polling(
                allowed_updates=Update.ALL_TYPES,
                stop_signals=None,
                close_loop=True,
            )
            LOGGER.warning("Telegram bot polling stopped; retrying in %s seconds", retry_seconds)
        except Exception:
            LOGGER.exception(
                "Telegram bot polling failed; CRM web remains available and polling will retry in %s seconds",
                retry_seconds,
            )
            if not loop.is_closed():
                loop.close()
        finally:
            asyncio.set_event_loop(None)
        time.sleep(retry_seconds)


def start_telegram_bot_thread() -> None:
    thread = threading.Thread(target=telegram_polling_loop, name="telegram-bot", daemon=True)
    thread.start()
    LOGGER.info("Telegram bot thread started")


def main() -> None:
    storage_report = validate_runtime_storage()
    LOGGER.info(
        "Persistent storage validated: path=%s required=%s size_bytes=%s",
        storage_report.get("path"),
        storage_report.get("required"),
        storage_report.get("size_bytes", "n/a"),
    )
    host, port = resolve_web_bind()
    server = make_server(host, port, web_app, server_class=ThreadingWSGIServer)
    LOGGER.info("CRM web is listening on http://%s:%s", host, port)

    start_bank_mail_import_thread()
    start_db_backup_thread()
    if env_truthy("BOT_ENABLED", True):
        start_telegram_bot_thread()
    else:
        LOGGER.info("Web-only mode is active; Telegram polling and bot reminders are disabled")

    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
