from __future__ import annotations

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
from scripts.import_bank_mail import run_bank_mail_import
from webapp import app as web_app


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


def main() -> None:
    host, port = resolve_web_bind()
    server = make_server(host, port, web_app, server_class=ThreadingWSGIServer)
    LOGGER.info("CRM web is listening on http://%s:%s", host, port)

    web_thread = threading.Thread(target=server.serve_forever, name="crm-web-server", daemon=True)
    web_thread.start()
    start_bank_mail_import_thread()

    application = build_application()
    LOGGER.info("Telegram bot and CRM web are starting")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
