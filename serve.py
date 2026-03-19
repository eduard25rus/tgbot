from __future__ import annotations

import logging
import os
import threading
from wsgiref.simple_server import make_server

from telegram import Update

from bot import build_application
from webapp import app as web_app


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.INFO,
)
LOGGER = logging.getLogger(__name__)


def resolve_web_bind() -> tuple[str, int]:
    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT") or os.getenv("PORT", "8000"))
    return host, port


def main() -> None:
    host, port = resolve_web_bind()
    server = make_server(host, port, web_app)
    LOGGER.info("CRM web is listening on http://%s:%s", host, port)

    web_thread = threading.Thread(target=server.serve_forever, name="crm-web-server", daemon=True)
    web_thread.start()

    application = build_application()
    LOGGER.info("Telegram bot and CRM web are starting")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
