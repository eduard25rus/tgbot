from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import Mock, patch


telegram_stub = types.ModuleType("telegram")
telegram_stub.Update = type("Update", (), {"ALL_TYPES": object()})
bot_stub = types.ModuleType("bot")
bot_stub.build_application = Mock()
backup_stub = types.ModuleType("scripts.backup_sqlite_to_s3")
backup_stub.backup_sqlite_to_storage = Mock()
mail_stub = types.ModuleType("scripts.import_bank_mail")
mail_stub.run_bank_mail_import = Mock()

with patch.dict(
    sys.modules,
    {
        "telegram": telegram_stub,
        "bot": bot_stub,
        "scripts.backup_sqlite_to_s3": backup_stub,
        "scripts.import_bank_mail": mail_stub,
    },
):
    import serve


class ServeResilienceTests(unittest.TestCase):
    def test_web_server_stays_primary_when_bot_is_enabled(self) -> None:
        server = Mock()
        with (
            patch.object(serve, "validate_runtime_storage", return_value={"path": "/tmp/test.db"}),
            patch.object(serve, "make_server", return_value=server),
            patch.object(serve, "start_bank_mail_import_thread") as start_mail,
            patch.object(serve, "start_db_backup_thread") as start_backup,
            patch.object(serve, "start_telegram_bot_thread") as start_bot,
            patch.object(serve, "env_truthy", return_value=True),
        ):
            serve.main()

        start_mail.assert_called_once_with()
        start_backup.assert_called_once_with()
        start_bot.assert_called_once_with()
        server.serve_forever.assert_called_once_with()
        server.server_close.assert_called_once_with()

    def test_background_jobs_still_start_when_bot_is_disabled(self) -> None:
        server = Mock()
        with (
            patch.object(serve, "validate_runtime_storage", return_value={"path": "/tmp/test.db"}),
            patch.object(serve, "make_server", return_value=server),
            patch.object(serve, "start_bank_mail_import_thread") as start_mail,
            patch.object(serve, "start_db_backup_thread") as start_backup,
            patch.object(serve, "start_telegram_bot_thread") as start_bot,
            patch.object(serve, "env_truthy", return_value=False),
        ):
            serve.main()

        start_mail.assert_called_once_with()
        start_backup.assert_called_once_with()
        start_bot.assert_not_called()
        server.serve_forever.assert_called_once_with()
        server.server_close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
