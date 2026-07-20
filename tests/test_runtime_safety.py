from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from runtime_safety import StorageSafetyError, validate_existing_sqlite, validate_runtime_storage


class RuntimeStorageSafetyTests(unittest.TestCase):
    def test_required_missing_database_fails_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            os.environ,
            {"DB_PATH": str(Path(tmp) / "contracts.db"), "REQUIRE_EXISTING_DB": "1"},
            clear=False,
        ):
            path = Path(os.environ["DB_PATH"])
            with self.assertRaises(StorageSafetyError):
                validate_runtime_storage()
            self.assertFalse(path.exists())

    def test_empty_database_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "contracts.db"
            path.touch()
            with self.assertRaises(StorageSafetyError):
                validate_existing_sqlite(path)

    def test_corrupt_database_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "contracts.db"
            path.write_bytes(b"not sqlite" * 1024)
            with self.assertRaises(StorageSafetyError):
                validate_existing_sqlite(path)

    def test_valid_existing_database_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "contracts.db"
            with sqlite3.connect(path) as connection:
                connection.execute("CREATE TABLE web_users (id INTEGER PRIMARY KEY)")
                connection.execute("INSERT INTO web_users DEFAULT VALUES")
            report = validate_existing_sqlite(path)
            self.assertEqual(report["quick_check"], "ok")
            self.assertEqual(report["tables"], 1)


if __name__ == "__main__":
    unittest.main()
