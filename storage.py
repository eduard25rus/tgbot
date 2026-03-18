from __future__ import annotations
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterator


DATE_FMT = "%Y-%m-%d"


@dataclass
class Contract:
    id: int
    chat_id: int
    title: str
    description: str
    end_date: date
    created_at: datetime


@dataclass
class Stage:
    id: int
    contract_id: int
    position: int
    name: str
    status: str
    notes: str
    end_date: date
    amount: float
    created_at: datetime
    contract_title: str
    chat_id: int


@dataclass
class Payment:
    id: int
    contract_id: int
    payment_date: date
    amount: float
    created_at: datetime
    contract_title: str
    chat_id: int


class Storage:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self.connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS chats (
                    chat_id INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS contracts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    end_date TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(chat_id) REFERENCES chats(chat_id)
                );

                CREATE TABLE IF NOT EXISTS stages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_id INTEGER NOT NULL,
                    position INTEGER NOT NULL DEFAULT 1,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'not_started',
                    notes TEXT NOT NULL DEFAULT '',
                    end_date TEXT NOT NULL,
                    amount REAL NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(contract_id) REFERENCES contracts(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS notification_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    days_before INTEGER NOT NULL,
                    target_date TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(chat_id, entity_type, entity_id, days_before, target_date)
                );

                CREATE TABLE IF NOT EXISTS access_grants (
                    owner_chat_id INTEGER NOT NULL,
                    viewer_user_id INTEGER NOT NULL UNIQUE,
                    viewer_username TEXT NOT NULL DEFAULT '',
                    viewer_name TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (owner_chat_id, viewer_user_id)
                );

                CREATE TABLE IF NOT EXISTS invite_tokens (
                    token TEXT PRIMARY KEY,
                    owner_chat_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_id INTEGER NOT NULL,
                    payment_date TEXT NOT NULL,
                    amount REAL NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(contract_id) REFERENCES contracts(id) ON DELETE CASCADE
                );
                """
            )
            columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(stages)").fetchall()
            }
            if "amount" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN amount REAL NOT NULL DEFAULT 0")
            if "position" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN position INTEGER NOT NULL DEFAULT 1")
            if "status" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN status TEXT NOT NULL DEFAULT 'not_started'")
            grant_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(access_grants)").fetchall()
            }
            if "viewer_username" not in grant_columns:
                conn.execute("ALTER TABLE access_grants ADD COLUMN viewer_username TEXT NOT NULL DEFAULT ''")
            if "viewer_name" not in grant_columns:
                conn.execute("ALTER TABLE access_grants ADD COLUMN viewer_name TEXT NOT NULL DEFAULT ''")
            contract_ids = [row["contract_id"] for row in conn.execute("SELECT DISTINCT contract_id FROM stages").fetchall()]
            for contract_id in contract_ids:
                self._normalize_stage_positions(conn, int(contract_id))

    def register_chat(self, chat_id: int) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO chats (chat_id, created_at)
                VALUES (?, ?)
                """,
                (chat_id, datetime.utcnow().isoformat()),
            )

    def create_invite_token(self, owner_chat_id: int, token: str) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO invite_tokens (token, owner_chat_id, created_at)
                VALUES (?, ?, ?)
                """,
                (token, owner_chat_id, datetime.utcnow().isoformat()),
            )

    def consume_invite_token(self, token: str, viewer_user_id: int, viewer_username: str, viewer_name: str) -> int | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT owner_chat_id
                FROM invite_tokens
                WHERE token = ?
                """,
                (token,),
            ).fetchone()
            if row is None:
                return None
            owner_chat_id = int(row["owner_chat_id"])
            conn.execute("DELETE FROM invite_tokens WHERE token = ?", (token,))
            conn.execute(
                """
                INSERT OR REPLACE INTO access_grants (
                    owner_chat_id, viewer_user_id, viewer_username, viewer_name, created_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (owner_chat_id, viewer_user_id, viewer_username, viewer_name, datetime.utcnow().isoformat()),
            )
            return owner_chat_id

    def get_shared_owner(self, viewer_user_id: int) -> int | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT owner_chat_id
                FROM access_grants
                WHERE viewer_user_id = ?
                """,
                (viewer_user_id,),
            ).fetchone()
        return int(row["owner_chat_id"]) if row else None

    def viewers_for_owner(self, owner_chat_id: int) -> list[int]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT viewer_user_id
                FROM access_grants
                WHERE owner_chat_id = ?
                ORDER BY viewer_user_id ASC
                """,
                (owner_chat_id,),
            ).fetchall()
        return [int(row["viewer_user_id"]) for row in rows]

    def viewer_grants_for_owner(self, owner_chat_id: int) -> list[dict]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT viewer_user_id, viewer_username, viewer_name, created_at
                FROM access_grants
                WHERE owner_chat_id = ?
                ORDER BY created_at ASC
                """,
                (owner_chat_id,),
            ).fetchall()
        return [
            {
                "viewer_user_id": int(row["viewer_user_id"]),
                "viewer_username": row["viewer_username"],
                "viewer_name": row["viewer_name"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def revoke_viewer_access(self, owner_chat_id: int, viewer_user_id: int) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                DELETE FROM access_grants
                WHERE owner_chat_id = ? AND viewer_user_id = ?
                """,
                (owner_chat_id, viewer_user_id),
            )
            return cursor.rowcount > 0

    def update_viewer_profile(self, owner_chat_id: int, viewer_user_id: int, viewer_username: str, viewer_name: str) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE access_grants
                SET viewer_username = ?, viewer_name = ?
                WHERE owner_chat_id = ? AND viewer_user_id = ?
                """,
                (viewer_username, viewer_name, owner_chat_id, viewer_user_id),
            )

    def add_contract(self, chat_id: int, title: str, description: str, end_date: date) -> int:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO chats (chat_id, created_at)
                VALUES (?, ?)
                """,
                (chat_id, datetime.utcnow().isoformat()),
            )
            cursor = conn.execute(
                """
                INSERT INTO contracts (chat_id, title, description, end_date, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (chat_id, title.strip(), description.strip(), end_date.strftime(DATE_FMT), datetime.utcnow().isoformat()),
            )
            return int(cursor.lastrowid)

    def add_stage(self, contract_id: int, position: int, notes: str, end_date: date, amount: float) -> int:
        with self.connection() as conn:
            max_position = int(
                conn.execute(
                    "SELECT COALESCE(MAX(position), 0) AS max_position FROM stages WHERE contract_id = ?",
                    (contract_id,),
                ).fetchone()["max_position"]
            )
            normalized_position = max(1, min(position, max_position + 1))
            conn.execute(
                """
                UPDATE stages
                SET position = position + 1
                WHERE contract_id = ? AND position >= ?
                """,
                (contract_id, normalized_position),
            )
            cursor = conn.execute(
                """
                INSERT INTO stages (contract_id, position, name, status, notes, end_date, amount, created_at)
                VALUES (?, ?, ?, 'not_started', ?, ?, ?, ?)
                """,
                (
                    contract_id,
                    normalized_position,
                    f"Этап {normalized_position}",
                    notes.strip(),
                    end_date.strftime(DATE_FMT),
                    amount,
                    datetime.utcnow().isoformat(),
                ),
            )
            self._normalize_stage_positions(conn, contract_id)
            return int(cursor.lastrowid)

    def get_stage(self, chat_id: int, stage_id: int) -> Stage | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT s.id, s.contract_id, s.position, s.name, s.status, s.notes, s.end_date, s.amount, s.created_at,
                       c.title AS contract_title, c.chat_id AS chat_id
                FROM stages s
                JOIN contracts c ON c.id = s.contract_id
                WHERE c.chat_id = ? AND s.id = ?
                """,
                (chat_id, stage_id),
            ).fetchone()
        return self._stage_from_row(row) if row else None

    def update_stage(self, chat_id: int, stage_id: int, name: str, notes: str, end_date: date, amount: float) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE stages
                SET name = ?, notes = ?, end_date = ?, amount = ?
                WHERE id = ?
                  AND contract_id IN (
                      SELECT id FROM contracts WHERE chat_id = ?
                  )
                """,
                (
                    name.strip(),
                    notes.strip(),
                    end_date.strftime(DATE_FMT),
                    amount,
                    stage_id,
                    chat_id,
                ),
            )
            return cursor.rowcount > 0

    def update_stage_status(self, chat_id: int, stage_id: int, status: str) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE stages
                SET status = ?
                WHERE id = ?
                  AND contract_id IN (
                      SELECT id FROM contracts WHERE chat_id = ?
                  )
                """,
                (status, stage_id, chat_id),
            )
            return cursor.rowcount > 0

    def replace_contract_stages(self, chat_id: int, contract_id: int, stage_items: list[dict]) -> bool:
        with self.connection() as conn:
            contract = conn.execute(
                """
                SELECT id
                FROM contracts
                WHERE id = ? AND chat_id = ?
                """,
                (contract_id, chat_id),
            ).fetchone()
            if contract is None:
                return False

            conn.execute("DELETE FROM stages WHERE contract_id = ?", (contract_id,))
            for index, item in enumerate(stage_items, start=1):
                conn.execute(
                    """
                    INSERT INTO stages (contract_id, position, name, status, notes, end_date, amount, created_at)
                    VALUES (?, ?, ?, 'not_started', '', ?, ?, ?)
                    """,
                    (
                        contract_id,
                        index,
                        f"Этап {index}",
                        item["end_date"].strftime(DATE_FMT),
                        item["amount"],
                        datetime.utcnow().isoformat(),
                    ),
                )
            conn.execute(
                """
                UPDATE contracts
                SET end_date = ?
                WHERE id = ? AND chat_id = ?
                """,
                (stage_items[-1]["end_date"].strftime(DATE_FMT), contract_id, chat_id),
            )
            return True

    def delete_contract(self, chat_id: int, contract_id: int) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                "DELETE FROM contracts WHERE id = ? AND chat_id = ?",
                (contract_id, chat_id),
            )
            return cursor.rowcount > 0

    def delete_stage(self, chat_id: int, stage_id: int) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT contract_id
                FROM stages
                WHERE id = ?
                  AND contract_id IN (
                      SELECT id FROM contracts WHERE chat_id = ?
                  )
                """,
                (stage_id, chat_id),
            ).fetchone()
            if row is None:
                return False
            cursor = conn.execute(
                """
                DELETE FROM stages
                WHERE id = ?
                  AND contract_id IN (
                      SELECT id FROM contracts WHERE chat_id = ?
                  )
                """,
                (stage_id, chat_id),
            )
            if cursor.rowcount > 0:
                self._normalize_stage_positions(conn, int(row["contract_id"]))
                return True
            return False

    def add_payment(self, chat_id: int, contract_id: int, payment_date: date, amount: float) -> int | None:
        with self.connection() as conn:
            contract = conn.execute(
                """
                SELECT id
                FROM contracts
                WHERE id = ? AND chat_id = ?
                """,
                (contract_id, chat_id),
            ).fetchone()
            if contract is None:
                return None
            cursor = conn.execute(
                """
                INSERT INTO payments (contract_id, payment_date, amount, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (contract_id, payment_date.strftime(DATE_FMT), amount, datetime.utcnow().isoformat()),
            )
            return int(cursor.lastrowid)

    def get_payment(self, chat_id: int, payment_id: int) -> Payment | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT p.id, p.contract_id, p.payment_date, p.amount, p.created_at,
                       c.title AS contract_title, c.chat_id AS chat_id
                FROM payments p
                JOIN contracts c ON c.id = p.contract_id
                WHERE c.chat_id = ? AND p.id = ?
                """,
                (chat_id, payment_id),
            ).fetchone()
        return self._payment_from_row(row) if row else None

    def update_payment(self, chat_id: int, payment_id: int, payment_date: date, amount: float) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE payments
                SET payment_date = ?, amount = ?
                WHERE id = ?
                  AND contract_id IN (
                      SELECT id FROM contracts WHERE chat_id = ?
                  )
                """,
                (payment_date.strftime(DATE_FMT), amount, payment_id, chat_id),
            )
            return cursor.rowcount > 0

    def delete_payment(self, chat_id: int, payment_id: int) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                DELETE FROM payments
                WHERE id = ?
                  AND contract_id IN (
                      SELECT id FROM contracts WHERE chat_id = ?
                  )
                """,
                (payment_id, chat_id),
            )
            return cursor.rowcount > 0

    def list_payments_for_contract(self, chat_id: int, contract_id: int) -> list[Payment]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT p.id, p.contract_id, p.payment_date, p.amount, p.created_at,
                       c.title AS contract_title, c.chat_id AS chat_id
                FROM payments p
                JOIN contracts c ON c.id = p.contract_id
                WHERE c.chat_id = ? AND c.id = ?
                ORDER BY p.payment_date ASC, p.id ASC
                """,
                (chat_id, contract_id),
            ).fetchall()
        return [self._payment_from_row(row) for row in rows]

    def contract_payment_total(self, chat_id: int, contract_id: int) -> float:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(p.amount), 0) AS total_amount
                FROM payments p
                JOIN contracts c ON c.id = p.contract_id
                WHERE c.chat_id = ? AND c.id = ?
                """,
                (chat_id, contract_id),
            ).fetchone()
        return float(row["total_amount"]) if row else 0.0

    def list_contracts(self, chat_id: int) -> list[Contract]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, chat_id, title, description, end_date, created_at
                FROM contracts
                WHERE chat_id = ?
                ORDER BY end_date ASC, id ASC
                """,
                (chat_id,),
            ).fetchall()
        return [self._contract_from_row(row) for row in rows]

    def get_contract(self, chat_id: int, contract_id: int) -> Contract | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT id, chat_id, title, description, end_date, created_at
                FROM contracts
                WHERE chat_id = ? AND id = ?
                """,
                (chat_id, contract_id),
            ).fetchone()
        return self._contract_from_row(row) if row else None

    def list_stages_for_contract(self, chat_id: int, contract_id: int) -> list[Stage]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT s.id, s.contract_id, s.position, s.name, s.status, s.notes, s.end_date, s.amount, s.created_at,
                       c.title AS contract_title, c.chat_id AS chat_id
                FROM stages s
                JOIN contracts c ON c.id = s.contract_id
                WHERE c.chat_id = ? AND c.id = ?
                ORDER BY s.position ASC, s.id ASC
                """,
                (chat_id, contract_id),
            ).fetchall()
        return [self._stage_from_row(row) for row in rows]

    def upcoming_items(self, chat_id: int, within_days: int = 30) -> list[dict]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT 'contract' AS entity_type,
                       c.id AS entity_id,
                       c.title AS title,
                       c.description AS details,
                       c.end_date AS end_date
                FROM contracts c
                WHERE c.chat_id = ?
                UNION ALL
                SELECT 'stage' AS entity_type,
                       s.id AS entity_id,
                       s.name || ' (' || c.title || ')' AS title,
                       s.notes AS details,
                       s.end_date AS end_date
                FROM stages s
                JOIN contracts c ON c.id = s.contract_id
                WHERE c.chat_id = ?
                ORDER BY end_date ASC
                """,
                (chat_id, chat_id),
            ).fetchall()

        today = date.today()
        items: list[dict] = []
        for row in rows:
            end_date = date.fromisoformat(row["end_date"])
            days_left = (end_date - today).days
            if days_left <= within_days:
                items.append(
                    {
                        "entity_type": row["entity_type"],
                        "entity_id": row["entity_id"],
                        "title": row["title"],
                        "details": row["details"],
                        "end_date": end_date,
                        "days_left": days_left,
                    }
                )
        return items

    def nearest_item(self, chat_id: int) -> dict | None:
        items = self.upcoming_items(chat_id, within_days=36500)
        if not items:
            return None
        items.sort(key=lambda item: (abs(item["days_left"]), item["end_date"], item["entity_id"]))
        return items[0]

    def reminder_candidates(self, days_before: tuple[int, ...]) -> list[dict]:
        with self.connection() as conn:
            contract_rows = conn.execute(
                """
                SELECT c.chat_id, 'contract' AS entity_type, c.id AS entity_id,
                       c.title AS title, c.description AS details, c.end_date AS end_date
                FROM contracts c
                """
            ).fetchall()
            stage_rows = conn.execute(
                """
                SELECT c.chat_id, 'stage' AS entity_type, s.id AS entity_id,
                       s.name || ' (' || c.title || ')' AS title,
                       s.notes AS details, s.end_date AS end_date
                FROM stages s
                JOIN contracts c ON c.id = s.contract_id
                """
            ).fetchall()

        today = date.today()
        result: list[dict] = []
        for row in [*contract_rows, *stage_rows]:
            end_date = date.fromisoformat(row["end_date"])
            days_left = (end_date - today).days
            if days_left in days_before:
                result.append(
                    {
                        "chat_id": row["chat_id"],
                        "entity_type": row["entity_type"],
                        "entity_id": row["entity_id"],
                        "title": row["title"],
                        "details": row["details"],
                        "end_date": end_date,
                        "days_left": days_left,
                    }
                )
        return result

    def reminder_already_sent(self, chat_id: int, entity_type: str, entity_id: int, days_before: int, target_date: date) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM notification_log
                WHERE chat_id = ?
                  AND entity_type = ?
                  AND entity_id = ?
                  AND days_before = ?
                  AND target_date = ?
                """,
                (chat_id, entity_type, entity_id, days_before, target_date.strftime(DATE_FMT)),
            ).fetchone()
        return row is not None

    def mark_reminder_sent(self, chat_id: int, entity_type: str, entity_id: int, days_before: int, target_date: date) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO notification_log (
                    chat_id, entity_type, entity_id, days_before, target_date, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    entity_type,
                    entity_id,
                    days_before,
                    target_date.strftime(DATE_FMT),
                    datetime.utcnow().isoformat(),
                ),
            )

    @staticmethod
    def _contract_from_row(row: sqlite3.Row) -> Contract:
        return Contract(
            id=row["id"],
            chat_id=row["chat_id"],
            title=row["title"],
            description=row["description"],
            end_date=date.fromisoformat(row["end_date"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _stage_from_row(row: sqlite3.Row) -> Stage:
        return Stage(
            id=row["id"],
            contract_id=row["contract_id"],
            position=row["position"],
            name=row["name"],
            status=row["status"],
            notes=row["notes"],
            end_date=date.fromisoformat(row["end_date"]),
            amount=float(row["amount"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            contract_title=row["contract_title"],
            chat_id=row["chat_id"],
        )

    @staticmethod
    def _payment_from_row(row: sqlite3.Row) -> Payment:
        return Payment(
            id=row["id"],
            contract_id=row["contract_id"],
            payment_date=date.fromisoformat(row["payment_date"]),
            amount=float(row["amount"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            contract_title=row["contract_title"],
            chat_id=row["chat_id"],
        )

    @staticmethod
    def _normalize_stage_positions(conn: sqlite3.Connection, contract_id: int) -> None:
        rows = conn.execute(
            """
            SELECT id
            FROM stages
            WHERE contract_id = ?
            ORDER BY position ASC, end_date ASC, id ASC
            """,
            (contract_id,),
        ).fetchall()
        for index, row in enumerate(rows, start=1):
            conn.execute(
                """
                UPDATE stages
                SET position = ?, name = ?
                WHERE id = ?
                """,
                (index, f"Этап {index}", row["id"]),
            )
