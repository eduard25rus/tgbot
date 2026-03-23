from __future__ import annotations
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Iterator
from typing import Optional


DATE_FMT = "%Y-%m-%d"
WEB_SECTION_IDS = ("contracts", "auctions", "expenses", "payroll", "finance", "access")


@dataclass
class Contract:
    id: int
    chat_id: int
    title: str
    description: str
    end_date: date
    advance_percent: Optional[float]
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


@dataclass
class Auction:
    id: int
    owner_chat_id: int
    auction_number: str
    bid_deadline: date
    amount: float
    advance_percent: Optional[float]
    title: str
    city: str
    source_url: str
    max_discount_percent: Optional[float]
    min_bid_amount: Optional[float]
    material_cost: Optional[float]
    estimate_status: str
    estimate_status_updated_at: Optional[datetime]
    submit_decision_status: str
    application_status: str
    result_status: str
    final_bid_amount: Optional[float]
    archived_at: Optional[datetime]
    deleted_at: Optional[datetime]
    created_at: datetime


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
                    advance_percent REAL,
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

                CREATE TABLE IF NOT EXISTS web_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_chat_id INTEGER NOT NULL,
                    email TEXT NOT NULL,
                    full_name TEXT NOT NULL,
                    role_name TEXT NOT NULL DEFAULT 'Viewer',
                    password_state TEXT NOT NULL DEFAULT 'pending_setup',
                    password_hash TEXT NOT NULL DEFAULT '',
                    is_active INTEGER NOT NULL DEFAULT 1,
                    is_super_admin INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    UNIQUE(owner_chat_id, email)
                );

                CREATE TABLE IF NOT EXISTS web_user_section_access (
                    user_id INTEGER NOT NULL,
                    section_id TEXT NOT NULL,
                    can_view INTEGER NOT NULL DEFAULT 0,
                    can_edit INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (user_id, section_id),
                    FOREIGN KEY(user_id) REFERENCES web_users(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS web_sessions (
                    token TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES web_users(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS web_password_setup_tokens (
                    token TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES web_users(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS auctions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_chat_id INTEGER NOT NULL,
                    auction_number TEXT NOT NULL,
                    bid_deadline TEXT NOT NULL,
                    amount REAL NOT NULL DEFAULT 0,
                    advance_percent REAL,
                    title TEXT NOT NULL,
                    city TEXT NOT NULL DEFAULT '',
                    source_url TEXT NOT NULL DEFAULT '',
                    max_discount_percent REAL,
                    min_bid_amount REAL,
                    material_cost REAL,
                    estimate_status TEXT NOT NULL DEFAULT 'pending',
                    estimate_status_updated_at TEXT,
                    submit_decision_status TEXT NOT NULL DEFAULT 'pending',
                    approval_status TEXT NOT NULL DEFAULT 'new',
                    application_status TEXT NOT NULL DEFAULT 'not_submitted',
                    result_status TEXT NOT NULL DEFAULT 'pending',
                    final_bid_amount REAL,
                    archived_at TEXT,
                    deleted_at TEXT,
                    created_at TEXT NOT NULL
                );
                """
            )
            columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(stages)").fetchall()
            }
            contract_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(contracts)").fetchall()
            }
            if "amount" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN amount REAL NOT NULL DEFAULT 0")
            if "position" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN position INTEGER NOT NULL DEFAULT 1")
            if "status" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN status TEXT NOT NULL DEFAULT 'not_started'")
            if "advance_percent" not in contract_columns:
                conn.execute("ALTER TABLE contracts ADD COLUMN advance_percent REAL")
            grant_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(access_grants)").fetchall()
            }
            if "viewer_username" not in grant_columns:
                conn.execute("ALTER TABLE access_grants ADD COLUMN viewer_username TEXT NOT NULL DEFAULT ''")
            if "viewer_name" not in grant_columns:
                conn.execute("ALTER TABLE access_grants ADD COLUMN viewer_name TEXT NOT NULL DEFAULT ''")
            web_user_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(web_users)").fetchall()
            }
            if "password_state" not in web_user_columns:
                conn.execute("ALTER TABLE web_users ADD COLUMN password_state TEXT NOT NULL DEFAULT 'pending_setup'")
            if "password_hash" not in web_user_columns:
                conn.execute("ALTER TABLE web_users ADD COLUMN password_hash TEXT NOT NULL DEFAULT ''")
            if "is_active" not in web_user_columns:
                conn.execute("ALTER TABLE web_users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
            if "is_super_admin" not in web_user_columns:
                conn.execute("ALTER TABLE web_users ADD COLUMN is_super_admin INTEGER NOT NULL DEFAULT 0")
            auction_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(auctions)").fetchall()
            }
            if "estimate_status" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN estimate_status TEXT NOT NULL DEFAULT 'pending'")
                conn.execute(
                    """
                    UPDATE auctions
                    SET estimate_status = CASE
                        WHEN approval_status IN ('estimate', 'approved') THEN 'approved'
                        WHEN approval_status = 'not_interesting' THEN 'rejected'
                        ELSE estimate_status
                    END
                    WHERE estimate_status = 'pending'
                    """
                )
            if "estimate_status_updated_at" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN estimate_status_updated_at TEXT")
            if "submit_decision_status" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN submit_decision_status TEXT NOT NULL DEFAULT 'pending'")
                conn.execute(
                    """
                    UPDATE auctions
                    SET submit_decision_status = CASE
                        WHEN approval_status = 'approved' THEN 'approved'
                        WHEN approval_status = 'not_interesting' THEN 'rejected'
                        ELSE submit_decision_status
                    END
                    WHERE submit_decision_status = 'pending'
                    """
                )
            if "max_discount_percent" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN max_discount_percent REAL")
            if "min_bid_amount" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN min_bid_amount REAL")
            if "material_cost" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN material_cost REAL")
            if "final_bid_amount" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN final_bid_amount REAL")
            if "archived_at" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN archived_at TEXT")
            if "deleted_at" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN deleted_at TEXT")
            if "advance_percent" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN advance_percent REAL")
                conn.execute(
                    """
                    UPDATE auctions
                    SET archived_at = created_at
                    WHERE submit_decision_status = 'rejected' OR result_status IN ('won', 'lost', 'rejected')
                    """
                )
            if "application_status" in auction_columns:
                conn.execute(
                    """
                    UPDATE auctions
                    SET submit_decision_status = 'submitted'
                    WHERE application_status = 'submitted' AND submit_decision_status = 'approved'
                    """
                )
            contract_ids = [row["contract_id"] for row in conn.execute("SELECT DISTINCT contract_id FROM stages").fetchall()]
            for contract_id in contract_ids:
                self._normalize_stage_positions(conn, int(contract_id))
            owner_ids = {
                int(row["chat_id"]) for row in conn.execute("SELECT DISTINCT chat_id FROM contracts").fetchall()
            }
            owner_ids.update(
                int(row["owner_chat_id"]) for row in conn.execute("SELECT DISTINCT owner_chat_id FROM auctions").fetchall()
            )
            for owner_chat_id in owner_ids:
                self._ensure_default_web_admin(conn, owner_chat_id)

    def register_chat(self, chat_id: int) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO chats (chat_id, created_at)
                VALUES (?, ?)
                """,
                (chat_id, datetime.utcnow().isoformat()),
            )
            self._ensure_default_web_admin(conn, chat_id)

    def ensure_default_web_admin(self, owner_chat_id: int) -> None:
        with self.connection() as conn:
            self._ensure_default_web_admin(conn, owner_chat_id)

    def list_web_users(self, owner_chat_id: int) -> list[dict]:
        with self.connection() as conn:
            self._ensure_default_web_admin(conn, owner_chat_id)
            rows = conn.execute(
                """
                SELECT id, owner_chat_id, email, full_name, role_name, password_state,
                       password_hash, is_active, is_super_admin, created_at
                FROM web_users
                WHERE owner_chat_id = ?
                ORDER BY is_super_admin DESC, created_at ASC, id ASC
                """,
                (owner_chat_id,),
            ).fetchall()
            permission_rows = conn.execute(
                """
                SELECT user_id, section_id, can_view, can_edit
                FROM web_user_section_access
                WHERE user_id IN (
                    SELECT id FROM web_users WHERE owner_chat_id = ?
                )
                ORDER BY user_id ASC, section_id ASC
                """,
                (owner_chat_id,),
            ).fetchall()

        permissions_by_user: dict[int, dict[str, dict[str, bool]]] = {}
        for row in permission_rows:
            user_id = int(row["user_id"])
            permissions_by_user.setdefault(user_id, {})[row["section_id"]] = {
                "can_view": bool(row["can_view"]),
                "can_edit": bool(row["can_edit"]),
            }

        users: list[dict] = []
        for row in rows:
            user_id = int(row["id"])
            users.append(
                {
                    "id": user_id,
                    "owner_chat_id": int(row["owner_chat_id"]),
                    "login": row["email"],
                    "email": row["email"],
                    "full_name": row["full_name"],
                    "role_name": row["role_name"],
                    "password_state": row["password_state"],
                    "password_hash": row["password_hash"],
                    "is_active": bool(row["is_active"]),
                    "is_super_admin": bool(row["is_super_admin"]),
                    "created_at": row["created_at"],
                    "permissions": self._permissions_payload(permissions_by_user.get(user_id, {})),
                }
            )
        return users

    def create_web_user(
        self,
        owner_chat_id: int,
        login: str,
        full_name: str,
        role_name: str,
        permissions: dict[str, dict[str, bool]],
    ) -> int:
        with self.connection() as conn:
            self._ensure_default_web_admin(conn, owner_chat_id)
            cursor = conn.execute(
                """
                INSERT INTO web_users (
                    owner_chat_id, email, full_name, role_name, password_state,
                    is_active, is_super_admin, created_at
                )
                VALUES (?, ?, ?, ?, 'pending_setup', 1, 0, ?)
                """,
                (
                    owner_chat_id,
                    login.strip().lower(),
                    full_name.strip(),
                    role_name.strip() or "Viewer",
                    datetime.utcnow().isoformat(),
                ),
            )
            user_id = int(cursor.lastrowid)
            self._replace_web_user_permissions(conn, user_id, permissions)
            return user_id

    def update_web_user(
        self,
        owner_chat_id: int,
        user_id: int,
        full_name: str,
        role_name: str,
        permissions: dict[str, dict[str, bool]],
    ) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT is_super_admin
                FROM web_users
                WHERE id = ? AND owner_chat_id = ?
                """,
                (user_id, owner_chat_id),
            ).fetchone()
            if row is None:
                return False
            is_super_admin = bool(row["is_super_admin"])
            conn.execute(
                """
                UPDATE web_users
                SET full_name = ?, role_name = ?
                WHERE id = ? AND owner_chat_id = ?
                """,
                (
                    full_name.strip() or ("BigBoss" if is_super_admin else "Пользователь"),
                    role_name.strip() or ("BigBoss" if is_super_admin else "Viewer"),
                    user_id,
                    owner_chat_id,
                ),
            )
            self._replace_web_user_permissions(
                conn,
                user_id,
                self._full_access_permissions() if is_super_admin else permissions,
            )
            return True

    def set_web_user_active(self, owner_chat_id: int, user_id: int, is_active: bool) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT is_super_admin
                FROM web_users
                WHERE id = ? AND owner_chat_id = ?
                """,
                (user_id, owner_chat_id),
            ).fetchone()
            if row is None or bool(row["is_super_admin"]):
                return False
            cursor = conn.execute(
                """
                UPDATE web_users
                SET is_active = ?
                WHERE id = ? AND owner_chat_id = ?
                """,
                (1 if is_active else 0, user_id, owner_chat_id),
            )
            return cursor.rowcount > 0

    def delete_web_user(self, owner_chat_id: int, user_id: int) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT is_super_admin
                FROM web_users
                WHERE id = ? AND owner_chat_id = ?
                """,
                (user_id, owner_chat_id),
            ).fetchone()
            if row is None or bool(row["is_super_admin"]):
                return False
            cursor = conn.execute(
                """
                DELETE FROM web_users
                WHERE id = ? AND owner_chat_id = ?
                """,
                (user_id, owner_chat_id),
            )
            return cursor.rowcount > 0

    def auth_hint_user(self) -> dict | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT id, owner_chat_id, email, full_name, role_name, password_state,
                       password_hash, is_active, is_super_admin, created_at
                FROM web_users
                WHERE is_super_admin = 1
                ORDER BY created_at ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            permissions = self._full_access_permissions() if bool(row["is_super_admin"]) else self._permissions_payload()
            return {
                "id": int(row["id"]),
                "owner_chat_id": int(row["owner_chat_id"]),
                "login": row["email"],
                "email": row["email"],
                "full_name": row["full_name"],
                "role_name": row["role_name"],
                "password_state": row["password_state"],
                "password_hash": row["password_hash"],
                "is_active": bool(row["is_active"]),
                "is_super_admin": bool(row["is_super_admin"]),
                "created_at": row["created_at"],
                "permissions": permissions,
            }

    def get_web_user_by_login(self, login: str) -> dict | None:
        normalized = login.strip().lower()
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT id, owner_chat_id, email, full_name, role_name, password_state,
                       password_hash, is_active, is_super_admin, created_at
                FROM web_users
                WHERE email = ?
                ORDER BY is_super_admin DESC, created_at ASC
                LIMIT 1
                """,
                (normalized,),
            ).fetchone()
            if row is None:
                return None
            permission_rows = conn.execute(
                """
                SELECT section_id, can_view, can_edit
                FROM web_user_section_access
                WHERE user_id = ?
                ORDER BY section_id ASC
                """,
                (int(row["id"]),),
            ).fetchall()
        permissions = {
            item["section_id"]: {
                "can_view": bool(item["can_view"]),
                "can_edit": bool(item["can_edit"]),
            }
            for item in permission_rows
        }
        return {
            "id": int(row["id"]),
            "owner_chat_id": int(row["owner_chat_id"]),
            "login": row["email"],
            "email": row["email"],
            "full_name": row["full_name"],
            "role_name": row["role_name"],
            "password_state": row["password_state"],
            "password_hash": row["password_hash"],
            "is_active": bool(row["is_active"]),
            "is_super_admin": bool(row["is_super_admin"]),
            "created_at": row["created_at"],
            "permissions": self._full_access_permissions() if bool(row["is_super_admin"]) else self._permissions_payload(permissions),
        }

    def get_web_user_by_id(self, user_id: int) -> dict | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT id, owner_chat_id, email, full_name, role_name, password_state,
                       password_hash, is_active, is_super_admin, created_at
                FROM web_users
                WHERE id = ?
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()
            if row is None:
                return None
            permission_rows = conn.execute(
                """
                SELECT section_id, can_view, can_edit
                FROM web_user_section_access
                WHERE user_id = ?
                ORDER BY section_id ASC
                """,
                (user_id,),
            ).fetchall()
        permissions = {
            item["section_id"]: {
                "can_view": bool(item["can_view"]),
                "can_edit": bool(item["can_edit"]),
            }
            for item in permission_rows
        }
        return {
            "id": int(row["id"]),
            "owner_chat_id": int(row["owner_chat_id"]),
            "login": row["email"],
            "email": row["email"],
            "full_name": row["full_name"],
            "role_name": row["role_name"],
            "password_state": row["password_state"],
            "password_hash": row["password_hash"],
            "is_active": bool(row["is_active"]),
            "is_super_admin": bool(row["is_super_admin"]),
            "created_at": row["created_at"],
            "permissions": self._full_access_permissions() if bool(row["is_super_admin"]) else self._permissions_payload(permissions),
        }

    def set_web_user_password(self, user_id: int, password_hash: str) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE web_users
                SET password_hash = ?, password_state = 'password_set'
                WHERE id = ?
                """,
                (password_hash, user_id),
            )
            return cursor.rowcount > 0

    def create_web_session(self, user_id: int, token: str) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO web_sessions (token, user_id, created_at)
                VALUES (?, ?, ?)
                """,
                (token, user_id, datetime.utcnow().isoformat()),
            )

    def delete_web_session(self, token: str) -> None:
        with self.connection() as conn:
            conn.execute("DELETE FROM web_sessions WHERE token = ?", (token,))

    def get_web_user_by_session(self, token: str) -> dict | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT u.id, u.owner_chat_id, u.email, u.full_name, u.role_name, u.password_state,
                       u.password_hash, u.is_active, u.is_super_admin, u.created_at
                FROM web_sessions s
                JOIN web_users u ON u.id = s.user_id
                WHERE s.token = ?
                LIMIT 1
                """,
                (token,),
            ).fetchone()
            if row is None:
                return None
            permission_rows = conn.execute(
                """
                SELECT section_id, can_view, can_edit
                FROM web_user_section_access
                WHERE user_id = ?
                ORDER BY section_id ASC
                """,
                (int(row["id"]),),
            ).fetchall()
        permissions = {
            item["section_id"]: {
                "can_view": bool(item["can_view"]),
                "can_edit": bool(item["can_edit"]),
            }
            for item in permission_rows
        }
        return {
            "id": int(row["id"]),
            "owner_chat_id": int(row["owner_chat_id"]),
            "login": row["email"],
            "email": row["email"],
            "full_name": row["full_name"],
            "role_name": row["role_name"],
            "password_state": row["password_state"],
            "password_hash": row["password_hash"],
            "is_active": bool(row["is_active"]),
            "is_super_admin": bool(row["is_super_admin"]),
            "created_at": row["created_at"],
            "permissions": self._full_access_permissions() if bool(row["is_super_admin"]) else self._permissions_payload(permissions),
        }

    def ensure_password_setup_token(self, user_id: int, token: str) -> str:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT token
                FROM web_password_setup_tokens
                WHERE user_id = ?
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()
            if row is not None:
                return row["token"]
            conn.execute(
                """
                INSERT INTO web_password_setup_tokens (token, user_id, created_at)
                VALUES (?, ?, ?)
                """,
                (token, user_id, datetime.utcnow().isoformat()),
            )
            return token

    def regenerate_password_setup_token(self, user_id: int, token: str) -> str:
        with self.connection() as conn:
            conn.execute("DELETE FROM web_password_setup_tokens WHERE user_id = ?", (user_id,))
            conn.execute(
                """
                INSERT INTO web_password_setup_tokens (token, user_id, created_at)
                VALUES (?, ?, ?)
                """,
                (token, user_id, datetime.utcnow().isoformat()),
            )
            return token

    def get_web_user_by_setup_token(self, token: str) -> dict | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT u.id, u.owner_chat_id, u.email, u.full_name, u.role_name, u.password_state,
                       u.password_hash, u.is_active, u.is_super_admin, u.created_at
                FROM web_password_setup_tokens t
                JOIN web_users u ON u.id = t.user_id
                WHERE t.token = ?
                LIMIT 1
                """,
                (token,),
            ).fetchone()
            if row is None:
                return None
        return self.get_web_user_by_id(int(row["id"]))

    def consume_password_setup_token(self, token: str) -> None:
        with self.connection() as conn:
            conn.execute("DELETE FROM web_password_setup_tokens WHERE token = ?", (token,))

    def list_auctions(self, owner_chat_id: int) -> list[Auction]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, owner_chat_id, auction_number, bid_deadline, amount, advance_percent, title, city, source_url, max_discount_percent, min_bid_amount, material_cost,
                       estimate_status, estimate_status_updated_at, submit_decision_status, application_status, result_status, final_bid_amount, archived_at, deleted_at, created_at
                FROM auctions
                WHERE owner_chat_id = ?
                ORDER BY bid_deadline ASC, id ASC
                """,
                (owner_chat_id,),
            ).fetchall()
        return [self._auction_from_row(row) for row in rows]

    def add_auction(
        self,
        owner_chat_id: int,
        auction_number: str,
        bid_deadline: date,
        amount: float,
        advance_percent: float | None,
        title: str,
        city: str,
        source_url: str,
    ) -> int:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO chats (chat_id, created_at)
                VALUES (?, ?)
                """,
                (owner_chat_id, datetime.utcnow().isoformat()),
            )
            self._ensure_default_web_admin(conn, owner_chat_id)
            cursor = conn.execute(
                """
                INSERT INTO auctions (
                    owner_chat_id, auction_number, bid_deadline, amount, advance_percent, title, city, source_url,
                    max_discount_percent, min_bid_amount, material_cost, estimate_status, submit_decision_status,
                    approval_status, application_status, result_status, final_bid_amount, created_at, archived_at, deleted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 'pending', 'pending', 'new', 'not_submitted', 'pending', NULL, ?, NULL, NULL)
                """,
                (
                    owner_chat_id,
                    auction_number.strip(),
                    bid_deadline.strftime(DATE_FMT),
                    amount,
                    advance_percent,
                    title.strip(),
                    city.strip(),
                    source_url.strip(),
                    datetime.utcnow().isoformat(),
                ),
            )
            return int(cursor.lastrowid)

    def update_auction_statuses(
        self,
        owner_chat_id: int,
        auction_id: int,
        estimate_status: str,
        material_cost: float | None,
        estimate_status_updated_at: datetime | None,
        submit_decision_status: str,
        application_status: str,
        result_status: str,
        final_bid_amount: float | None,
        archived_at: datetime | None,
    ) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE auctions
                SET estimate_status = ?, material_cost = ?, estimate_status_updated_at = ?, submit_decision_status = ?, application_status = ?, result_status = ?, final_bid_amount = ?, archived_at = ?
                WHERE id = ? AND owner_chat_id = ?
                """,
                (
                    estimate_status,
                    material_cost,
                    estimate_status_updated_at.isoformat() if estimate_status_updated_at is not None else None,
                    submit_decision_status,
                    application_status,
                    result_status,
                    final_bid_amount,
                    archived_at.isoformat() if archived_at is not None else None,
                    auction_id,
                    owner_chat_id,
                ),
            )
            return cursor.rowcount > 0

    def update_auction_max_discount(
        self,
        owner_chat_id: int,
        auction_id: int,
        max_discount_percent: float | None,
        min_bid_amount: float | None,
    ) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE auctions
                SET max_discount_percent = ?, min_bid_amount = ?
                WHERE id = ? AND owner_chat_id = ?
                """,
                (max_discount_percent, min_bid_amount, auction_id, owner_chat_id),
            )
            return cursor.rowcount > 0

    def update_auction_deadline(
        self,
        owner_chat_id: int,
        auction_id: int,
        bid_deadline: date,
    ) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE auctions
                SET bid_deadline = ?
                WHERE id = ? AND owner_chat_id = ?
                """,
                (bid_deadline.strftime(DATE_FMT), auction_id, owner_chat_id),
            )
            return cursor.rowcount > 0

    def update_auction_amount(
        self,
        owner_chat_id: int,
        auction_id: int,
        amount: float,
        advance_percent: float | None,
        max_discount_percent: float | None,
    ) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE auctions
                SET amount = ?, advance_percent = ?, max_discount_percent = ?
                WHERE id = ? AND owner_chat_id = ?
                """,
                (amount, advance_percent, max_discount_percent, auction_id, owner_chat_id),
            )
            return cursor.rowcount > 0

    def update_auction_details(
        self,
        owner_chat_id: int,
        auction_id: int,
        auction_number: str,
        title: str,
        city: str,
        source_url: str,
        created_date: date,
        advance_percent: float | None,
    ) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT created_at
                FROM auctions
                WHERE id = ? AND owner_chat_id = ?
                """,
                (auction_id, owner_chat_id),
            ).fetchone()
            if row is None:
                return False
            existing_created_at = datetime.fromisoformat(row["created_at"])
            updated_created_at = datetime.combine(created_date, existing_created_at.time() or time.min)
            cursor = conn.execute(
                """
                UPDATE auctions
                SET auction_number = ?, title = ?, city = ?, source_url = ?, created_at = ?, advance_percent = ?
                WHERE id = ? AND owner_chat_id = ?
                """,
                (
                    auction_number.strip(),
                    title.strip(),
                    city.strip(),
                    source_url.strip(),
                    updated_created_at.isoformat(),
                    advance_percent,
                    auction_id,
                    owner_chat_id,
                ),
            )
            return cursor.rowcount > 0

    def soft_delete_auction(self, owner_chat_id: int, auction_id: int, deleted_at: datetime) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE auctions
                SET deleted_at = ?
                WHERE id = ? AND owner_chat_id = ?
                """,
                (deleted_at.isoformat(), auction_id, owner_chat_id),
            )
            return cursor.rowcount > 0

    def hard_delete_auction(self, owner_chat_id: int, auction_id: int) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                DELETE FROM auctions
                WHERE id = ? AND owner_chat_id = ?
                """,
                (auction_id, owner_chat_id),
            )
            return cursor.rowcount > 0

    def hard_delete_all_deleted_auctions(self, owner_chat_id: int) -> int:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                DELETE FROM auctions
                WHERE owner_chat_id = ? AND deleted_at IS NOT NULL
                """,
                (owner_chat_id,),
            )
            return cursor.rowcount

    def ensure_demo_auctions(self, owner_chat_id: int) -> None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS total FROM auctions WHERE owner_chat_id = ?",
                (owner_chat_id,),
            ).fetchone()
            if int(row["total"]) > 0:
                return
            self._ensure_default_web_admin(conn, owner_chat_id)
            demo_items = [
                ("0145300000126000011", date(2026, 3, 27), 18_450_000.0, "Капремонт школы №8", "Хабаровск", "https://zakupki.example/auction/11", "approved", "pending", "not_submitted", "pending"),
                ("0320300027726000048", date(2026, 3, 29), 42_900_000.0, "Реконструкция стадиона Восток", "Владивосток", "https://zakupki.example/auction/48", "approved", "approved", "submitted", "pending"),
                ("0817200000326000005", date(2026, 4, 2), 11_780_000.0, "Благоустройство дворового пространства", "Уссурийск", "https://zakupki.example/auction/05", "rejected", "rejected", "not_submitted", "pending"),
                ("0258200000426000039", date(2026, 4, 5), 67_300_000.0, "Строительство ФОКа в пригороде", "Находка", "https://zakupki.example/auction/39", "approved", "approved", "submitted", "won"),
                ("0411100001126000021", date(2026, 4, 8), 23_650_000.0, "Ремонт городской поликлиники", "Артем", "https://zakupki.example/auction/21", "approved", "approved", "submitted", "lost"),
            ]
            for item in demo_items:
                conn.execute(
                    """
                    INSERT INTO auctions (
                        owner_chat_id, auction_number, bid_deadline, amount, advance_percent, title, city, source_url,
                        max_discount_percent, min_bid_amount, material_cost, estimate_status, submit_decision_status, approval_status, application_status, result_status, final_bid_amount, created_at, deleted_at
                    )
                    VALUES (?, ?, ?, ?, NULL, ?, ?, ?, NULL, NULL, NULL, ?, ?, 'new', ?, ?, NULL, ?, NULL)
                    """,
                    (
                        owner_chat_id,
                        item[0],
                        item[1].strftime(DATE_FMT),
                        item[2],
                        item[3],
                        item[4],
                        item[5],
                        item[6],
                        item[7],
                        item[8],
                        item[9],
                        datetime.utcnow().isoformat(),
                    ),
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

    def add_contract(self, chat_id: int, title: str, description: str, end_date: date, advance_percent: float | None = None) -> int:
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
                INSERT INTO contracts (chat_id, title, description, end_date, advance_percent, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (chat_id, title.strip(), description.strip(), end_date.strftime(DATE_FMT), advance_percent, datetime.utcnow().isoformat()),
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
                SELECT id, chat_id, title, description, end_date, advance_percent, created_at
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
                SELECT id, chat_id, title, description, end_date, advance_percent, created_at
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
            advance_percent=float(row["advance_percent"]) if row["advance_percent"] is not None else None,
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
    def _auction_from_row(row: sqlite3.Row) -> Auction:
        return Auction(
            id=row["id"],
            owner_chat_id=row["owner_chat_id"],
            auction_number=row["auction_number"],
            bid_deadline=date.fromisoformat(row["bid_deadline"]),
            amount=float(row["amount"]),
            advance_percent=float(row["advance_percent"]) if row["advance_percent"] is not None else None,
            title=row["title"],
            city=row["city"],
            source_url=row["source_url"],
            max_discount_percent=float(row["max_discount_percent"]) if row["max_discount_percent"] is not None else None,
            min_bid_amount=float(row["min_bid_amount"]) if row["min_bid_amount"] is not None else None,
            material_cost=float(row["material_cost"]) if row["material_cost"] is not None else None,
            estimate_status=row["estimate_status"],
            estimate_status_updated_at=datetime.fromisoformat(row["estimate_status_updated_at"]) if row["estimate_status_updated_at"] is not None else None,
            submit_decision_status=row["submit_decision_status"],
            application_status=row["application_status"],
            result_status=row["result_status"],
            final_bid_amount=float(row["final_bid_amount"]) if row["final_bid_amount"] is not None else None,
            archived_at=datetime.fromisoformat(row["archived_at"]) if row["archived_at"] is not None else None,
            deleted_at=datetime.fromisoformat(row["deleted_at"]) if row["deleted_at"] is not None else None,
            created_at=datetime.fromisoformat(row["created_at"]),
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

    def _ensure_default_web_admin(self, conn: sqlite3.Connection, owner_chat_id: int) -> int:
        row = conn.execute(
            """
            SELECT id
            FROM web_users
            WHERE owner_chat_id = ? AND is_super_admin = 1
            LIMIT 1
            """,
            (owner_chat_id,),
        ).fetchone()
        if row is not None:
            admin_id = int(row["id"])
            self._replace_web_user_permissions(conn, admin_id, self._full_access_permissions())
            return admin_id

        cursor = conn.execute(
            """
            INSERT INTO web_users (
                owner_chat_id, email, full_name, role_name, password_state,
                is_active, is_super_admin, created_at
            )
            VALUES (?, ?, 'BigBoss', 'BigBoss', 'local_only', 1, 1, ?)
            """,
            (
                owner_chat_id,
                f"bigboss-{owner_chat_id}@local.crm",
                datetime.utcnow().isoformat(),
            ),
        )
        admin_id = int(cursor.lastrowid)
        self._replace_web_user_permissions(conn, admin_id, self._full_access_permissions())
        return admin_id

    def _replace_web_user_permissions(
        self,
        conn: sqlite3.Connection,
        user_id: int,
        permissions: dict[str, dict[str, bool]],
    ) -> None:
        normalized = self._permissions_payload(permissions)
        conn.execute("DELETE FROM web_user_section_access WHERE user_id = ?", (user_id,))
        for section_id, values in normalized.items():
            conn.execute(
                """
                INSERT INTO web_user_section_access (user_id, section_id, can_view, can_edit)
                VALUES (?, ?, ?, ?)
                """,
                (
                    user_id,
                    section_id,
                    1 if values["can_view"] else 0,
                    1 if values["can_edit"] else 0,
                ),
            )

    def _permissions_payload(
        self,
        permissions: dict[str, dict[str, bool]] | None = None,
    ) -> dict[str, dict[str, bool]]:
        permissions = permissions or {}
        payload: dict[str, dict[str, bool]] = {}
        for section_id in WEB_SECTION_IDS:
            raw = permissions.get(section_id, {})
            can_edit = bool(raw.get("can_edit"))
            can_view = bool(raw.get("can_view")) or can_edit
            payload[section_id] = {"can_view": can_view, "can_edit": can_edit}
        return payload

    def _full_access_permissions(self) -> dict[str, dict[str, bool]]:
        return {
            section_id: {"can_view": True, "can_edit": True}
            for section_id in WEB_SECTION_IDS
        }
