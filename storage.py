from __future__ import annotations
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Iterator
from typing import Optional


DATE_FMT = "%Y-%m-%d"
WEB_SECTION_IDS = ("contracts", "auctions", "events", "tasks", "payables", "expenses", "payroll", "finance", "access")


@dataclass
class Contract:
    id: int
    chat_id: int
    title: str
    object_name: str
    object_address: str
    contract_number: str
    eis_url: str
    nmck_amount: float
    reduction_percent: float
    description: str
    signed_date: Optional[date]
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
    status_updated_at: Optional[datetime]
    status_updated_by_name: str
    payment_status: str
    payment_status_updated_at: Optional[datetime]
    payment_status_updated_by_name: str
    advance_invoice_issued: bool
    advance_invoice_issued_at: Optional[datetime]
    advance_invoice_issued_by_name: str
    final_invoice_issued: bool
    final_invoice_issued_at: Optional[datetime]
    final_invoice_issued_by_name: str
    notes: str
    start_date: Optional[date]
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
class LegalLetter:
    id: int
    contract_id: int
    direction: str
    letter_date: date
    subject: str
    comment: str
    file_name: str
    file_path: str
    created_by_user_id: Optional[int]
    created_by_name: str
    created_at: datetime
    contract_title: str
    chat_id: int


@dataclass
class LegalLetterAttachment:
    id: int
    letter_id: int
    contract_id: int
    file_name: str
    file_path: str
    created_at: datetime
    contract_title: str
    chat_id: int


@dataclass
class ContractEvent:
    id: int
    chat_id: int
    contract_id: int
    stage_id: Optional[int]
    event_date: date
    event_type: str
    source_kind: str
    source_ref: str
    title: str
    description: str
    actor_name: str
    created_at: datetime


@dataclass
class Auction:
    id: int
    owner_chat_id: int
    registry_position: int
    created_by_user_id: Optional[int]
    created_by_name: str
    auction_number: str
    bid_deadline: date
    amount: float
    advance_percent: Optional[float]
    title: str
    city: str
    source_url: str
    max_discount_percent: Optional[float]
    min_bid_amount: Optional[float]
    max_discount_updated_at: Optional[datetime]
    max_discount_updated_by_name: str
    material_cost: Optional[float]
    work_cost: Optional[float]
    other_cost: Optional[float]
    estimate_comment: str
    estimate_status: str
    estimate_status_updated_at: Optional[datetime]
    estimate_status_updated_by_name: str
    submit_decision_status: str
    submit_status_updated_at: Optional[datetime]
    submit_status_updated_by_name: str
    application_status: str
    result_status: str
    result_status_updated_at: Optional[datetime]
    result_status_updated_by_name: str
    final_bid_amount: Optional[float]
    archived_at: Optional[datetime]
    deleted_at: Optional[datetime]
    created_at: datetime


@dataclass
class AuctionEvent:
    id: int
    owner_chat_id: int
    auction_id: int
    event_date: date
    event_type: str
    source_kind: str
    source_ref: str
    title: str
    description: str
    actor_name: str
    created_at: datetime


@dataclass
class PayrollEmployee:
    id: int
    owner_chat_id: int
    full_name: str
    role_title: str
    is_active: bool
    created_at: datetime


@dataclass
class PayrollRow:
    employee_id: int
    owner_chat_id: int
    full_name: str
    role_title: str
    is_active: bool
    payroll_month: date
    accrued_amount: float
    advance_card_amount: float
    advance_card_paid_amount: float
    advance_card_paid_date: Optional[date]
    advance_cash_amount: float
    advance_cash_paid_amount: float
    advance_cash_paid_date: Optional[date]
    salary_amount: float
    salary_paid_amount: float
    salary_paid_date: Optional[date]
    bonus_amount: float
    bonus_paid_amount: float
    bonus_paid_date: Optional[date]
    note: str


@dataclass
class PayableEntry:
    id: int
    owner_chat_id: int
    counterparty: str
    document_ref: str
    document_date: Optional[date]
    object_name: str
    comment: str
    amount: float
    paid_amount: float
    paid_date: Optional[date]
    due_date: date
    created_by_user_id: Optional[int]
    created_by_name: str
    deleted_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


@dataclass
class TaskEntry:
    id: int
    owner_chat_id: int
    title: str
    description: str
    due_date: date
    assignee_kind: str
    assignee_user_id: Optional[int]
    assignee_name: str
    assignee_role_code: str
    assignee_role_name: str
    status: str
    completion_comment: str
    created_by_user_id: Optional[int]
    created_by_name: str
    created_at: datetime
    completed_at: Optional[datetime]
    completed_by_name: str
    deleted_at: Optional[datetime]


@dataclass
class TaskComment:
    id: int
    task_id: int
    comment_type: str
    body: str
    author_user_id: Optional[int]
    author_name: str
    created_at: datetime


@dataclass
class TaskAttachment:
    id: int
    task_id: int
    comment_id: Optional[int]
    file_name: str
    file_path: str
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
                    object_name TEXT NOT NULL DEFAULT '',
                    object_address TEXT NOT NULL DEFAULT '',
                    contract_number TEXT NOT NULL DEFAULT '',
                    eis_url TEXT NOT NULL DEFAULT '',
                    nmck_amount REAL NOT NULL DEFAULT 0,
                    reduction_percent REAL NOT NULL DEFAULT 0,
                    description TEXT NOT NULL DEFAULT '',
                    signed_date TEXT,
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
                    status_updated_at TEXT,
                    status_updated_by_name TEXT NOT NULL DEFAULT '',
                    payment_status TEXT NOT NULL DEFAULT 'unpaid',
                    payment_status_updated_at TEXT,
                    payment_status_updated_by_name TEXT NOT NULL DEFAULT '',
                    advance_invoice_issued INTEGER NOT NULL DEFAULT 0,
                    advance_invoice_issued_at TEXT,
                    advance_invoice_issued_by_name TEXT NOT NULL DEFAULT '',
                    final_invoice_issued INTEGER NOT NULL DEFAULT 0,
                    final_invoice_issued_at TEXT,
                    final_invoice_issued_by_name TEXT NOT NULL DEFAULT '',
                    notes TEXT NOT NULL DEFAULT '',
                    start_date TEXT,
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

                CREATE TABLE IF NOT EXISTS legal_letters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_id INTEGER NOT NULL,
                    direction TEXT NOT NULL DEFAULT 'outgoing',
                    letter_date TEXT NOT NULL,
                    subject TEXT NOT NULL DEFAULT '',
                    comment TEXT NOT NULL DEFAULT '',
                    file_name TEXT NOT NULL DEFAULT '',
                    file_path TEXT NOT NULL DEFAULT '',
                    created_by_user_id INTEGER,
                    created_by_name TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(contract_id) REFERENCES contracts(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS legal_letter_attachments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    letter_id INTEGER NOT NULL,
                    file_name TEXT NOT NULL DEFAULT '',
                    file_path TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(letter_id) REFERENCES legal_letters(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS contract_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    contract_id INTEGER NOT NULL,
                    stage_id INTEGER,
                    event_date TEXT NOT NULL,
                    event_type TEXT NOT NULL DEFAULT '',
                    source_kind TEXT NOT NULL DEFAULT '',
                    source_ref TEXT NOT NULL DEFAULT '',
                    title TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    actor_name TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(contract_id) REFERENCES contracts(id) ON DELETE CASCADE,
                    FOREIGN KEY(stage_id) REFERENCES stages(id) ON DELETE SET NULL
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

                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_chat_id INTEGER NOT NULL,
                    title TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    due_date TEXT NOT NULL,
                    assignee_kind TEXT NOT NULL DEFAULT 'user',
                    assignee_user_id INTEGER,
                    assignee_name TEXT NOT NULL DEFAULT '',
                    assignee_role_code TEXT NOT NULL DEFAULT '',
                    assignee_role_name TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'open',
                    completion_comment TEXT NOT NULL DEFAULT '',
                    created_by_user_id INTEGER,
                    created_by_name TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    completed_by_name TEXT NOT NULL DEFAULT '',
                    deleted_at TEXT,
                    FOREIGN KEY(assignee_user_id) REFERENCES web_users(id) ON DELETE SET NULL,
                    FOREIGN KEY(created_by_user_id) REFERENCES web_users(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS task_comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    comment_type TEXT NOT NULL DEFAULT 'comment',
                    body TEXT NOT NULL DEFAULT '',
                    author_user_id INTEGER,
                    author_name TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
                    FOREIGN KEY(author_user_id) REFERENCES web_users(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS task_attachments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    comment_id INTEGER,
                    file_name TEXT NOT NULL DEFAULT '',
                    file_path TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
                    FOREIGN KEY(comment_id) REFERENCES task_comments(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS auctions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_chat_id INTEGER NOT NULL,
                    registry_position INTEGER,
                    created_by_user_id INTEGER,
                    created_by_name TEXT NOT NULL DEFAULT '',
                    auction_number TEXT NOT NULL,
                    bid_deadline TEXT NOT NULL,
                    amount REAL NOT NULL DEFAULT 0,
                    advance_percent REAL,
                    title TEXT NOT NULL,
                    city TEXT NOT NULL DEFAULT '',
                    source_url TEXT NOT NULL DEFAULT '',
                    max_discount_percent REAL,
                    min_bid_amount REAL,
                    max_discount_updated_at TEXT,
                    max_discount_updated_by_name TEXT NOT NULL DEFAULT '',
                    material_cost REAL,
                    work_cost REAL,
                    other_cost REAL,
                    estimate_comment TEXT NOT NULL DEFAULT '',
                    estimate_status TEXT NOT NULL DEFAULT 'pending',
                    estimate_status_updated_at TEXT,
                    estimate_status_updated_by_name TEXT NOT NULL DEFAULT '',
                    submit_decision_status TEXT NOT NULL DEFAULT 'pending',
                    submit_status_updated_at TEXT,
                    submit_status_updated_by_name TEXT NOT NULL DEFAULT '',
                    approval_status TEXT NOT NULL DEFAULT 'new',
                    application_status TEXT NOT NULL DEFAULT 'not_submitted',
                    result_status TEXT NOT NULL DEFAULT 'pending',
                    result_status_updated_at TEXT,
                    result_status_updated_by_name TEXT NOT NULL DEFAULT '',
                    final_bid_amount REAL,
                    archived_at TEXT,
                    deleted_at TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS auction_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_chat_id INTEGER NOT NULL,
                    auction_id INTEGER NOT NULL,
                    event_date TEXT NOT NULL,
                    event_type TEXT NOT NULL DEFAULT '',
                    source_kind TEXT NOT NULL DEFAULT '',
                    source_ref TEXT NOT NULL DEFAULT '',
                    title TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    actor_name TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(auction_id) REFERENCES auctions(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS payroll_employees (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_chat_id INTEGER NOT NULL,
                    full_name TEXT NOT NULL,
                    role_title TEXT NOT NULL DEFAULT '',
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    UNIQUE(owner_chat_id, full_name, role_title)
                );

                CREATE TABLE IF NOT EXISTS payroll_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_chat_id INTEGER NOT NULL,
                    employee_id INTEGER NOT NULL,
                    payroll_month TEXT NOT NULL,
                    accrued_amount REAL NOT NULL DEFAULT 0,
                    advance_card_amount REAL NOT NULL DEFAULT 0,
                    advance_card_paid_amount REAL NOT NULL DEFAULT 0,
                    advance_card_paid_date TEXT,
                    advance_cash_amount REAL NOT NULL DEFAULT 0,
                    advance_cash_paid_amount REAL NOT NULL DEFAULT 0,
                    advance_cash_paid_date TEXT,
                    salary_amount REAL NOT NULL DEFAULT 0,
                    salary_paid_amount REAL NOT NULL DEFAULT 0,
                    salary_paid_date TEXT,
                    bonus_amount REAL NOT NULL DEFAULT 0,
                    bonus_paid_amount REAL NOT NULL DEFAULT 0,
                    bonus_paid_date TEXT,
                    note TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(owner_chat_id, employee_id, payroll_month),
                    FOREIGN KEY(employee_id) REFERENCES payroll_employees(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS payables (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_chat_id INTEGER NOT NULL,
                    counterparty TEXT NOT NULL,
                    document_ref TEXT NOT NULL DEFAULT '',
                    document_date TEXT,
                    object_name TEXT NOT NULL DEFAULT '',
                    comment TEXT NOT NULL DEFAULT '',
                    amount REAL NOT NULL DEFAULT 0,
                    paid_amount REAL NOT NULL DEFAULT 0,
                    paid_date TEXT,
                    due_date TEXT NOT NULL,
                    created_by_user_id INTEGER,
                    created_by_name TEXT NOT NULL DEFAULT '',
                    deleted_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(stages)").fetchall()
            }
            contract_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(contracts)").fetchall()
            }
            payroll_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(payroll_entries)").fetchall()
            }
            payable_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(payables)").fetchall()
            }
            task_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()
            }
            if "amount" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN amount REAL NOT NULL DEFAULT 0")
            if "position" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN position INTEGER NOT NULL DEFAULT 1")
            if "status" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN status TEXT NOT NULL DEFAULT 'not_started'")
            if "status_updated_at" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN status_updated_at TEXT")
            if "status_updated_by_name" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN status_updated_by_name TEXT NOT NULL DEFAULT ''")
            if "payment_status" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN payment_status TEXT NOT NULL DEFAULT 'unpaid'")
            if "payment_status_updated_at" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN payment_status_updated_at TEXT")
            if "payment_status_updated_by_name" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN payment_status_updated_by_name TEXT NOT NULL DEFAULT ''")
            if "advance_invoice_issued" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN advance_invoice_issued INTEGER NOT NULL DEFAULT 0")
            if "advance_invoice_issued_at" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN advance_invoice_issued_at TEXT")
            if "advance_invoice_issued_by_name" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN advance_invoice_issued_by_name TEXT NOT NULL DEFAULT ''")
            if "final_invoice_issued" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN final_invoice_issued INTEGER NOT NULL DEFAULT 0")
            if "final_invoice_issued_at" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN final_invoice_issued_at TEXT")
            if "final_invoice_issued_by_name" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN final_invoice_issued_by_name TEXT NOT NULL DEFAULT ''")
            if "start_date" not in columns:
                conn.execute("ALTER TABLE stages ADD COLUMN start_date TEXT")
            conn.execute(
                """
                UPDATE stages
                SET payment_status = CASE
                    WHEN status = 'paid' THEN 'paid'
                    ELSE COALESCE(payment_status, 'unpaid')
                END
                WHERE payment_status IS NULL OR payment_status = ''
                """
            )
            conn.execute(
                """
                UPDATE stages
                SET status = CASE
                    WHEN status IN ('waiting_payment', 'paid') THEN 'completed'
                    ELSE status
                END
                WHERE status IN ('waiting_payment', 'paid')
                """
            )
            conn.execute(
                """
                UPDATE stages
                SET status = 'accepted_eis'
                WHERE status = 'closed_iis'
                """
            )
            if "advance_percent" not in contract_columns:
                conn.execute("ALTER TABLE contracts ADD COLUMN advance_percent REAL")
            if "signed_date" not in contract_columns:
                conn.execute("ALTER TABLE contracts ADD COLUMN signed_date TEXT")
                conn.execute(
                    """
                    UPDATE contracts
                    SET signed_date = substr(created_at, 1, 10)
                    WHERE signed_date IS NULL OR signed_date = ''
                    """
                )
            if "contract_number" not in contract_columns:
                conn.execute("ALTER TABLE contracts ADD COLUMN contract_number TEXT NOT NULL DEFAULT ''")
            if "eis_url" not in contract_columns:
                conn.execute("ALTER TABLE contracts ADD COLUMN eis_url TEXT NOT NULL DEFAULT ''")
            if "nmck_amount" not in contract_columns:
                conn.execute("ALTER TABLE contracts ADD COLUMN nmck_amount REAL NOT NULL DEFAULT 0")
            if "reduction_percent" not in contract_columns:
                conn.execute("ALTER TABLE contracts ADD COLUMN reduction_percent REAL NOT NULL DEFAULT 0")
            if "object_name" not in contract_columns:
                conn.execute("ALTER TABLE contracts ADD COLUMN object_name TEXT NOT NULL DEFAULT ''")
                conn.execute("UPDATE contracts SET object_name = title WHERE object_name = ''")
            if "object_address" not in contract_columns:
                conn.execute("ALTER TABLE contracts ADD COLUMN object_address TEXT NOT NULL DEFAULT ''")
            payroll_alters = [
                ("advance_card_paid_amount", "REAL NOT NULL DEFAULT 0"),
                ("advance_card_paid_date", "TEXT"),
                ("advance_cash_paid_amount", "REAL NOT NULL DEFAULT 0"),
                ("advance_cash_paid_date", "TEXT"),
                ("salary_paid_amount", "REAL NOT NULL DEFAULT 0"),
                ("salary_paid_date", "TEXT"),
                ("bonus_paid_amount", "REAL NOT NULL DEFAULT 0"),
                ("bonus_paid_date", "TEXT"),
            ]
            for column_name, column_def in payroll_alters:
                if column_name not in payroll_columns:
                    conn.execute(f"ALTER TABLE payroll_entries ADD COLUMN {column_name} {column_def}")
            if "deleted_at" not in payable_columns:
                conn.execute("ALTER TABLE payables ADD COLUMN deleted_at TEXT")
            if "assignee_kind" not in task_columns:
                conn.execute("ALTER TABLE tasks ADD COLUMN assignee_kind TEXT NOT NULL DEFAULT 'user'")
            if "assignee_role_code" not in task_columns:
                conn.execute("ALTER TABLE tasks ADD COLUMN assignee_role_code TEXT NOT NULL DEFAULT ''")
            if "deleted_at" not in task_columns:
                conn.execute("ALTER TABLE tasks ADD COLUMN deleted_at TEXT")
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
            if "estimate_status_updated_by_name" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN estimate_status_updated_by_name TEXT NOT NULL DEFAULT ''")
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
            if "submit_status_updated_at" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN submit_status_updated_at TEXT")
            if "submit_status_updated_by_name" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN submit_status_updated_by_name TEXT NOT NULL DEFAULT ''")
            if "max_discount_percent" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN max_discount_percent REAL")
            if "min_bid_amount" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN min_bid_amount REAL")
            if "max_discount_updated_at" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN max_discount_updated_at TEXT")
            if "max_discount_updated_by_name" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN max_discount_updated_by_name TEXT NOT NULL DEFAULT ''")
            if "material_cost" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN material_cost REAL")
            if "work_cost" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN work_cost REAL")
            if "other_cost" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN other_cost REAL")
            if "estimate_comment" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN estimate_comment TEXT NOT NULL DEFAULT ''")
            if "final_bid_amount" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN final_bid_amount REAL")
            if "result_status_updated_at" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN result_status_updated_at TEXT")
            if "result_status_updated_by_name" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN result_status_updated_by_name TEXT NOT NULL DEFAULT ''")
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
                    WHERE submit_decision_status = 'rejected' OR result_status IN ('recognized_winner', 'lost', 'rejected')
                    """
                )
            if "registry_position" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN registry_position INTEGER")
            if "created_by_user_id" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN created_by_user_id INTEGER")
            if "created_by_name" not in auction_columns:
                conn.execute("ALTER TABLE auctions ADD COLUMN created_by_name TEXT NOT NULL DEFAULT ''")
            self._backfill_auction_registry_positions(conn)
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
                    full_name.strip() or ("Админ" if is_super_admin else "Пользователь"),
                    role_name.strip() or ("Админ" if is_super_admin else "Viewer"),
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
                SELECT id, owner_chat_id, registry_position, created_by_user_id, created_by_name, auction_number, bid_deadline, amount, advance_percent, title, city, source_url, max_discount_percent, min_bid_amount, max_discount_updated_at, max_discount_updated_by_name, material_cost, work_cost, other_cost, estimate_comment,
                       estimate_status, estimate_status_updated_at, estimate_status_updated_by_name, submit_decision_status, submit_status_updated_at, submit_status_updated_by_name,
                       application_status, result_status, result_status_updated_at, result_status_updated_by_name, final_bid_amount, archived_at, deleted_at, created_at
                FROM auctions
                WHERE owner_chat_id = ?
                ORDER BY registry_position ASC, id ASC
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
        created_by_user_id: int | None,
        created_by_name: str,
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
            registry_position = self._next_auction_registry_position(conn, owner_chat_id)
            cursor = conn.execute(
                """
                INSERT INTO auctions (
                    owner_chat_id, registry_position, created_by_user_id, created_by_name, auction_number, bid_deadline, amount, advance_percent, title, city, source_url,
                    max_discount_percent, min_bid_amount, material_cost, work_cost, other_cost, estimate_status, submit_decision_status,
                    approval_status, application_status, result_status, final_bid_amount, created_at, archived_at, deleted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, 'pending', 'pending', 'new', 'not_submitted', 'pending', NULL, ?, NULL, NULL)
                """,
                (
                    owner_chat_id,
                    registry_position,
                    created_by_user_id,
                    created_by_name.strip(),
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
        work_cost: float | None,
        other_cost: float | None,
        estimate_comment: str,
        estimate_status_updated_at: datetime | None,
        estimate_status_updated_by_name: str,
        submit_decision_status: str,
        submit_status_updated_at: datetime | None,
        submit_status_updated_by_name: str,
        application_status: str,
        result_status: str,
        result_status_updated_at: datetime | None,
        result_status_updated_by_name: str,
        final_bid_amount: float | None,
        archived_at: datetime | None,
    ) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE auctions
                SET estimate_status = ?, material_cost = ?, work_cost = ?, other_cost = ?, estimate_comment = ?, estimate_status_updated_at = ?, estimate_status_updated_by_name = ?,
                    submit_decision_status = ?, submit_status_updated_at = ?, submit_status_updated_by_name = ?,
                    application_status = ?, result_status = ?, result_status_updated_at = ?, result_status_updated_by_name = ?,
                    final_bid_amount = ?, archived_at = ?
                WHERE id = ? AND owner_chat_id = ?
                """,
                (
                    estimate_status,
                    material_cost,
                    work_cost,
                    other_cost,
                    estimate_comment,
                    estimate_status_updated_at.isoformat() if estimate_status_updated_at is not None else None,
                    estimate_status_updated_by_name,
                    submit_decision_status,
                    submit_status_updated_at.isoformat() if submit_status_updated_at is not None else None,
                    submit_status_updated_by_name,
                    application_status,
                    result_status,
                    result_status_updated_at.isoformat() if result_status_updated_at is not None else None,
                    result_status_updated_by_name,
                    final_bid_amount,
                    archived_at.isoformat() if archived_at is not None else None,
                    auction_id,
                    owner_chat_id,
                ),
            )
            return cursor.rowcount > 0

    def add_auction_event(
        self,
        owner_chat_id: int,
        auction_id: int,
        event_date: date,
        event_type: str,
        title: str,
        description: str = "",
        actor_name: str = "",
        source_kind: str = "",
        source_ref: str = "",
    ) -> int | None:
        with self.connection() as conn:
            auction = conn.execute(
                """
                SELECT id
                FROM auctions
                WHERE id = ? AND owner_chat_id = ?
                """,
                (auction_id, owner_chat_id),
            ).fetchone()
            if auction is None:
                return None
            cursor = conn.execute(
                """
                INSERT INTO auction_events (
                    owner_chat_id, auction_id, event_date, event_type,
                    source_kind, source_ref, title, description, actor_name, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    owner_chat_id,
                    auction_id,
                    event_date.strftime(DATE_FMT),
                    event_type.strip(),
                    source_kind.strip(),
                    source_ref.strip(),
                    title.strip(),
                    description.strip(),
                    actor_name.strip(),
                    datetime.utcnow().isoformat(),
                ),
            )
            return int(cursor.lastrowid)

    def update_auction_max_discount(
        self,
        owner_chat_id: int,
        auction_id: int,
        max_discount_percent: float | None,
        min_bid_amount: float | None,
        updated_at: datetime | None,
        updated_by_name: str,
    ) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE auctions
                SET max_discount_percent = ?, min_bid_amount = ?, max_discount_updated_at = ?, max_discount_updated_by_name = ?
                WHERE id = ? AND owner_chat_id = ?
                """,
                (
                    max_discount_percent,
                    min_bid_amount,
                    updated_at.isoformat() if updated_at is not None else None,
                    updated_by_name,
                    auction_id,
                    owner_chat_id,
                ),
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

    def list_auction_events(self, owner_chat_id: int, auction_id: int) -> list[AuctionEvent]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, owner_chat_id, auction_id, event_date, event_type,
                       source_kind, source_ref, title, description, actor_name, created_at
                FROM auction_events
                WHERE owner_chat_id = ? AND auction_id = ?
                ORDER BY event_date DESC, created_at DESC, id DESC
                """,
                (owner_chat_id, auction_id),
            ).fetchall()
        return [self._auction_event_from_row(row) for row in rows]

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

    def restore_deleted_auction(self, owner_chat_id: int, auction_id: int) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE auctions
                SET deleted_at = NULL
                WHERE id = ? AND owner_chat_id = ? AND deleted_at IS NOT NULL
                """,
                (auction_id, owner_chat_id),
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
                        owner_chat_id, registry_position, created_by_user_id, created_by_name, auction_number, bid_deadline, amount, advance_percent, title, city, source_url,
                        max_discount_percent, min_bid_amount, material_cost, estimate_status, submit_decision_status, approval_status, application_status, result_status, final_bid_amount, created_at, deleted_at
                    )
                    VALUES (?, ?, NULL, 'Система', ?, ?, ?, NULL, ?, ?, ?, NULL, NULL, NULL, ?, ?, 'new', ?, ?, NULL, ?, NULL)
                    """,
                    (
                        owner_chat_id,
                        self._next_auction_registry_position(conn, owner_chat_id),
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

    def add_contract(
        self,
        chat_id: int,
        object_name: str,
        object_address: str,
        contract_number: str,
        eis_url: str,
        nmck_amount: float,
        reduction_percent: float,
        description: str,
        signed_date: date | None,
        end_date: date,
        advance_percent: float | None = None,
    ) -> int:
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
                INSERT INTO contracts (chat_id, title, object_name, object_address, contract_number, eis_url, nmck_amount, reduction_percent, description, signed_date, end_date, advance_percent, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    f"{object_name.strip()}, {object_address.strip()}" if object_address.strip() else object_name.strip(),
                    object_name.strip(),
                    object_address.strip(),
                    contract_number.strip(),
                    eis_url.strip(),
                    nmck_amount,
                    reduction_percent,
                    description.strip(),
                    signed_date.strftime(DATE_FMT) if signed_date is not None else None,
                    end_date.strftime(DATE_FMT),
                    advance_percent,
                    datetime.utcnow().isoformat(),
                ),
            )
            return int(cursor.lastrowid)

    def update_contract_identity(self, chat_id: int, contract_id: int, contract_number: str, eis_url: str, nmck_amount: float, reduction_percent: float) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE contracts
                SET contract_number = ?, eis_url = ?, nmck_amount = ?, reduction_percent = ?
                WHERE id = ? AND chat_id = ?
                """,
                (contract_number.strip(), eis_url.strip(), nmck_amount, reduction_percent, contract_id, chat_id),
            )
            return cursor.rowcount > 0

    def add_stage(self, contract_id: int, position: int, notes: str, start_date: date | None, end_date: date, amount: float) -> int:
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
                INSERT INTO stages (
                    contract_id, position, name, status, status_updated_at, status_updated_by_name,
                    payment_status, payment_status_updated_at, payment_status_updated_by_name,
                    advance_invoice_issued, advance_invoice_issued_at, advance_invoice_issued_by_name,
                    final_invoice_issued, final_invoice_issued_at, final_invoice_issued_by_name,
                    notes, start_date, end_date, amount, created_at
                )
                VALUES (?, ?, ?, 'not_started', NULL, '', 'unpaid', NULL, '', 0, NULL, '', 0, NULL, '', ?, ?, ?, ?, ?)
                """,
                (
                    contract_id,
                    normalized_position,
                    f"Этап {normalized_position}",
                    notes.strip(),
                    start_date.strftime(DATE_FMT) if start_date is not None else None,
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
                SELECT s.id, s.contract_id, s.position, s.name, s.status, s.status_updated_at, s.status_updated_by_name,
                       s.payment_status, s.payment_status_updated_at, s.payment_status_updated_by_name,
                       s.advance_invoice_issued, s.advance_invoice_issued_at, s.advance_invoice_issued_by_name,
                       s.final_invoice_issued, s.final_invoice_issued_at, s.final_invoice_issued_by_name,
                       s.notes, s.start_date, s.end_date, s.amount, s.created_at,
                       c.title AS contract_title, c.chat_id AS chat_id
                FROM stages s
                JOIN contracts c ON c.id = s.contract_id
                WHERE c.chat_id = ? AND s.id = ?
                """,
                (chat_id, stage_id),
            ).fetchone()
        return self._stage_from_row(row) if row else None

    def update_stage(self, chat_id: int, stage_id: int, name: str, notes: str, start_date: date | None, end_date: date, amount: float) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE stages
                SET name = ?, notes = ?, start_date = ?, end_date = ?, amount = ?
                WHERE id = ?
                  AND contract_id IN (
                      SELECT id FROM contracts WHERE chat_id = ?
                  )
                """,
                (
                    name.strip(),
                    notes.strip(),
                    start_date.strftime(DATE_FMT) if start_date is not None else None,
                    end_date.strftime(DATE_FMT),
                    amount,
                    stage_id,
                    chat_id,
                ),
            )
            return cursor.rowcount > 0

    def update_stage_status(
        self,
        chat_id: int,
        stage_id: int,
        status: str,
        status_updated_at: datetime | None,
        status_updated_by_name: str,
    ) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE stages
                SET status = ?, status_updated_at = ?, status_updated_by_name = ?
                WHERE id = ?
                  AND contract_id IN (
                      SELECT id FROM contracts WHERE chat_id = ?
                  )
                """,
                (
                    status,
                    status_updated_at.isoformat() if status_updated_at is not None else None,
                    status_updated_by_name,
                    stage_id,
                    chat_id,
                ),
            )
            return cursor.rowcount > 0

    def update_stage_payment_status(
        self,
        chat_id: int,
        stage_id: int,
        payment_status: str,
        payment_status_updated_at: datetime | None,
        payment_status_updated_by_name: str,
    ) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE stages
                SET payment_status = ?, payment_status_updated_at = ?, payment_status_updated_by_name = ?
                WHERE id = ?
                  AND contract_id IN (
                      SELECT id FROM contracts WHERE chat_id = ?
                  )
                """,
                (
                    payment_status,
                    payment_status_updated_at.isoformat() if payment_status_updated_at is not None else None,
                    payment_status_updated_by_name,
                    stage_id,
                    chat_id,
                ),
            )
            return cursor.rowcount > 0

    def update_stage_invoice_status(
        self,
        chat_id: int,
        stage_id: int,
        invoice_kind: str,
        issued: bool,
        issued_at: datetime | None,
        issued_by_name: str,
    ) -> bool:
        if invoice_kind == "advance":
            issued_column = "advance_invoice_issued"
            at_column = "advance_invoice_issued_at"
            by_column = "advance_invoice_issued_by_name"
        elif invoice_kind == "final":
            issued_column = "final_invoice_issued"
            at_column = "final_invoice_issued_at"
            by_column = "final_invoice_issued_by_name"
        else:
            return False
        with self.connection() as conn:
            cursor = conn.execute(
                f"""
                UPDATE stages
                SET {issued_column} = ?, {at_column} = ?, {by_column} = ?
                WHERE id = ?
                  AND contract_id IN (
                      SELECT id FROM contracts WHERE chat_id = ?
                  )
                """,
                (
                    1 if issued else 0,
                    issued_at.isoformat() if issued and issued_at is not None else None,
                    issued_by_name if issued else "",
                    stage_id,
                    chat_id,
                ),
            )
            return cursor.rowcount > 0

    def update_stage_deadline(self, chat_id: int, stage_id: int, start_date: date | None, end_date: date) -> bool:
        with self.connection() as conn:
            stage = conn.execute(
                """
                SELECT s.contract_id
                FROM stages s
                JOIN contracts c ON c.id = s.contract_id
                WHERE s.id = ? AND c.chat_id = ?
                """,
                (stage_id, chat_id),
            ).fetchone()
            if stage is None:
                return False
            cursor = conn.execute(
                """
                UPDATE stages
                SET start_date = ?, end_date = ?
                WHERE id = ?
                  AND contract_id IN (
                      SELECT id FROM contracts WHERE chat_id = ?
                  )
                """,
                (
                    start_date.strftime(DATE_FMT) if start_date is not None else None,
                    end_date.strftime(DATE_FMT),
                    stage_id,
                    chat_id,
                ),
            )
            if cursor.rowcount > 0:
                max_stage_date = conn.execute(
                    "SELECT MAX(end_date) AS max_end_date FROM stages WHERE contract_id = ?",
                    (stage["contract_id"],),
                ).fetchone()["max_end_date"]
                if max_stage_date is not None:
                    conn.execute(
                        "UPDATE contracts SET end_date = ? WHERE id = ? AND chat_id = ?",
                        (max_stage_date, stage["contract_id"], chat_id),
                    )
            return cursor.rowcount > 0

    def update_stage_amount(self, chat_id: int, stage_id: int, amount: float) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE stages
                SET amount = ?
                WHERE id = ?
                  AND contract_id IN (
                      SELECT id FROM contracts WHERE chat_id = ?
                  )
                """,
                (amount, stage_id, chat_id),
            )
            return cursor.rowcount > 0

    def update_contract_advance_percent(self, chat_id: int, contract_id: int, advance_percent: float | None) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE contracts
                SET advance_percent = ?
                WHERE id = ? AND chat_id = ?
                """,
                (advance_percent, contract_id, chat_id),
            )
            return cursor.rowcount > 0

    def update_contract_signed_date(self, chat_id: int, contract_id: int, signed_date: date | None) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE contracts
                SET signed_date = ?
                WHERE id = ? AND chat_id = ?
                """,
                (signed_date.strftime(DATE_FMT) if signed_date is not None else None, contract_id, chat_id),
            )
            return cursor.rowcount > 0

    def update_contract_main_info(self, chat_id: int, contract_id: int, object_name: str, object_address: str, description: str) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE contracts
                SET title = ?, object_name = ?, object_address = ?, description = ?
                WHERE id = ? AND chat_id = ?
                """,
                (
                    f"{object_name.strip()}, {object_address.strip()}" if object_address.strip() else object_name.strip(),
                    object_name.strip(),
                    object_address.strip(),
                    description.strip(),
                    contract_id,
                    chat_id,
                ),
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
                    INSERT INTO stages (
                        contract_id, position, name, status, status_updated_at, status_updated_by_name,
                        payment_status, payment_status_updated_at, payment_status_updated_by_name,
                        advance_invoice_issued, advance_invoice_issued_at, advance_invoice_issued_by_name,
                        final_invoice_issued, final_invoice_issued_at, final_invoice_issued_by_name,
                        notes, start_date, end_date, amount, created_at
                    )
                    VALUES (?, ?, ?, 'not_started', NULL, '', 'unpaid', NULL, '', 0, NULL, '', 0, NULL, '', '', ?, ?, ?, ?)
                    """,
                    (
                        contract_id,
                        index,
                        f"Этап {index}",
                        item.get("start_date").strftime(DATE_FMT) if item.get("start_date") is not None else None,
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

    def add_contract_event(
        self,
        chat_id: int,
        contract_id: int,
        event_date: date,
        event_type: str,
        title: str,
        description: str = "",
        actor_name: str = "",
        stage_id: int | None = None,
        source_kind: str = "",
        source_ref: str = "",
    ) -> int | None:
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
                INSERT INTO contract_events (
                    chat_id, contract_id, stage_id, event_date, event_type,
                    source_kind, source_ref, title, description, actor_name, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    contract_id,
                    stage_id,
                    event_date.strftime(DATE_FMT),
                    event_type.strip(),
                    source_kind.strip(),
                    source_ref.strip(),
                    title.strip(),
                    description.strip(),
                    actor_name.strip(),
                    datetime.utcnow().isoformat(),
                ),
            )
            return int(cursor.lastrowid)

    def add_legal_letter(
        self,
        chat_id: int,
        contract_id: int,
        direction: str,
        letter_date: date,
        subject: str,
        comment: str,
        file_name: str,
        file_path: str,
        created_by_user_id: int | None,
        created_by_name: str,
    ) -> int | None:
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
                INSERT INTO legal_letters (
                    contract_id, direction, letter_date, subject, comment,
                    file_name, file_path, created_by_user_id, created_by_name, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    contract_id,
                    direction,
                    letter_date.strftime(DATE_FMT),
                    subject.strip(),
                    comment.strip(),
                    file_name.strip(),
                    file_path.strip(),
                    created_by_user_id,
                    created_by_name.strip(),
                    datetime.utcnow().isoformat(),
                ),
            )
            return int(cursor.lastrowid)

    def add_legal_letter_attachment(self, chat_id: int, letter_id: int, file_name: str, file_path: str) -> int | None:
        with self.connection() as conn:
            letter = conn.execute(
                """
                SELECT l.id
                FROM legal_letters l
                JOIN contracts c ON c.id = l.contract_id
                WHERE l.id = ? AND c.chat_id = ?
                """,
                (letter_id, chat_id),
            ).fetchone()
            if letter is None:
                return None
            cursor = conn.execute(
                """
                INSERT INTO legal_letter_attachments (letter_id, file_name, file_path, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (letter_id, file_name.strip(), file_path.strip(), datetime.utcnow().isoformat()),
            )
            return int(cursor.lastrowid)

    def update_legal_letter(
        self,
        chat_id: int,
        letter_id: int,
        direction: str,
        letter_date: date,
        subject: str,
        comment: str,
    ) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE legal_letters
                SET direction = ?, letter_date = ?, subject = ?, comment = ?
                WHERE id = ?
                  AND contract_id IN (
                      SELECT id FROM contracts WHERE chat_id = ?
                  )
                """,
                (
                    direction,
                    letter_date.strftime(DATE_FMT),
                    subject.strip(),
                    comment.strip(),
                    letter_id,
                    chat_id,
                ),
            )
            return cursor.rowcount > 0

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

    def get_legal_letter(self, chat_id: int, letter_id: int) -> LegalLetter | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT l.id, l.contract_id, l.direction, l.letter_date, l.subject, l.comment,
                       l.file_name, l.file_path, l.created_by_user_id, l.created_by_name, l.created_at,
                       c.title AS contract_title, c.chat_id AS chat_id
                FROM legal_letters l
                JOIN contracts c ON c.id = l.contract_id
                WHERE c.chat_id = ? AND l.id = ?
                """,
                (chat_id, letter_id),
            ).fetchone()
        return self._legal_letter_from_row(row) if row else None

    def get_legal_letter_attachment(self, chat_id: int, attachment_id: int) -> LegalLetterAttachment | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT a.id, a.letter_id, l.contract_id, a.file_name, a.file_path, a.created_at,
                       c.title AS contract_title, c.chat_id AS chat_id
                FROM legal_letter_attachments a
                JOIN legal_letters l ON l.id = a.letter_id
                JOIN contracts c ON c.id = l.contract_id
                WHERE a.id = ? AND c.chat_id = ?
                """,
                (attachment_id, chat_id),
            ).fetchone()
        return self._legal_letter_attachment_from_row(row) if row else None

    def delete_legal_letter_attachment(self, chat_id: int, attachment_id: int) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                DELETE FROM legal_letter_attachments
                WHERE id = ?
                  AND letter_id IN (
                      SELECT l.id
                      FROM legal_letters l
                      JOIN contracts c ON c.id = l.contract_id
                      WHERE c.chat_id = ?
                  )
                """,
                (attachment_id, chat_id),
            )
            return cursor.rowcount > 0

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

    def list_legal_letters_for_contract(self, chat_id: int, contract_id: int) -> list[LegalLetter]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT l.id, l.contract_id, l.direction, l.letter_date, l.subject, l.comment,
                       l.file_name, l.file_path, l.created_by_user_id, l.created_by_name, l.created_at,
                       c.title AS contract_title, c.chat_id AS chat_id
                FROM legal_letters l
                JOIN contracts c ON c.id = l.contract_id
                WHERE c.chat_id = ? AND c.id = ?
                ORDER BY l.letter_date DESC, l.id DESC
                """,
                (chat_id, contract_id),
            ).fetchall()
        return [self._legal_letter_from_row(row) for row in rows]

    def list_legal_letter_attachments_for_contract(self, chat_id: int, contract_id: int) -> list[LegalLetterAttachment]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT a.id, a.letter_id, l.contract_id, a.file_name, a.file_path, a.created_at,
                       c.title AS contract_title, c.chat_id AS chat_id
                FROM legal_letter_attachments a
                JOIN legal_letters l ON l.id = a.letter_id
                JOIN contracts c ON c.id = l.contract_id
                WHERE c.chat_id = ? AND c.id = ?
                ORDER BY a.id ASC
                """,
                (chat_id, contract_id),
            ).fetchall()
        return [self._legal_letter_attachment_from_row(row) for row in rows]

    def list_contract_events(self, chat_id: int, contract_id: int) -> list[ContractEvent]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, chat_id, contract_id, stage_id, event_date, event_type,
                       source_kind, source_ref, title, description, actor_name, created_at
                FROM contract_events
                WHERE chat_id = ? AND contract_id = ?
                ORDER BY event_date DESC, created_at DESC, id DESC
                """,
                (chat_id, contract_id),
            ).fetchall()
        return [self._contract_event_from_row(row) for row in rows]

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
                SELECT id, chat_id, title, object_name, object_address, contract_number, eis_url, nmck_amount, reduction_percent, description, signed_date, end_date, advance_percent, created_at
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
                SELECT id, chat_id, title, object_name, object_address, contract_number, eis_url, nmck_amount, reduction_percent, description, signed_date, end_date, advance_percent, created_at
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
                SELECT s.id, s.contract_id, s.position, s.name, s.status, s.status_updated_at, s.status_updated_by_name,
                       s.payment_status, s.payment_status_updated_at, s.payment_status_updated_by_name,
                       s.advance_invoice_issued, s.advance_invoice_issued_at, s.advance_invoice_issued_by_name,
                       s.final_invoice_issued, s.final_invoice_issued_at, s.final_invoice_issued_by_name,
                       s.notes, s.start_date, s.end_date, s.amount, s.created_at,
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
            object_name=row["object_name"] or row["title"] or "",
            object_address=row["object_address"] or "",
            contract_number=row["contract_number"] or "",
            eis_url=row["eis_url"] or "",
            nmck_amount=float(row["nmck_amount"]) if row["nmck_amount"] is not None else 0.0,
            reduction_percent=float(row["reduction_percent"]) if row["reduction_percent"] is not None else 0.0,
            description=row["description"],
            signed_date=date.fromisoformat(row["signed_date"]) if row["signed_date"] is not None and row["signed_date"] != "" else None,
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
            status_updated_at=datetime.fromisoformat(row["status_updated_at"]) if row["status_updated_at"] is not None else None,
            status_updated_by_name=row["status_updated_by_name"] or "",
            payment_status=row["payment_status"] or "unpaid",
            payment_status_updated_at=datetime.fromisoformat(row["payment_status_updated_at"]) if row["payment_status_updated_at"] is not None else None,
            payment_status_updated_by_name=row["payment_status_updated_by_name"] or "",
            advance_invoice_issued=bool(row["advance_invoice_issued"]),
            advance_invoice_issued_at=datetime.fromisoformat(row["advance_invoice_issued_at"]) if row["advance_invoice_issued_at"] is not None else None,
            advance_invoice_issued_by_name=row["advance_invoice_issued_by_name"] or "",
            final_invoice_issued=bool(row["final_invoice_issued"]),
            final_invoice_issued_at=datetime.fromisoformat(row["final_invoice_issued_at"]) if row["final_invoice_issued_at"] is not None else None,
            final_invoice_issued_by_name=row["final_invoice_issued_by_name"] or "",
            notes=row["notes"],
            start_date=date.fromisoformat(row["start_date"]) if row["start_date"] is not None else None,
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
    def _legal_letter_from_row(row: sqlite3.Row) -> LegalLetter:
        return LegalLetter(
            id=row["id"],
            contract_id=row["contract_id"],
            direction=row["direction"] or "outgoing",
            letter_date=date.fromisoformat(row["letter_date"]),
            subject=row["subject"] or "",
            comment=row["comment"] or "",
            file_name=row["file_name"] or "",
            file_path=row["file_path"] or "",
            created_by_user_id=int(row["created_by_user_id"]) if row["created_by_user_id"] is not None else None,
            created_by_name=row["created_by_name"] or "",
            created_at=datetime.fromisoformat(row["created_at"]),
            contract_title=row["contract_title"],
            chat_id=row["chat_id"],
        )

    @staticmethod
    def _legal_letter_attachment_from_row(row: sqlite3.Row) -> LegalLetterAttachment:
        return LegalLetterAttachment(
            id=row["id"],
            letter_id=row["letter_id"],
            contract_id=row["contract_id"],
            file_name=row["file_name"] or "",
            file_path=row["file_path"] or "",
            created_at=datetime.fromisoformat(row["created_at"]),
            contract_title=row["contract_title"],
            chat_id=row["chat_id"],
        )

    @staticmethod
    def _contract_event_from_row(row: sqlite3.Row) -> ContractEvent:
        return ContractEvent(
            id=row["id"],
            chat_id=row["chat_id"],
            contract_id=row["contract_id"],
            stage_id=int(row["stage_id"]) if row["stage_id"] is not None else None,
            event_date=date.fromisoformat(row["event_date"]),
            event_type=row["event_type"] or "",
            source_kind=row["source_kind"] or "",
            source_ref=row["source_ref"] or "",
            title=row["title"] or "",
            description=row["description"] or "",
            actor_name=row["actor_name"] or "",
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _auction_from_row(row: sqlite3.Row) -> Auction:
        return Auction(
            id=row["id"],
            owner_chat_id=row["owner_chat_id"],
            registry_position=int(row["registry_position"]) if row["registry_position"] is not None else int(row["id"]),
            created_by_user_id=int(row["created_by_user_id"]) if row["created_by_user_id"] is not None else None,
            created_by_name=row["created_by_name"] or "",
            auction_number=row["auction_number"],
            bid_deadline=date.fromisoformat(row["bid_deadline"]),
            amount=float(row["amount"]),
            advance_percent=float(row["advance_percent"]) if row["advance_percent"] is not None else None,
            title=row["title"],
            city=row["city"],
            source_url=row["source_url"],
            max_discount_percent=float(row["max_discount_percent"]) if row["max_discount_percent"] is not None else None,
            min_bid_amount=float(row["min_bid_amount"]) if row["min_bid_amount"] is not None else None,
            max_discount_updated_at=datetime.fromisoformat(row["max_discount_updated_at"]) if row["max_discount_updated_at"] is not None else None,
            max_discount_updated_by_name=row["max_discount_updated_by_name"] or "",
            material_cost=float(row["material_cost"]) if row["material_cost"] is not None else None,
            work_cost=float(row["work_cost"]) if row["work_cost"] is not None else None,
            other_cost=float(row["other_cost"]) if row["other_cost"] is not None else None,
            estimate_comment=row["estimate_comment"] or "",
            estimate_status=row["estimate_status"],
            estimate_status_updated_at=datetime.fromisoformat(row["estimate_status_updated_at"]) if row["estimate_status_updated_at"] is not None else None,
            estimate_status_updated_by_name=row["estimate_status_updated_by_name"] or "",
            submit_decision_status=row["submit_decision_status"],
            submit_status_updated_at=datetime.fromisoformat(row["submit_status_updated_at"]) if row["submit_status_updated_at"] is not None else None,
            submit_status_updated_by_name=row["submit_status_updated_by_name"] or "",
            application_status=row["application_status"],
            result_status=row["result_status"],
            result_status_updated_at=datetime.fromisoformat(row["result_status_updated_at"]) if row["result_status_updated_at"] is not None else None,
            result_status_updated_by_name=row["result_status_updated_by_name"] or "",
            final_bid_amount=float(row["final_bid_amount"]) if row["final_bid_amount"] is not None else None,
            archived_at=datetime.fromisoformat(row["archived_at"]) if row["archived_at"] is not None else None,
            deleted_at=datetime.fromisoformat(row["deleted_at"]) if row["deleted_at"] is not None else None,
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _auction_event_from_row(row: sqlite3.Row) -> AuctionEvent:
        return AuctionEvent(
            id=row["id"],
            owner_chat_id=row["owner_chat_id"],
            auction_id=row["auction_id"],
            event_date=date.fromisoformat(row["event_date"]),
            event_type=row["event_type"] or "",
            source_kind=row["source_kind"] or "",
            source_ref=row["source_ref"] or "",
            title=row["title"] or "",
            description=row["description"] or "",
            actor_name=row["actor_name"] or "",
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _next_auction_registry_position(conn: sqlite3.Connection, owner_chat_id: int) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(registry_position), 0) AS max_position FROM auctions WHERE owner_chat_id = ?",
            (owner_chat_id,),
        ).fetchone()
        return int(row["max_position"]) + 1

    @staticmethod
    def _backfill_auction_registry_positions(conn: sqlite3.Connection) -> None:
        owner_rows = conn.execute("SELECT DISTINCT owner_chat_id FROM auctions").fetchall()
        for owner_row in owner_rows:
            owner_chat_id = int(owner_row["owner_chat_id"])
            max_row = conn.execute(
                "SELECT COALESCE(MAX(registry_position), 0) AS max_position FROM auctions WHERE owner_chat_id = ?",
                (owner_chat_id,),
            ).fetchone()
            next_position = int(max_row["max_position"]) + 1
            rows = conn.execute(
                """
                SELECT id
                FROM auctions
                WHERE owner_chat_id = ? AND registry_position IS NULL
                ORDER BY datetime(created_at) ASC, id ASC
                """,
                (owner_chat_id,),
            ).fetchall()
            for row in rows:
                conn.execute(
                    """
                    UPDATE auctions
                    SET registry_position = ?
                    WHERE id = ? AND owner_chat_id = ? AND registry_position IS NULL
                    """,
                    (next_position, int(row["id"]), owner_chat_id),
                )
                next_position += 1

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

    def ensure_payroll_seed(self, owner_chat_id: int) -> None:
        with self.connection() as conn:
            has_rows = conn.execute(
                "SELECT 1 FROM payroll_entries WHERE owner_chat_id = ? LIMIT 1",
                (owner_chat_id,),
            ).fetchone()
            if has_rows is None:
                seed_rows = [
                    ("Эдуард Шевченко", "Учредитель", {"2026-01": (300000, 14642, 0, 285358, 0, ""), "2026-02": (300000, 17000, 0, 283000, 0, ""), "2026-03": (300000, 17000, 0, 283000, 0, "")}),
                    ("Денис Учайкин", "Учредитель", {"2026-01": (300000, 14824, 0, 285176, 0, ""), "2026-02": (300000, 17000, 0, 283000, 0, ""), "2026-03": (300000, 17000, 0, 283000, 0, "")}),
                    ("Александр Волкоруб", "Нач стройки", {"2026-01": (75000, 14642, 0, 60358, 0, ""), "2026-02": (150000, 17000, 50000, 83000, 0, "80 000 руб Сане"), "2026-03": (150000, 17000, 50000, 83000, 0, "")}),
                    ("Икрам Алимов", "Снабжение & Люди", {"2026-01": (110000, 14642, 40000, 55358, 0, ""), "2026-02": (110000, 17000, 40000, 53000, 0, ""), "2026-03": (110000, 17000, 40000, 53000, 0, "")}),
                    ("Александр Ю", "Снабженец", {"2026-01": (120000, 0, 50000, 70000, 0, ""), "2026-02": (120000, 0, 50000, 70000, 0, ""), "2026-03": (120000, 0, 50000, 70000, 0, "")}),
                    ("Захар Шкуркин", "Прораб", {"2026-01": (120000, 0, 40000, 80000, 0, ""), "2026-02": (120000, 0, 40000, 80000, 0, ""), "2026-03": (120000, 0, 40000, 80000, 0, "")}),
                    ("Ольга Жмурина", "Ведущий бухгалтер", {"2026-01": (150000, 0, 60000, 90000, 0, ""), "2026-02": (150000, 0, 60000, 90000, 0, ""), "2026-03": (150000, 0, 60000, 90000, 0, "")}),
                    ("Ирина", "Госзакупки", {"2026-01": (100000, 0, 40000, 60000, 0, ""), "2026-02": (100000, 0, 40000, 60000, 0, ""), "2026-03": (100000, 0, 40000, 60000, 0, "")}),
                    ("Дарья", "Специалист ПТО", {"2026-01": (30000, 0, 0, 30000, 0, ""), "2026-02": (30000, 0, 0, 30000, 0, ""), "2026-03": (30000, 0, 0, 30000, 0, "")}),
                    ("Алимов Анвар", "Специалист", {"2026-02": (0, 8839, 0, 0, 0, ""), "2026-03": (0, 8839, 0, 0, 0, "")}),
                ]
                for full_name, role_title, months in seed_rows:
                    employee_id = self._ensure_payroll_employee(conn, owner_chat_id, full_name, role_title)
                    for month_key, values in months.items():
                        month_date = date.fromisoformat(f"{month_key}-01")
                        self._upsert_payroll_entry(
                            conn,
                            owner_chat_id,
                            employee_id,
                            month_date,
                            values[0],
                            values[1],
                            values[2],
                            values[3],
                            values[4],
                            values[5],
                        )
            self._ensure_payroll_hasan(conn, owner_chat_id)

    def list_payroll_employees(self, owner_chat_id: int) -> list[PayrollEmployee]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, owner_chat_id, full_name, role_title, is_active, created_at
                FROM payroll_employees
                WHERE owner_chat_id = ?
                ORDER BY is_active DESC, id ASC
                """,
                (owner_chat_id,),
            ).fetchall()
        return [self._payroll_employee_from_row(row) for row in rows]

    def add_payroll_employee(self, owner_chat_id: int, full_name: str, role_title: str) -> int:
        with self.connection() as conn:
            return self._ensure_payroll_employee(conn, owner_chat_id, full_name.strip(), role_title.strip())

    def list_payroll_months(self, owner_chat_id: int) -> list[date]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT payroll_month
                FROM payroll_entries
                WHERE owner_chat_id = ?
                ORDER BY payroll_month DESC
                """,
                (owner_chat_id,),
            ).fetchall()
        return [date.fromisoformat(row["payroll_month"]) for row in rows]

    def list_payroll_rows(self, owner_chat_id: int, payroll_month: date) -> list[PayrollRow]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    e.id AS employee_id,
                    e.owner_chat_id,
                    e.full_name,
                    e.role_title,
                    e.is_active,
                    COALESCE(p.payroll_month, ?) AS payroll_month,
                    COALESCE(p.accrued_amount, 0) AS accrued_amount,
                    COALESCE(p.advance_card_amount, 0) AS advance_card_amount,
                    COALESCE(p.advance_card_paid_amount, 0) AS advance_card_paid_amount,
                    p.advance_card_paid_date AS advance_card_paid_date,
                    COALESCE(p.advance_cash_amount, 0) AS advance_cash_amount,
                    COALESCE(p.advance_cash_paid_amount, 0) AS advance_cash_paid_amount,
                    p.advance_cash_paid_date AS advance_cash_paid_date,
                    COALESCE(p.salary_amount, 0) AS salary_amount,
                    COALESCE(p.salary_paid_amount, 0) AS salary_paid_amount,
                    p.salary_paid_date AS salary_paid_date,
                    COALESCE(p.bonus_amount, 0) AS bonus_amount,
                    COALESCE(p.bonus_paid_amount, 0) AS bonus_paid_amount,
                    p.bonus_paid_date AS bonus_paid_date,
                    COALESCE(p.note, '') AS note
                FROM payroll_employees e
                LEFT JOIN payroll_entries p
                  ON p.employee_id = e.id
                 AND p.owner_chat_id = e.owner_chat_id
                 AND p.payroll_month = ?
                WHERE e.owner_chat_id = ?
                ORDER BY e.is_active DESC, e.id ASC
                """,
                (payroll_month.strftime(DATE_FMT), payroll_month.strftime(DATE_FMT), owner_chat_id),
            ).fetchall()
        return [self._payroll_row_from_row(row) for row in rows]

    def upsert_payroll_amount(
        self,
        owner_chat_id: int,
        employee_id: int,
        payroll_month: date,
        field_name: str,
        amount: float,
    ) -> bool:
        field_map = {
            "accrued_amount": "accrued_amount",
            "advance_card_amount": "advance_card_amount",
            "advance_cash_amount": "advance_cash_amount",
            "salary_amount": "salary_amount",
            "bonus_amount": "bonus_amount",
        }
        column = field_map.get(field_name)
        if column is None:
            return False
        with self.connection() as conn:
            employee = conn.execute(
                "SELECT id FROM payroll_employees WHERE id = ? AND owner_chat_id = ?",
                (employee_id, owner_chat_id),
            ).fetchone()
            if employee is None:
                return False
            self._upsert_payroll_entry(conn, owner_chat_id, employee_id, payroll_month, updated_field=column, updated_value=amount)
            return True

    def upsert_payroll_payment(
        self,
        owner_chat_id: int,
        employee_id: int,
        payroll_month: date,
        payment_kind: str,
        planned_amount: float,
        paid_amount: float | None,
        paid_date: date | None,
        is_paid: bool,
    ) -> bool:
        kind_map = {
            "advance_card": ("advance_card_amount", "advance_card_paid_amount", "advance_card_paid_date"),
            "advance_cash": ("advance_cash_amount", "advance_cash_paid_amount", "advance_cash_paid_date"),
            "salary": ("salary_amount", "salary_paid_amount", "salary_paid_date"),
            "bonus": ("bonus_amount", "bonus_paid_amount", "bonus_paid_date"),
        }
        columns = kind_map.get(payment_kind)
        if columns is None:
            return False
        with self.connection() as conn:
            employee = conn.execute(
                "SELECT id FROM payroll_employees WHERE id = ? AND owner_chat_id = ?",
                (employee_id, owner_chat_id),
            ).fetchone()
            if employee is None:
                return False
            self._upsert_payroll_entry(
                conn,
                owner_chat_id,
                employee_id,
                payroll_month,
                updated_payment={
                    "planned_column": columns[0],
                    "paid_column": columns[1],
                    "date_column": columns[2],
                    "planned_amount": planned_amount,
                    "paid_amount": 0.0 if not is_paid else float(paid_amount or 0),
                    "paid_date": None if not is_paid else paid_date,
                },
            )
            return True

    def upsert_payroll_note(self, owner_chat_id: int, employee_id: int, payroll_month: date, note: str) -> bool:
        with self.connection() as conn:
            employee = conn.execute(
                "SELECT id FROM payroll_employees WHERE id = ? AND owner_chat_id = ?",
                (employee_id, owner_chat_id),
            ).fetchone()
            if employee is None:
                return False
            self._upsert_payroll_entry(conn, owner_chat_id, employee_id, payroll_month, updated_field="note", updated_value=note.strip())
            return True

    @staticmethod
    def _payroll_employee_from_row(row: sqlite3.Row) -> PayrollEmployee:
        return PayrollEmployee(
            id=int(row["id"]),
            owner_chat_id=int(row["owner_chat_id"]),
            full_name=row["full_name"],
            role_title=row["role_title"] or "",
            is_active=bool(row["is_active"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _payroll_row_from_row(row: sqlite3.Row) -> PayrollRow:
        return PayrollRow(
            employee_id=int(row["employee_id"]),
            owner_chat_id=int(row["owner_chat_id"]),
            full_name=row["full_name"],
            role_title=row["role_title"] or "",
            is_active=bool(row["is_active"]),
            payroll_month=date.fromisoformat(row["payroll_month"]),
            accrued_amount=float(row["accrued_amount"]),
            advance_card_amount=float(row["advance_card_amount"]),
            advance_card_paid_amount=float(row["advance_card_paid_amount"]),
            advance_card_paid_date=date.fromisoformat(row["advance_card_paid_date"]) if row["advance_card_paid_date"] else None,
            advance_cash_amount=float(row["advance_cash_amount"]),
            advance_cash_paid_amount=float(row["advance_cash_paid_amount"]),
            advance_cash_paid_date=date.fromisoformat(row["advance_cash_paid_date"]) if row["advance_cash_paid_date"] else None,
            salary_amount=float(row["salary_amount"]),
            salary_paid_amount=float(row["salary_paid_amount"]),
            salary_paid_date=date.fromisoformat(row["salary_paid_date"]) if row["salary_paid_date"] else None,
            bonus_amount=float(row["bonus_amount"]),
            bonus_paid_amount=float(row["bonus_paid_amount"]),
            bonus_paid_date=date.fromisoformat(row["bonus_paid_date"]) if row["bonus_paid_date"] else None,
            note=row["note"] or "",
        )

    def _ensure_payroll_employee(self, conn: sqlite3.Connection, owner_chat_id: int, full_name: str, role_title: str) -> int:
        existing = conn.execute(
            """
            SELECT id
            FROM payroll_employees
            WHERE owner_chat_id = ? AND full_name = ? AND role_title = ?
            LIMIT 1
            """,
            (owner_chat_id, full_name, role_title),
        ).fetchone()
        if existing is not None:
            return int(existing["id"])
        cursor = conn.execute(
            """
            INSERT INTO payroll_employees (owner_chat_id, full_name, role_title, is_active, created_at)
            VALUES (?, ?, ?, 1, ?)
            """,
            (owner_chat_id, full_name, role_title, datetime.utcnow().isoformat()),
        )
        return int(cursor.lastrowid)

    def _ensure_payroll_hasan(self, conn: sqlite3.Connection, owner_chat_id: int) -> None:
        employee_id = self._ensure_payroll_employee(conn, owner_chat_id, "Хасан", "")
        for month_key in ("2026-01", "2026-02", "2026-03"):
            month_date = date.fromisoformat(f"{month_key}-01")
            exists = conn.execute(
                """
                SELECT 1
                FROM payroll_entries
                WHERE owner_chat_id = ? AND employee_id = ? AND payroll_month = ?
                LIMIT 1
                """,
                (owner_chat_id, employee_id, month_date.strftime(DATE_FMT)),
            ).fetchone()
            if exists is not None:
                continue
            self._upsert_payroll_entry(
                conn,
                owner_chat_id,
                employee_id,
                month_date,
                8839,
                0,
                0,
                0,
                0,
                "",
            )

    def ensure_payables_seed(self, owner_chat_id: int) -> None:
        with self.connection() as conn:
            has_rows = conn.execute(
                "SELECT 1 FROM payables WHERE owner_chat_id = ? LIMIT 1",
                (owner_chat_id,),
            ).fetchone()
            if has_rows is not None:
                return
            self._ensure_default_web_admin(conn, owner_chat_id)
            demo_rows = [
                ("ВШ-РСС", "№ ЗЯИ-258", date(2026, 3, 13), "Строитель", "Сверка по подрядчику", 34102.09, 0.0, None, date(2026, 4, 13), None, "Система"),
                ("ВШ-РСС", "№ ЗЯИ-277", date(2026, 3, 18), "Строитель", "Сверка по подрядчику", 15530.00, 0.0, None, date(2026, 4, 18), None, "Система"),
                ("Распутная Е.В. ИП (Мансард-Мастер)", "№ 131", date(2026, 3, 16), "Строитель", "Работы по объекту", 1800.00, 0.0, None, date(2026, 3, 16), None, "Система"),
                ("Сырцова Н. П. ИП", "№ 43", date(2026, 3, 12), "Библиотека №13", "Вывоз мусора", 28000.00, 0.0, None, date(2026, 3, 27), None, "Система"),
                ("Сырцова Н. П. ИП", "№ 44", date(2026, 3, 14), "Библиотека №13", "Вывоз мусора", 14000.00, 0.0, None, date(2026, 3, 29), None, "Система"),
                ("Сырцова Н. П. ИП", "№ 46", date(2026, 3, 21), "Библиотека №13", "Вывоз мусора", 17000.00, 0.0, None, date(2026, 4, 5), None, "Система"),
                ("Придворная Р.О. ИП", "№ 19", date(2026, 3, 16), "Строитель", "Аренда гидромолота", 36000.00, 0.0, None, date(2026, 3, 16), None, "Система"),
                ("ВЛ Снаб", "№ 18", date(2026, 3, 18), "Строитель", "Щебень", 114000.00, 0.0, None, date(2026, 4, 18), None, "Система"),
                ("Уютстрой ДВ (Инфострой)", "№ ЦБ-428", date(2026, 3, 12), "Строитель", "Поставка по объекту", 9752.00, 0.0, None, date(2026, 3, 19), None, "Система"),
                ("Металл Хауз", "№ ЦУ-572", date(2026, 3, 23), "Строитель", "Поставка по объекту", 1280.00, 0.0, None, date(2026, 3, 24), None, "Система"),
            ]
            now = datetime.utcnow().isoformat()
            for counterparty, document_ref, document_date, object_name, comment, amount, paid_amount, paid_date, due_date, created_by_user_id, created_by_name in demo_rows:
                conn.execute(
                    """
                    INSERT INTO payables (
                        owner_chat_id, counterparty, document_ref, document_date, object_name, comment,
                        amount, paid_amount, paid_date, due_date, created_by_user_id, created_by_name, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        owner_chat_id,
                        counterparty,
                        document_ref,
                        document_date.strftime(DATE_FMT) if document_date is not None else None,
                        object_name,
                        comment,
                        amount,
                        paid_amount,
                        paid_date.strftime(DATE_FMT) if paid_date is not None else None,
                        due_date.strftime(DATE_FMT),
                        created_by_user_id,
                        created_by_name,
                        now,
                        now,
                    ),
                )

    def list_payables(self, owner_chat_id: int) -> list[PayableEntry]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    id, owner_chat_id, counterparty, document_ref, document_date, object_name, comment,
                    amount, paid_amount, paid_date, due_date, created_by_user_id, created_by_name, deleted_at, created_at, updated_at
                FROM payables
                WHERE owner_chat_id = ?
                ORDER BY counterparty COLLATE NOCASE ASC, due_date ASC, COALESCE(document_date, due_date) ASC, id ASC
                """,
                (owner_chat_id,),
            ).fetchall()
        return [self._payable_from_row(row) for row in rows]

    def get_payable(self, owner_chat_id: int, payable_id: int) -> PayableEntry | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT
                    id, owner_chat_id, counterparty, document_ref, document_date, object_name, comment,
                    amount, paid_amount, paid_date, due_date, created_by_user_id, created_by_name, deleted_at, created_at, updated_at
                FROM payables
                WHERE owner_chat_id = ? AND id = ?
                LIMIT 1
                """,
                (owner_chat_id, payable_id),
            ).fetchone()
        return self._payable_from_row(row) if row is not None else None

    def add_payable(
        self,
        owner_chat_id: int,
        counterparty: str,
        document_ref: str,
        document_date: date | None,
        object_name: str,
        comment: str,
        amount: float,
        due_date: date,
        created_by_user_id: int | None,
        created_by_name: str,
    ) -> int:
        with self.connection() as conn:
            now = datetime.utcnow().isoformat()
            cursor = conn.execute(
                """
                INSERT INTO payables (
                    owner_chat_id, counterparty, document_ref, document_date, object_name, comment,
                    amount, paid_amount, paid_date, due_date, created_by_user_id, created_by_name, deleted_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, ?, ?, NULL, ?, ?)
                """,
                (
                    owner_chat_id,
                    counterparty.strip(),
                    document_ref.strip(),
                    document_date.strftime(DATE_FMT) if document_date is not None else None,
                    object_name.strip(),
                    comment.strip(),
                    amount,
                    due_date.strftime(DATE_FMT),
                    created_by_user_id,
                    created_by_name.strip(),
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def update_payable_details(
        self,
        owner_chat_id: int,
        payable_id: int,
        counterparty: str,
        document_ref: str,
        document_date: date | None,
        object_name: str,
        comment: str,
        amount: float,
        due_date: date,
    ) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE payables
                SET counterparty = ?, document_ref = ?, document_date = ?, object_name = ?, comment = ?,
                    amount = ?, due_date = ?, updated_at = ?
                WHERE id = ? AND owner_chat_id = ?
                """,
                (
                    counterparty.strip(),
                    document_ref.strip(),
                    document_date.strftime(DATE_FMT) if document_date is not None else None,
                    object_name.strip(),
                    comment.strip(),
                    amount,
                    due_date.strftime(DATE_FMT),
                    datetime.utcnow().isoformat(),
                    payable_id,
                    owner_chat_id,
                ),
            )
            return cursor.rowcount > 0

    def update_payable_payment(
        self,
        owner_chat_id: int,
        payable_id: int,
        paid_amount: float,
        paid_date: date | None,
    ) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE payables
                SET paid_amount = ?, paid_date = ?, updated_at = ?
                WHERE id = ? AND owner_chat_id = ?
                """,
                (
                    paid_amount,
                    paid_date.strftime(DATE_FMT) if paid_date is not None else None,
                    datetime.utcnow().isoformat(),
                    payable_id,
                    owner_chat_id,
                ),
            )
            return cursor.rowcount > 0

    def soft_delete_payable(self, owner_chat_id: int, payable_id: int, deleted_at: datetime) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE payables
                SET deleted_at = ?, updated_at = ?
                WHERE id = ? AND owner_chat_id = ?
                """,
                (deleted_at.isoformat(), deleted_at.isoformat(), payable_id, owner_chat_id),
            )
            return cursor.rowcount > 0

    def restore_deleted_payable(self, owner_chat_id: int, payable_id: int) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE payables
                SET deleted_at = NULL, updated_at = ?
                WHERE id = ? AND owner_chat_id = ? AND deleted_at IS NOT NULL
                """,
                (datetime.utcnow().isoformat(), payable_id, owner_chat_id),
            )
            return cursor.rowcount > 0

    def hard_delete_payable(self, owner_chat_id: int, payable_id: int) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                DELETE FROM payables
                WHERE id = ? AND owner_chat_id = ?
                """,
                (payable_id, owner_chat_id),
            )
            return cursor.rowcount > 0

    def hard_delete_all_deleted_payables(self, owner_chat_id: int) -> int:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                DELETE FROM payables
                WHERE owner_chat_id = ? AND deleted_at IS NOT NULL
                """,
                (owner_chat_id,),
            )
            return cursor.rowcount

    def list_tasks(self, owner_chat_id: int, include_deleted: bool = False) -> list[TaskEntry]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    id, owner_chat_id, title, description, due_date, assignee_kind, assignee_user_id,
                    assignee_name, assignee_role_code, assignee_role_name, status, completion_comment,
                    created_by_user_id, created_by_name, created_at, completed_at, completed_by_name, deleted_at
                FROM tasks
                WHERE owner_chat_id = ? AND (? = 1 OR deleted_at IS NULL)
                ORDER BY deleted_at IS NOT NULL, due_date ASC, created_at DESC, id DESC
                """,
                (owner_chat_id, 1 if include_deleted else 0),
            ).fetchall()
        return [self._task_from_row(row) for row in rows]

    def get_task(self, owner_chat_id: int, task_id: int) -> TaskEntry | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT
                    id, owner_chat_id, title, description, due_date, assignee_kind, assignee_user_id,
                    assignee_name, assignee_role_code, assignee_role_name, status, completion_comment,
                    created_by_user_id, created_by_name, created_at, completed_at, completed_by_name, deleted_at
                FROM tasks
                WHERE owner_chat_id = ? AND id = ?
                """,
                (owner_chat_id, task_id),
            ).fetchone()
        return self._task_from_row(row) if row else None

    def add_task(
        self,
        owner_chat_id: int,
        title: str,
        description: str,
        due_date: date,
        assignee_kind: str,
        assignee_user_id: int | None,
        assignee_name: str,
        assignee_role_code: str,
        assignee_role_name: str,
        created_by_user_id: int | None,
        created_by_name: str,
    ) -> int:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO tasks (
                    owner_chat_id, title, description, due_date,
                    assignee_kind, assignee_user_id, assignee_name, assignee_role_code, assignee_role_name,
                    status, completion_comment,
                    created_by_user_id, created_by_name, created_at, completed_at, completed_by_name, deleted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', '', ?, ?, ?, NULL, '', NULL)
                """,
                (
                    owner_chat_id,
                    title.strip(),
                    description.strip(),
                    due_date.strftime(DATE_FMT),
                    assignee_kind.strip(),
                    assignee_user_id,
                    assignee_name.strip(),
                    assignee_role_code.strip(),
                    assignee_role_name.strip(),
                    created_by_user_id,
                    created_by_name.strip(),
                    datetime.utcnow().isoformat(),
                ),
            )
            return int(cursor.lastrowid)

    def update_task(
        self,
        owner_chat_id: int,
        task_id: int,
        title: str,
        description: str,
        due_date: date,
        assignee_kind: str,
        assignee_user_id: int | None,
        assignee_name: str,
        assignee_role_code: str,
        assignee_role_name: str,
    ) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE tasks
                SET title = ?, description = ?, due_date = ?, assignee_kind = ?,
                    assignee_user_id = ?, assignee_name = ?, assignee_role_code = ?, assignee_role_name = ?
                WHERE id = ? AND owner_chat_id = ? AND deleted_at IS NULL
                """,
                (
                    title.strip(),
                    description.strip(),
                    due_date.strftime(DATE_FMT),
                    assignee_kind.strip(),
                    assignee_user_id,
                    assignee_name.strip(),
                    assignee_role_code.strip(),
                    assignee_role_name.strip(),
                    task_id,
                    owner_chat_id,
                ),
            )
            return cursor.rowcount > 0

    def update_task_status(
        self,
        owner_chat_id: int,
        task_id: int,
        status: str,
        completion_comment: str,
        completed_at: datetime | None,
        completed_by_name: str,
    ) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE tasks
                SET status = ?, completion_comment = ?, completed_at = ?, completed_by_name = ?
                WHERE id = ? AND owner_chat_id = ? AND deleted_at IS NULL
                """,
                (
                    status.strip(),
                    completion_comment.strip(),
                    completed_at.isoformat() if completed_at is not None else None,
                    completed_by_name.strip(),
                    task_id,
                    owner_chat_id,
                ),
            )
            return cursor.rowcount > 0

    def soft_delete_task(self, owner_chat_id: int, task_id: int, deleted_at: datetime) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE tasks
                SET deleted_at = ?
                WHERE id = ? AND owner_chat_id = ? AND deleted_at IS NULL
                """,
                (deleted_at.isoformat(), task_id, owner_chat_id),
            )
            return cursor.rowcount > 0

    def restore_deleted_task(self, owner_chat_id: int, task_id: int) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE tasks
                SET deleted_at = NULL
                WHERE id = ? AND owner_chat_id = ? AND deleted_at IS NOT NULL
                """,
                (task_id, owner_chat_id),
            )
            return cursor.rowcount > 0

    def hard_delete_task(self, owner_chat_id: int, task_id: int) -> bool:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                DELETE FROM tasks
                WHERE id = ? AND owner_chat_id = ? AND deleted_at IS NOT NULL
                """,
                (task_id, owner_chat_id),
            )
            return cursor.rowcount > 0

    def hard_delete_all_deleted_tasks(self, owner_chat_id: int) -> int:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                DELETE FROM tasks
                WHERE owner_chat_id = ? AND deleted_at IS NOT NULL
                """,
                (owner_chat_id,),
            )
            return cursor.rowcount

    def list_task_comments(self, owner_chat_id: int, task_id: int) -> list[TaskComment]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT c.id, c.task_id, c.comment_type, c.body, c.author_user_id, c.author_name, c.created_at
                FROM task_comments c
                JOIN tasks t ON t.id = c.task_id
                WHERE t.owner_chat_id = ? AND c.task_id = ?
                ORDER BY c.created_at ASC, c.id ASC
                """,
                (owner_chat_id, task_id),
            ).fetchall()
        return [self._task_comment_from_row(row) for row in rows]

    def add_task_comment(
        self,
        owner_chat_id: int,
        task_id: int,
        comment_type: str,
        body: str,
        author_user_id: int | None,
        author_name: str,
    ) -> int | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT id FROM tasks WHERE id = ? AND owner_chat_id = ?",
                (task_id, owner_chat_id),
            ).fetchone()
            if row is None:
                return None
            cursor = conn.execute(
                """
                INSERT INTO task_comments (task_id, comment_type, body, author_user_id, author_name, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    comment_type.strip() or "comment",
                    body.strip(),
                    author_user_id,
                    author_name.strip(),
                    datetime.utcnow().isoformat(),
                ),
            )
            return int(cursor.lastrowid)

    def list_task_attachments(self, owner_chat_id: int, task_id: int) -> list[TaskAttachment]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT a.id, a.task_id, a.comment_id, a.file_name, a.file_path, a.created_at
                FROM task_attachments a
                JOIN tasks t ON t.id = a.task_id
                WHERE t.owner_chat_id = ? AND a.task_id = ?
                ORDER BY a.created_at ASC, a.id ASC
                """,
                (owner_chat_id, task_id),
            ).fetchall()
        return [self._task_attachment_from_row(row) for row in rows]

    def add_task_attachment(self, owner_chat_id: int, task_id: int, comment_id: int | None, file_name: str, file_path: str) -> int | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT id FROM tasks WHERE id = ? AND owner_chat_id = ?",
                (task_id, owner_chat_id),
            ).fetchone()
            if row is None:
                return None
            cursor = conn.execute(
                """
                INSERT INTO task_attachments (task_id, comment_id, file_name, file_path, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    comment_id,
                    file_name.strip(),
                    file_path.strip(),
                    datetime.utcnow().isoformat(),
                ),
            )
            return int(cursor.lastrowid)

    def get_task_attachment(self, owner_chat_id: int, attachment_id: int) -> TaskAttachment | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT a.id, a.task_id, a.comment_id, a.file_name, a.file_path, a.created_at
                FROM task_attachments a
                JOIN tasks t ON t.id = a.task_id
                WHERE t.owner_chat_id = ? AND a.id = ?
                """,
                (owner_chat_id, attachment_id),
            ).fetchone()
        return self._task_attachment_from_row(row) if row else None

    @staticmethod
    def _payable_from_row(row: sqlite3.Row) -> PayableEntry:
        return PayableEntry(
            id=int(row["id"]),
            owner_chat_id=int(row["owner_chat_id"]),
            counterparty=row["counterparty"],
            document_ref=row["document_ref"] or "",
            document_date=date.fromisoformat(row["document_date"]) if row["document_date"] else None,
            object_name=row["object_name"] or "",
            comment=row["comment"] or "",
            amount=float(row["amount"]),
            paid_amount=float(row["paid_amount"]),
            paid_date=date.fromisoformat(row["paid_date"]) if row["paid_date"] else None,
            due_date=date.fromisoformat(row["due_date"]),
            created_by_user_id=int(row["created_by_user_id"]) if row["created_by_user_id"] is not None else None,
            created_by_name=row["created_by_name"] or "",
            deleted_at=datetime.fromisoformat(row["deleted_at"]) if row["deleted_at"] else None,
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _task_from_row(row: sqlite3.Row) -> TaskEntry:
        return TaskEntry(
            id=int(row["id"]),
            owner_chat_id=int(row["owner_chat_id"]),
            title=row["title"] or "",
            description=row["description"] or "",
            due_date=date.fromisoformat(row["due_date"]),
            assignee_kind=row["assignee_kind"] or "user",
            assignee_user_id=int(row["assignee_user_id"]) if row["assignee_user_id"] is not None else None,
            assignee_name=row["assignee_name"] or "",
            assignee_role_code=row["assignee_role_code"] or "",
            assignee_role_name=row["assignee_role_name"] or "",
            status=row["status"] or "open",
            completion_comment=row["completion_comment"] or "",
            created_by_user_id=int(row["created_by_user_id"]) if row["created_by_user_id"] is not None else None,
            created_by_name=row["created_by_name"] or "",
            created_at=datetime.fromisoformat(row["created_at"]),
            completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
            completed_by_name=row["completed_by_name"] or "",
            deleted_at=datetime.fromisoformat(row["deleted_at"]) if row["deleted_at"] else None,
        )

    @staticmethod
    def _task_comment_from_row(row: sqlite3.Row) -> TaskComment:
        return TaskComment(
            id=int(row["id"]),
            task_id=int(row["task_id"]),
            comment_type=row["comment_type"] or "comment",
            body=row["body"] or "",
            author_user_id=int(row["author_user_id"]) if row["author_user_id"] is not None else None,
            author_name=row["author_name"] or "",
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _task_attachment_from_row(row: sqlite3.Row) -> TaskAttachment:
        return TaskAttachment(
            id=int(row["id"]),
            task_id=int(row["task_id"]),
            comment_id=int(row["comment_id"]) if row["comment_id"] is not None else None,
            file_name=row["file_name"] or "",
            file_path=row["file_path"] or "",
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    def _upsert_payroll_entry(
        self,
        conn: sqlite3.Connection,
        owner_chat_id: int,
        employee_id: int,
        payroll_month: date,
        accrued_amount: float | None = None,
        advance_card_amount: float | None = None,
        advance_cash_amount: float | None = None,
        salary_amount: float | None = None,
        bonus_amount: float | None = None,
        note: str | None = None,
        updated_field: str | None = None,
        updated_value: float | str | None = None,
        updated_payment: dict | None = None,
    ) -> None:
        should_sync_salary_amount = any(
            value is not None
            for value in (accrued_amount, advance_card_amount, advance_cash_amount)
        )
        row = conn.execute(
            """
            SELECT id, accrued_amount, advance_card_amount, advance_card_paid_amount, advance_card_paid_date,
                   advance_cash_amount, advance_cash_paid_amount, advance_cash_paid_date,
                   salary_amount, salary_paid_amount, salary_paid_date,
                   bonus_amount, bonus_paid_amount, bonus_paid_date, note
            FROM payroll_entries
            WHERE owner_chat_id = ? AND employee_id = ? AND payroll_month = ?
            LIMIT 1
            """,
            (owner_chat_id, employee_id, payroll_month.strftime(DATE_FMT)),
        ).fetchone()
        now = datetime.utcnow().isoformat()
        payload = {
            "accrued_amount": float(accrued_amount or 0),
            "advance_card_amount": float(advance_card_amount or 0),
            "advance_card_paid_amount": 0.0,
            "advance_card_paid_date": None,
            "advance_cash_amount": float(advance_cash_amount or 0),
            "advance_cash_paid_amount": 0.0,
            "advance_cash_paid_date": None,
            "salary_amount": float(salary_amount or 0),
            "salary_paid_amount": 0.0,
            "salary_paid_date": None,
            "bonus_amount": float(bonus_amount or 0),
            "bonus_paid_amount": 0.0,
            "bonus_paid_date": None,
            "note": note or "",
        }
        if row is not None:
            payload = {
                "accrued_amount": float(row["accrued_amount"]),
                "advance_card_amount": float(row["advance_card_amount"]),
                "advance_card_paid_amount": float(row["advance_card_paid_amount"]),
                "advance_card_paid_date": row["advance_card_paid_date"],
                "advance_cash_amount": float(row["advance_cash_amount"]),
                "advance_cash_paid_amount": float(row["advance_cash_paid_amount"]),
                "advance_cash_paid_date": row["advance_cash_paid_date"],
                "salary_amount": float(row["salary_amount"]),
                "salary_paid_amount": float(row["salary_paid_amount"]),
                "salary_paid_date": row["salary_paid_date"],
                "bonus_amount": float(row["bonus_amount"]),
                "bonus_paid_amount": float(row["bonus_paid_amount"]),
                "bonus_paid_date": row["bonus_paid_date"],
                "note": row["note"] or "",
            }
        if updated_field is not None:
            payload[updated_field] = updated_value if updated_field == "note" else float(updated_value or 0)
            if updated_field in {"accrued_amount", "advance_card_amount", "advance_cash_amount"}:
                should_sync_salary_amount = True
        if updated_payment is not None:
            payload[updated_payment["planned_column"]] = float(updated_payment["planned_amount"] or 0)
            payload[updated_payment["paid_column"]] = float(updated_payment["paid_amount"] or 0)
            payload[updated_payment["date_column"]] = (
                updated_payment["paid_date"].strftime(DATE_FMT)
                if updated_payment.get("paid_date") is not None
                else None
            )
            if updated_payment["planned_column"] in {"advance_card_amount", "advance_cash_amount"}:
                should_sync_salary_amount = True
        if row is None or should_sync_salary_amount:
            payload["salary_amount"] = round(
                max(
                    float(payload["accrued_amount"])
                    - float(payload["advance_card_amount"])
                    - float(payload["advance_cash_amount"]),
                    0.0,
                ),
                2,
            )
        if row is None:
            conn.execute(
                """
                INSERT INTO payroll_entries (
                    owner_chat_id, employee_id, payroll_month, accrued_amount, advance_card_amount,
                    advance_card_paid_amount, advance_card_paid_date,
                    advance_cash_amount, advance_cash_paid_amount, advance_cash_paid_date,
                    salary_amount, salary_paid_amount, salary_paid_date,
                    bonus_amount, bonus_paid_amount, bonus_paid_date, note, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    owner_chat_id,
                    employee_id,
                    payroll_month.strftime(DATE_FMT),
                    payload["accrued_amount"],
                    payload["advance_card_amount"],
                    payload["advance_card_paid_amount"],
                    payload["advance_card_paid_date"],
                    payload["advance_cash_amount"],
                    payload["advance_cash_paid_amount"],
                    payload["advance_cash_paid_date"],
                    payload["salary_amount"],
                    payload["salary_paid_amount"],
                    payload["salary_paid_date"],
                    payload["bonus_amount"],
                    payload["bonus_paid_amount"],
                    payload["bonus_paid_date"],
                    payload["note"],
                    now,
                    now,
                ),
            )
            return
        conn.execute(
            """
            UPDATE payroll_entries
            SET accrued_amount = ?, advance_card_amount = ?, advance_card_paid_amount = ?, advance_card_paid_date = ?,
                advance_cash_amount = ?, advance_cash_paid_amount = ?, advance_cash_paid_date = ?,
                salary_amount = ?, salary_paid_amount = ?, salary_paid_date = ?,
                bonus_amount = ?, bonus_paid_amount = ?, bonus_paid_date = ?, note = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                payload["accrued_amount"],
                payload["advance_card_amount"],
                payload["advance_card_paid_amount"],
                payload["advance_card_paid_date"],
                payload["advance_cash_amount"],
                payload["advance_cash_paid_amount"],
                payload["advance_cash_paid_date"],
                payload["salary_amount"],
                payload["salary_paid_amount"],
                payload["salary_paid_date"],
                payload["bonus_amount"],
                payload["bonus_paid_amount"],
                payload["bonus_paid_date"],
                payload["note"],
                now,
                row["id"],
            ),
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
            conn.execute(
                """
                UPDATE web_users
                SET role_name = CASE WHEN role_name IN ('BigBoss', '', 'Admin') THEN 'Админ' ELSE role_name END
                WHERE id = ?
                """,
                (admin_id,),
            )
            self._replace_web_user_permissions(conn, admin_id, self._full_access_permissions())
            return admin_id

        cursor = conn.execute(
            """
            INSERT INTO web_users (
                owner_chat_id, email, full_name, role_name, password_state,
                is_active, is_super_admin, created_at
            )
            VALUES (?, ?, 'Админ', 'Админ', 'local_only', 1, 1, ?)
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
