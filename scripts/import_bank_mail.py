#!/usr/bin/env python3
"""Import daily 1C bank statements from an IMAP mailbox into CRM DDS."""

from __future__ import annotations

import email
import hashlib
import imaplib
import os
import sys
from email.header import decode_header
from email.message import Message
from email.utils import parsedate_to_datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from storage import Storage
from webapp import import_bank_1c_statement


def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Не задана переменная окружения {name}")
    return value


def decode_mime_header(raw: str | None) -> str:
    if not raw:
        return ""
    parts: list[str] = []
    for value, encoding in decode_header(raw):
        if isinstance(value, bytes):
            parts.append(value.decode(encoding or "utf-8", errors="replace"))
        else:
            parts.append(value)
    return "".join(parts).strip()


def message_date(message: Message) -> str:
    raw = message.get("Date", "")
    if not raw:
        return ""
    try:
        return parsedate_to_datetime(raw).isoformat()
    except (TypeError, ValueError):
        return raw


def iter_txt_attachments(message: Message):
    for part in message.walk():
        if part.is_multipart():
            continue
        disposition = (part.get_content_disposition() or "").lower()
        filename = decode_mime_header(part.get_filename())
        content_type = (part.get_content_type() or "").lower()
        if disposition != "attachment" and not filename:
            continue
        if filename and not filename.lower().endswith(".txt"):
            continue
        if not filename and content_type != "text/plain":
            continue
        payload = part.get_payload(decode=True)
        if payload:
            yield filename or "bank-statement.txt", payload


def imap_ok(response) -> bool:
    return bool(response) and response[0] == "OK"


def main() -> int:
    db_path = os.getenv("DB_PATH", "contracts.db")
    owner_chat_id = int(env_required("BANK_MAIL_OWNER_CHAT_ID"))
    login = env_required("BANK_MAIL_LOGIN")
    password = env_required("BANK_MAIL_PASSWORD")
    host = os.getenv("BANK_MAIL_IMAP_HOST", "imap.mail.ru").strip() or "imap.mail.ru"
    port = int(os.getenv("BANK_MAIL_IMAP_PORT", "993"))
    folder = os.getenv("BANK_MAIL_FOLDER", "INBOX").strip() or "INBOX"
    sender_filter = os.getenv("BANK_MAIL_SENDER", "").strip().casefold()
    limit = int(os.getenv("BANK_MAIL_LIMIT", "20"))
    mark_seen = os.getenv("BANK_MAIL_MARK_SEEN", "1").strip().lower() not in {"0", "false", "no"}

    storage = Storage(db_path)
    imported_files = 0
    skipped_files = 0
    error_files = 0

    with imaplib.IMAP4_SSL(host, port) as imap:
        imap.login(login, password)
        status, _ = imap.select(folder)
        if status != "OK":
            raise RuntimeError(f"Не удалось открыть папку IMAP: {folder}")
        status, data = imap.uid("search", None, "UNSEEN")
        if status != "OK":
            raise RuntimeError("Не удалось найти новые письма в IMAP")
        uids = (data[0] or b"").split()[:limit]
        for uid in uids:
            status, fetched = imap.uid("fetch", uid, "(RFC822)")
            if status != "OK" or not fetched:
                continue
            raw_message = next((item[1] for item in fetched if isinstance(item, tuple) and len(item) > 1), None)
            if not raw_message:
                continue
            message = email.message_from_bytes(raw_message)
            message_from = decode_mime_header(message.get("From"))
            if sender_filter and sender_filter not in message_from.casefold():
                continue
            subject = decode_mime_header(message.get("Subject"))
            uid_text = uid.decode("ascii", errors="ignore")
            message_had_attachment = False
            message_had_errors = False
            for filename, payload in iter_txt_attachments(message):
                message_had_attachment = True
                attachment_hash = hashlib.sha256(payload).hexdigest()
                if storage.bank_statement_mail_attachment_exists(owner_chat_id, attachment_hash):
                    skipped_files += 1
                    continue
                try:
                    result = import_bank_1c_statement(
                        storage,
                        owner_chat_id,
                        payload,
                        created_by_user_id=None,
                        created_by_name="Автоимпорт выписки",
                    )
                    storage.add_bank_statement_mail_import(
                        owner_chat_id,
                        login,
                        folder,
                        uid_text,
                        subject,
                        message_from,
                        message_date(message),
                        filename,
                        attachment_hash,
                        "processed",
                        result.imported_count,
                        result.duplicate_count,
                        result.skipped_count,
                        result.balance_count,
                    )
                    imported_files += 1
                except Exception as exc:
                    message_had_errors = True
                    error_files += 1
                    storage.add_bank_statement_mail_import(
                        owner_chat_id,
                        login,
                        folder,
                        uid_text,
                        subject,
                        message_from,
                        message_date(message),
                        filename,
                        attachment_hash,
                        "error",
                        0,
                        0,
                        0,
                        0,
                        str(exc),
                    )
            if mark_seen and message_had_attachment and not message_had_errors:
                imap.uid("store", uid, "+FLAGS", r"(\Seen)")
        imap.logout()

    print(
        "Bank mail import finished: "
        f"processed={imported_files}, already_processed={skipped_files}, errors={error_files}"
    )
    return 0 if error_files == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
