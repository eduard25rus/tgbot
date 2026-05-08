#!/usr/bin/env python3
"""Import daily 1C bank statements from an IMAP mailbox into CRM DDS."""

from __future__ import annotations

import email
import hashlib
import imaplib
import os
import re
import ssl
import sys
from html import unescape
from http.cookiejar import CookieJar
from email.header import decode_header
from email.message import Message
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import URLError
from urllib.parse import unquote
from urllib.parse import urljoin
from urllib.parse import urlsplit
from urllib.request import build_opener
from urllib.request import HTTPCookieProcessor
from urllib.request import HTTPSHandler
from urllib.request import Request


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


def iter_html_parts(message: Message):
    for part in message.walk():
        if part.is_multipart() or part.get_content_type() != "text/html":
            continue
        payload = part.get_payload(decode=True)
        if payload:
            yield payload.decode(part.get_content_charset() or "utf-8", errors="replace")


def iter_text_parts(message: Message):
    for part in message.walk():
        if part.is_multipart() or part.get_content_type() not in {"text/html", "text/plain"}:
            continue
        payload = part.get_payload(decode=True)
        if payload:
            yield payload.decode(part.get_content_charset() or "utf-8", errors="replace")


def iter_sber_statement_links(message: Message):
    seen: set[str] = set()
    for text in iter_text_parts(message):
        raw_links: list[str] = []
        raw_links.extend(re.findall(r"""href=["']([^"']+)""", text, flags=re.IGNORECASE))
        raw_links.extend(re.findall(r"""https?://[^\s"'<>]+""", text))
        for raw_link in raw_links:
            link = unescape(raw_link).strip()
            link = link.rstrip(").,;")
            if "sbi.sberbank.ru" not in link or "/statements/download/mail/reports/" not in link:
                continue
            if link in seen:
                continue
            seen.add(link)
            yield link


def filename_from_content_disposition(raw: str | None) -> str:
    if not raw:
        return ""
    filename_star = re.search(r"filename\*=UTF-8''([^;]+)", raw, flags=re.IGNORECASE)
    if filename_star:
        return unquote(filename_star.group(1).strip().strip('"'))
    filename = re.search(r'filename="?([^";]+)"?', raw, flags=re.IGNORECASE)
    if filename:
        return decode_mime_header(filename.group(1).strip())
    return ""


def statement_request(link: str) -> Request:
    return Request(
        link,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/plain,text/html,application/octet-stream,*/*",
        },
    )


def statement_opener(*, verify_ssl: bool = True):
    handlers = [HTTPCookieProcessor(CookieJar())]
    if not verify_ssl:
        handlers.append(HTTPSHandler(context=ssl._create_unverified_context()))
    return build_opener(*handlers)


def fetch_statement_url(opener, link: str) -> tuple[str, bytes, str, str]:
    with opener.open(statement_request(link), timeout=30) as response:
        data = response.read()
        filename = filename_from_content_disposition(response.headers.get("Content-Disposition"))
        content_type = response.headers.get("Content-Type", "")
        final_url = response.geturl()
    return filename, data, content_type, final_url


def decode_download_html(data: bytes, content_type: str) -> str:
    charset_match = re.search(r"charset=([\w.-]+)", content_type or "", flags=re.IGNORECASE)
    encodings = [charset_match.group(1)] if charset_match else []
    encodings.extend(["utf-8", "cp1251"])
    for encoding in encodings:
        try:
            return data.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            continue
    return data.decode("utf-8", errors="replace")


def html_download_candidates(html: str, base_url: str) -> list[str]:
    raw_links: list[tuple[str, bool]] = []
    raw_links.extend((match, False) for match in re.findall(r"""href=["']([^"']+)""", html, flags=re.IGNORECASE))
    raw_links.extend((match, True) for match in re.findall(r"""action=["']([^"']+)""", html, flags=re.IGNORECASE))
    raw_links.extend((match, False) for match in re.findall(r"""url=([^"'>\s]+)""", html, flags=re.IGNORECASE))
    raw_links.extend((match, False) for match in re.findall(r"""location(?:\.href)?\s*=\s*["']([^"']+)""", html, flags=re.IGNORECASE))
    raw_links.extend((match.replace(r"\/", "/"), False) for match in re.findall(r"""https?:\\?/\\?/[^"'\s<>]+""", html))
    candidates: list[str] = []
    seen: set[str] = set()
    for raw_link, allow_fallback in raw_links:
        raw_link = unescape(raw_link).strip()
        lowered_raw = raw_link.casefold()
        if not raw_link or lowered_raw.startswith(("javascript:", "mailto:", "tel:", "#")):
            continue
        link = urljoin(base_url, raw_link)
        parsed = urlsplit(link)
        lowered = link.casefold()
        if parsed.netloc and "sbi.sberbank.ru" not in parsed.netloc.casefold():
            continue
        if re.search(r"\.(css|js|json|png|jpe?g|gif|svg|ico|woff2?)($|[?#])", parsed.path.casefold()):
            continue
        if "/manifest/" in parsed.path.casefold():
            continue
        if link in seen:
            continue
        if not allow_fallback and not any(
            marker in lowered for marker in ("statement", "statements", "download", "reports", ".txt", "onec")
        ):
            continue
        seen.add(link)
        candidates.append(link)
    candidates.sort(key=lambda item: (".txt" not in item.casefold(), "download" not in item.casefold(), len(item)))
    return candidates


def html_form_summary(html: str) -> str:
    form_matches = re.findall(r"(?is)<form\b[^>]*>(.*?)</form>", html)
    if not form_matches:
        return ""
    form_bits: list[str] = []
    for form_html in form_matches[:2]:
        names = re.findall(r"""name=["']([^"']+)""", form_html, flags=re.IGNORECASE)
        action_match = re.search(r"""action=["']([^"']+)""", form_html, flags=re.IGNORECASE)
        action = unescape(action_match.group(1)).strip() if action_match else ""
        bit = "form"
        if action:
            bit += f" action={action}"
        if names:
            bit += " fields=" + ",".join(names[:8])
        form_bits.append(bit)
    return " | ".join(form_bits)


def looks_like_expired_sber_page(html: str) -> bool:
    text = compact_html_text(html, limit=1200).casefold()
    return any(
        marker in text
        for marker in (
            "срок действия",
            "истек",
            "истёк",
            "недоступ",
            "не найд",
            "ошибка",
        )
    )


def sber_html_error_prefix(html: str) -> str:
    if looks_like_expired_sber_page(html):
        return "Сбер вернул HTML-страницу вместо TXT 1С. Похоже, ссылка на выписку уже истекла. "
    return "Сбер вернул HTML-страницу вместо TXT 1С, и внутри нее не нашлась прямая ссылка на файл. "


def compact_html_text(html: str, limit: int = 700) -> str:
    cleaned = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
    cleaned = re.sub(r"(?s)<[^>]+>", " ", cleaned)
    cleaned = unescape(cleaned)
    cleaned = " ".join(cleaned.split())
    return cleaned[:limit]


def html_debug_summary(html: str, base_url: str, content_type: str, size: int) -> str:
    title_match = re.search(r"(?is)<title[^>]*>(.*?)</title>", html)
    title = " ".join(unescape(title_match.group(1)).split()) if title_match else ""
    raw_links = []
    raw_links.extend(re.findall(r"""href=["']([^"']+)""", html, flags=re.IGNORECASE))
    raw_links.extend(re.findall(r"""action=["']([^"']+)""", html, flags=re.IGNORECASE))
    raw_scripts = re.findall(r"""src=["']([^"']+)""", html, flags=re.IGNORECASE)
    links = []
    seen = set()
    for raw_link in raw_links:
        link = urljoin(base_url, unescape(raw_link).strip())
        if link in seen:
            continue
        seen.add(link)
        links.append(link)
        if len(links) >= 5:
            break
    parts = [
        f"Content-Type: {content_type or 'не указан'}",
        f"размер: {size} байт",
    ]
    if title:
        parts.append(f"title: {title}")
    text = compact_html_text(html)
    if text:
        parts.append(f"текст: {text}")
    if links:
        parts.append("ссылки: " + " | ".join(links))
    if raw_scripts:
        scripts = [urljoin(base_url, unescape(item).strip()) for item in raw_scripts[:5]]
        parts.append("scripts: " + " | ".join(scripts))
    forms = html_form_summary(html)
    if forms:
        parts.append(forms)
    return "; ".join(parts)


def download_statement_link(link: str) -> tuple[str, bytes]:
    try:
        opener = statement_opener()
        return download_statement_link_with_opener(opener, link)
    except URLError as exc:
        if not isinstance(getattr(exc, "reason", None), ssl.SSLCertVerificationError):
            raise
        opener = statement_opener(verify_ssl=False)
        return download_statement_link_with_opener(opener, link)


def download_statement_link_with_opener(opener, link: str) -> tuple[str, bytes]:
    visited: set[str] = set()
    last_content_type = ""
    last_size = 0
    last_url = link
    last_html_error = ""
    for _attempt in range(4):
        if link in visited:
            break
        visited.add(link)
        filename, data, content_type, final_url = fetch_statement_url(opener, link)
        last_content_type = content_type
        last_size = len(data)
        last_url = final_url
        if b"1CClientBankExchange" in data:
            return filename or "kl_to_1c.txt", data
        if "html" not in (content_type or "").casefold():
            break
        html = decode_download_html(data, content_type)
        last_html_error = sber_html_error_prefix(html) + html_debug_summary(html, final_url, content_type, len(data))
        candidates = [candidate for candidate in html_download_candidates(html, final_url) if candidate not in visited]
        if not candidates:
            raise ValueError(last_html_error)
        link = candidates[0]
    if last_html_error:
        raise ValueError(
            "Сбер вернул по ссылке не TXT 1С после перехода по найденной ссылке. "
            f"Последний URL: {last_url}. Content-Type: {last_content_type or 'не указан'}, размер: {last_size} байт. "
            f"Первая HTML-страница: {last_html_error}"
        )
    raise ValueError(
        "Сбер вернул по ссылке не TXT 1С. "
        f"Последний URL: {last_url}. Content-Type: {last_content_type or 'не указан'}, размер: {last_size} байт."
    )


def iter_statement_sources(message: Message):
    yield from iter_txt_attachments(message)
    for link in iter_sber_statement_links(message):
        yield download_statement_link(link)


def add_mail_import_error(
    storage: Storage,
    owner_chat_id: int,
    login: str,
    folder: str,
    uid_text: str,
    subject: str,
    message_from: str,
    message: Message,
    filename: str,
    error: Exception,
) -> None:
    storage.add_bank_statement_mail_import(
        owner_chat_id,
        login,
        folder,
        uid_text,
        subject,
        message_from,
        message_date(message),
        filename,
        "",
        "error",
        0,
        0,
        0,
        0,
        str(error),
    )


def imap_ok(response) -> bool:
    return bool(response) and response[0] == "OK"


def matches_text_filter(value: str, raw_filter: str) -> bool:
    needles = [item.strip().casefold() for item in raw_filter.split(",") if item.strip()]
    if not needles:
        return True
    haystack = value.casefold()
    return any(needle in haystack for needle in needles)


def run_bank_mail_import() -> tuple[int, int, int]:
    db_path = os.getenv("DB_PATH", "contracts.db")
    owner_chat_id = int(env_required("BANK_MAIL_OWNER_CHAT_ID"))
    login = env_required("BANK_MAIL_LOGIN")
    password = env_required("BANK_MAIL_PASSWORD")
    host = os.getenv("BANK_MAIL_IMAP_HOST", "imap.mail.ru").strip() or "imap.mail.ru"
    port = int(os.getenv("BANK_MAIL_IMAP_PORT", "993"))
    folder = os.getenv("BANK_MAIL_FOLDER", "INBOX").strip() or "INBOX"
    sender_filter = os.getenv("BANK_MAIL_SENDER", "sberbusiness@sberbank.ru").strip().casefold()
    subject_filter = os.getenv("BANK_MAIL_SUBJECT", "Выписка по сч").strip()
    limit = int(os.getenv("BANK_MAIL_LIMIT", "20"))
    mark_seen = os.getenv("BANK_MAIL_MARK_SEEN", "1").strip().lower() not in {"0", "false", "no"}
    search_query = os.getenv("BANK_MAIL_SEARCH", "ALL").strip() or "ALL"

    storage = Storage(db_path)
    imported_files = 0
    skipped_files = 0
    error_files = 0

    with imaplib.IMAP4_SSL(host, port) as imap:
        imap.login(login, password)
        status, _ = imap.select(folder)
        if status != "OK":
            raise RuntimeError(f"Не удалось открыть папку IMAP: {folder}")
        status, data = imap.uid("search", None, search_query)
        if status != "OK":
            raise RuntimeError("Не удалось найти новые письма в IMAP")
        uids = list(reversed((data[0] or b"").split()[-limit:]))
        for uid in uids:
            status, fetched = imap.uid("fetch", uid, "(BODY.PEEK[])")
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
            if not matches_text_filter(subject, subject_filter):
                continue
            uid_text = uid.decode("ascii", errors="ignore")
            message_had_statement = False
            message_had_errors = False
            statement_sources: list[tuple[str, bytes]] = list(iter_txt_attachments(message))
            for link in iter_sber_statement_links(message):
                message_had_statement = True
                try:
                    statement_sources.append(download_statement_link(link))
                except Exception as exc:
                    message_had_errors = True
                    error_files += 1
                    add_mail_import_error(
                        storage,
                        owner_chat_id,
                        login,
                        folder,
                        uid_text,
                        subject,
                        message_from,
                        message,
                        "Ссылка на выписку Сбера",
                        exc,
                    )
            for filename, payload in statement_sources:
                message_had_statement = True
                attachment_hash = hashlib.sha256(payload).hexdigest()
                if storage.bank_statement_mail_attachment_exists(owner_chat_id, attachment_hash):
                    storage.add_bank_statement_mail_import(
                        owner_chat_id,
                        login,
                        folder,
                        uid_text,
                        subject,
                        message_from,
                        message_date(message),
                        filename,
                        "",
                        "duplicate",
                        0,
                        1,
                        0,
                        0,
                        "Эта выписка уже была обработана ранее.",
                    )
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
                    add_mail_import_error(
                        storage,
                        owner_chat_id,
                        login,
                        folder,
                        uid_text,
                        subject,
                        message_from,
                        message,
                        filename,
                        exc,
                    )
            if mark_seen and message_had_statement and not message_had_errors:
                imap.uid("store", uid, "+FLAGS", r"(\Seen)")
        imap.logout()

    print(
        "Bank mail import finished: "
        f"processed={imported_files}, already_processed={skipped_files}, errors={error_files}"
    )
    return imported_files, skipped_files, error_files


def main() -> int:
    _imported_files, _skipped_files, error_files = run_bank_mail_import()
    return 0 if error_files == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
