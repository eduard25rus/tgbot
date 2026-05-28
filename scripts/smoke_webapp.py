#!/usr/bin/env python3
"""Fast smoke-check for the CRM web app.

The script renders key CRM pages through the WSGI app against a temporary
SQLite database. It is intentionally small: the goal is to catch broken imports,
route crashes, bad date parsing and accidental 500 responses before deployment.
"""

from __future__ import annotations

import io
import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlencode, urlsplit


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))


OWNER_CHAT_ID = 329457823


def seed_demo_data(db_path: Path) -> str:
    os.environ["DB_PATH"] = str(db_path)

    from storage import Storage

    storage = Storage(str(db_path))
    today = date.today()

    contract_id = storage.add_contract(
        OWNER_CHAT_ID,
        "Smoke object",
        "Smoke address",
        "",
        "0123456789",
        "https://zakupki.gov.ru/",
        1_500_000,
        0,
        "Smoke contract",
        today,
        today + timedelta(days=30),
        30,
    )
    storage.add_stage(
        contract_id,
        1,
        "Smoke stage",
        today,
        today + timedelta(days=14),
        1_500_000,
    )
    storage.add_auction(
        OWNER_CHAT_ID,
        "0123456789000001",
        today + timedelta(days=7),
        1_000_000,
        10,
        "Smoke auction",
        "Владивосток",
        "https://zakupki.gov.ru/",
        None,
        "Smoke",
    )
    storage.add_jurisprudence_object(OWNER_CHAT_ID, "Smoke court object", "", None, "Smoke")
    court_case_id = storage.add_court_case(
        OWNER_CHAT_ID,
        "Smoke court object",
        "А51-smoke",
        "Smoke court case",
        "plaintiff",
        "Smoke opponent",
        100_000,
        today + timedelta(days=10),
        "active",
        "Smoke court comment",
        None,
        "Smoke",
    )
    if court_case_id is not None:
        storage.add_court_event(
            OWNER_CHAT_ID,
            court_case_id,
            today,
            "hearing",
            "Smoke hearing",
            "Smoke event",
            None,
            "Smoke",
        )

    storage.ensure_default_web_admin(OWNER_CHAT_ID)
    admin = next(user for user in storage.list_web_users(OWNER_CHAT_ID) if user["is_super_admin"])
    token = "smoke-session-token"
    storage.create_web_session(admin["id"], token)
    return token


def call_app(
    path: str,
    session_token: str,
    method: str = "GET",
    body: bytes = b"",
    content_type: str = "application/x-www-form-urlencoded",
) -> tuple[str, list[tuple[str, str]], bytes]:
    import webapp

    target = urlsplit(path)
    captured: dict[str, object] = {}

    def start_response(status: str, headers: list[tuple[str, str]], exc_info=None):
        captured["status"] = status
        captured["headers"] = headers

    environ = {
        "REQUEST_METHOD": method,
        "SCRIPT_NAME": "",
        "PATH_INFO": target.path or "/",
        "QUERY_STRING": target.query,
        "SERVER_NAME": "localhost",
        "SERVER_PORT": "8000",
        "SERVER_PROTOCOL": "HTTP/1.1",
        "REMOTE_ADDR": "127.0.0.1",
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": "http",
        "wsgi.input": io.BytesIO(body),
        "wsgi.errors": sys.stderr,
        "wsgi.multithread": False,
        "wsgi.multiprocess": False,
        "wsgi.run_once": False,
        "CONTENT_LENGTH": str(len(body)),
        "CONTENT_TYPE": content_type,
        "HTTP_COOKIE": f"{webapp.SESSION_COOKIE}={session_token}",
    }
    body = b"".join(webapp.app(environ, start_response))
    return str(captured.get("status", "")), list(captured.get("headers", [])), body


def main() -> int:
    paths = [
        "/",
        "/contracts",
        "/auctions",
        "/events",
        "/tasks",
        "/settings",
        "/directories",
        "/payables",
        "/expenses",
        "/expenses/imports",
        "/payroll",
        "/workforce",
        "/dashboards",
        "/finance-analysis",
        "/finance-register",
        "/finance-loans",
        "/finance-receivables",
        "/finance-liabilities",
        "/cashoperations",
        "/jurisprudence/letters",
        "/jurisprudence/courts",
        "/jurisprudence/courts/1",
        "/access",
        "/favicon.png",
    ]

    with tempfile.TemporaryDirectory(prefix="crm-smoke-") as temp_dir:
        db_path = Path(temp_dir) / "smoke.db"
        token = seed_demo_data(db_path)

        failures: list[str] = []
        for path in paths:
            try:
                status, _headers, body = call_app(path, token)
            except Exception as exc:
                failures.append(f"{path}: exception {type(exc).__name__}: {exc}")
                continue

            status_code = int(status.split(" ", 1)[0] or "0")
            if status_code >= 500:
                failures.append(f"{path}: {status}")
                continue
            if body.startswith(b"A server error occurred") or b"Traceback" in body:
                failures.append(f"{path}: error marker in response")

        try:
            form_body = urlencode(
                {
                    "full_name": "Александр Волкотруб",
                    "login": "smoke.workforce",
                    "role_name": "Менеджер проектов",
                    "view_workforce": "on",
                    "edit_workforce": "on",
                }
            ).encode("utf-8")
            status, _headers, _body = call_app(
                f"/access/users/new?owner={OWNER_CHAT_ID}&mode=general",
                token,
                method="POST",
                body=form_body,
            )
            status_code = int(status.split(" ", 1)[0] or "0")
            if status_code not in {302, 303}:
                failures.append(f"/access/users/new workforce: {status}")
            status, _headers, body = call_app(f"/access?owner={OWNER_CHAT_ID}&mode=general", token)
            if not status.startswith("200") or "Александр Волкотруб".encode("utf-8") not in body:
                failures.append("/access/users/new workforce: user was not rendered after add")
            from storage import Storage

            storage = Storage(str(db_path))
            user = next(
                (item for item in storage.list_web_users(OWNER_CHAT_ID) if item["login"] == "smoke.workforce"),
                None,
            )
            if user is None:
                failures.append("/access/users/new workforce: user was not saved")
            else:
                cash_form_body = urlencode(
                    {
                        "enabled": "1",
                        "role": "limited",
                        "default_screen": "home",
                        "personal_cashbox": "1",
                        "preview_login": "smoke.workforce",
                        "can_add_expense": "1",
                    }
                ).encode("utf-8")
                status, _headers, _body = call_app(
                    f"/access/cash/users/{user['id']}/update?owner={OWNER_CHAT_ID}&mode=cash",
                    token,
                    method="POST",
                    body=cash_form_body,
                )
                status_code = int(status.split(" ", 1)[0] or "0")
                if status_code not in {302, 303}:
                    failures.append(f"/access/cash/users update personal cashbox: {status}")
                personal_cashbox_code = storage.personal_cashbox_code_for_user(user["id"])
                access = storage.get_mobile_cash_access_for_user(user["id"])
                cashbox_codes = {item["code"] for item in storage.list_cashbox_directory(OWNER_CHAT_ID)}
                cashbox_labels = {
                    item["code"]: item["label"]
                    for item in storage.list_cashbox_directory(OWNER_CHAT_ID)
                }
                if personal_cashbox_code not in cashbox_codes:
                    failures.append("/access/cash/users update personal cashbox: cashbox was not created")
                if cashbox_labels.get(personal_cashbox_code) != "Касса Александра":
                    failures.append("/access/cash/users update personal cashbox: cashbox label was not normalized")
                if personal_cashbox_code not in set(access["allowed_cashbox_codes"] if access else []):
                    failures.append("/access/cash/users update personal cashbox: cashbox was not allowed")
                if access and access["default_cashbox_code"] != personal_cashbox_code:
                    failures.append("/access/cash/users update personal cashbox: cashbox was not selected by default")
                cash_form_body = urlencode(
                    {
                        "enabled": "1",
                        "role": "limited",
                        "default_screen": "home",
                        "preview_login": "smoke.workforce",
                        "can_add_expense": "1",
                    }
                ).encode("utf-8")
                status, _headers, _body = call_app(
                    f"/access/cash/users/{user['id']}/update?owner={OWNER_CHAT_ID}&mode=cash",
                    token,
                    method="POST",
                    body=cash_form_body,
                )
                status_code = int(status.split(" ", 1)[0] or "0")
                if status_code not in {302, 303}:
                    failures.append(f"/access/cash/users update personal cashbox off: {status}")
                access = storage.get_mobile_cash_access_for_user(user["id"])
                if personal_cashbox_code in set(access["allowed_cashbox_codes"] if access else []):
                    failures.append("/access/cash/users update personal cashbox off: cashbox remained allowed")
        except Exception as exc:
            failures.append(f"/access/users/new workforce: exception {type(exc).__name__}: {exc}")

        if failures:
            print("Smoke check failed:")
            for failure in failures:
                print(f" - {failure}")
            return 1

    print(f"Smoke check passed: {len(paths)} pages rendered without 500.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
