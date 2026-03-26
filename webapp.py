from __future__ import annotations

import os
import hashlib
import secrets
import re
from datetime import datetime
from datetime import date
from datetime import timedelta
from datetime import timezone
from http.cookies import SimpleCookie
from html import escape
from typing import Iterable
from urllib.parse import parse_qs
from urllib.parse import quote_plus
from wsgiref.simple_server import make_server

from storage import Storage, WEB_SECTION_IDS


STATUS_META = {
    "not_started": ("Не приступили", "chip"),
    "in_progress": ("В работе", "chip warn"),
    "completed": ("Выполнен", "chip ok"),
    "closed_iis": ("Закрыт на ИИС", "chip"),
}

STAGE_PAYMENT_META = {
    "unpaid": ("Не оплачено", "chip"),
    "paid": ("Оплачен", "chip ok"),
}

AUCTION_ESTIMATE_META = {
    "pending": ("Не решено", "chip"),
    "approved": ("Считать", "chip warn"),
    "calculated": ("Просчитан", "chip ok"),
    "not_calculated": ("Не просчитан", "chip danger"),
    "rejected": ("Не считать", "chip"),
}

AUCTION_SUBMIT_DECISION_META = {
    "pending": ("Не решено", "chip"),
    "approved": ("Подавать заявку", "chip warn"),
    "submitted": ("Заявка подана", "chip ok"),
    "rejected": ("Не подаемся", "chip"),
}

AUCTION_RESULT_META = {
    "not_participated": ("Не участвовали", "chip"),
    "pending": ("Ждем розыгрыш", "chip warn"),
    "won": ("Выигран", "chip accent"),
    "recognized_winner": ("Признан победителем", "chip ok"),
    "lost": ("Проигран", "chip danger"),
    "rejected": ("Заявка отклонена", "chip danger"),
}

AUCTION_ESTIMATE_OPTIONS = [
    ("pending", "Не решено"),
    ("approved", "Считать"),
    ("calculated", "Просчитан"),
    ("not_calculated", "Не просчитан"),
    ("rejected", "Не считать"),
]

AUCTION_SUBMIT_OPTIONS = [
    ("pending", "Не решено"),
    ("approved", "Подавать заявку"),
    ("submitted", "Заявка подана"),
    ("rejected", "Не подаемся"),
]

AUCTION_RESULT_OPTIONS = [
    ("not_participated", "Не участвовали"),
    ("pending", "Ждем розыгрыш"),
    ("won", "Выигран"),
    ("recognized_winner", "Признан победителем"),
    ("lost", "Проигран"),
    ("rejected", "Заявка отклонена"),
]

MAX_DISCOUNT_OPTIONS = [0.0, 2.5, 5.0, 7.5, 10.0, 12.5, 15.0, 17.5, 20.0, 22.5, 25.0]
SESSION_COOKIE = "felis_session"
PREVIEW_ROLE_COOKIE = "felis_preview_role"
VLADIVOSTOK_TZ = timezone(timedelta(hours=10))
ROLE_PREVIEW_OPTIONS = [
    ("", "Админ"),
    ("procurement", "Отдел госзакупок"),
    ("supply", "Отдел снабжения"),
    ("management", "Руководство компании"),
]


def format_amount(amount: float) -> str:
    whole, frac = f"{amount:.2f}".split(".")
    parts = []
    while whole:
        parts.append(whole[-3:])
        whole = whole[:-3]
    return f"{' '.join(reversed(parts))},{frac} ₽"


def request_base_url(environ: dict | None = None) -> str:
    explicit = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
    if explicit:
        return explicit
    if environ is None:
        host = os.getenv("WEB_HOST", "127.0.0.1")
        port = int(os.getenv("WEB_PORT") or os.getenv("PORT", "8000"))
        if host in {"0.0.0.0", ""}:
            host = "127.0.0.1"
        return f"http://{host}:{port}"
    forwarded_proto = environ.get("HTTP_X_FORWARDED_PROTO", "").strip()
    proto = forwarded_proto or environ.get("wsgi.url_scheme", "http")
    forwarded_host = environ.get("HTTP_X_FORWARDED_HOST", "").strip()
    host = forwarded_host or environ.get("HTTP_HOST", "").strip()
    if not host:
        server_name = environ.get("SERVER_NAME", "127.0.0.1")
        server_port = environ.get("SERVER_PORT", "8000")
        if (proto == "https" and server_port == "443") or (proto == "http" and server_port == "80"):
            host = server_name
        else:
            host = f"{server_name}:{server_port}"
    return f"{proto}://{host}"


def format_amount_input(amount: float) -> str:
    whole, frac = f"{amount:.2f}".split(".")
    parts = []
    while whole:
        parts.append(whole[-3:])
        whole = whole[:-3]
    return f"{' '.join(reversed(parts))},{frac}"


def format_percent(value: float) -> str:
    return f"{value:.1f}".replace(".", ",") + "%"


def format_discount_percent(value: float) -> str:
    return "-" + format_percent(value)


def format_date(value: date) -> str:
    return value.strftime("%d-%m-%Y")


RU_MONTH_NAMES = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}


def format_month_label(value: date) -> str:
    return f"{RU_MONTH_NAMES[value.month]} {value.year}"


def format_datetime(value: datetime) -> str:
    localized = value
    if value.tzinfo is None:
        localized = value.replace(tzinfo=timezone.utc).astimezone(VLADIVOSTOK_TZ)
    else:
        localized = value.astimezone(VLADIVOSTOK_TZ)
    return localized.strftime("%d-%m-%Y %H:%M")


def parse_date(raw: str) -> date:
    raw = raw.strip()
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError("Нужен формат даты DD-MM-YYYY")


def parse_amount(raw: str) -> float:
    amount = float(raw.strip().replace(" ", "").replace(",", "."))
    if amount < 0:
        raise ValueError("Сумма не может быть отрицательной")
    return round(amount, 2)


def hash_password(password: str, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 200_000)
    return f"{salt}${derived.hex()}"


def verify_password(password: str, stored_hash: str) -> bool:
    if not stored_hash or "$" not in stored_hash:
        return False
    salt, expected = stored_hash.split("$", 1)
    return hash_password(password, salt) == f"{salt}${expected}"


def parse_cookies(environ) -> dict[str, str]:
    raw = environ.get("HTTP_COOKIE", "")
    if not raw:
        return {}
    cookie = SimpleCookie()
    cookie.load(raw)
    return {key: morsel.value for key, morsel in cookie.items()}


def parse_optional_number(raw: str) -> float | None:
    cleaned = raw.strip().replace(" ", "").replace(",", ".")
    if cleaned == "":
        return None
    return round(float(cleaned), 2)


def validate_auction_number(raw: str) -> str:
    normalized = raw.strip()
    if not normalized:
        raise ValueError("Нужно указать номер аукциона")
    if not normalized.isdigit():
        raise ValueError("В номере аукциона должны быть только цифры")
    return normalized


def auction_min_amount(total_amount: float, discount_percent: float) -> float:
    return round(total_amount * (1 - discount_percent / 100), 2)


def is_auction_archived(item) -> bool:
    if is_auction_deleted(item):
        return False
    if item.submit_decision_status == "rejected":
        return True
    return item.result_status in {"recognized_winner", "lost", "rejected"}


def is_auction_deleted(item) -> bool:
    return item.deleted_at is not None


def status_badge(status: str) -> str:
    label, css = STATUS_META.get(status, STATUS_META["not_started"])
    return f'<span class="{css}">{escape(label)}</span>'


def owner_options(storage: Storage) -> list[int]:
    with storage.connection() as conn:
        rows = conn.execute(
            """
            SELECT owner_id
            FROM (
                SELECT DISTINCT chat_id AS owner_id FROM contracts
                UNION
                SELECT DISTINCT owner_chat_id AS owner_id FROM auctions
            )
            ORDER BY owner_id ASC
            """
        ).fetchall()
    return [int(row["owner_id"]) for row in rows]


def pick_owner(storage: Storage, requested_owner: str | None) -> int | None:
    owners = owner_options(storage)
    if not owners:
        return None
    if requested_owner:
        try:
            parsed = int(requested_owner)
        except ValueError:
            parsed = None
        if parsed in owners:
            return parsed
    return owners[0]


def parse_month_key(raw: str | None) -> date | None:
    if not raw:
        return None
    raw = raw.strip()
    try:
        return date.fromisoformat(f"{raw}-01")
    except ValueError:
        return None


def contract_payload(storage: Storage, owner_chat_id: int) -> list[dict]:
    contracts = storage.list_contracts(owner_chat_id)
    payload: list[dict] = []
    for contract in contracts:
        stages = storage.list_stages_for_contract(owner_chat_id, contract.id)
        payments = storage.list_payments_for_contract(owner_chat_id, contract.id)
        total_amount = sum(stage.amount for stage in stages)
        paid_amount = sum(payment.amount for payment in payments)
        debt_amount = max(total_amount - paid_amount, 0.0)
        paid_ratio = (paid_amount / total_amount) if total_amount > 0 else 0.0
        advance_amount = round(total_amount * (contract.advance_percent or 0) / 100, 2) if total_amount > 0 else 0.0
        overdue_stages = sum(1 for stage in stages if stage.end_date < date.today())
        first_stage_start_date = next((stage.start_date for stage in stages if stage.start_date is not None), None)
        payload.append(
            {
                "contract": contract,
                "stages": stages,
                "payments": payments,
                "total_amount": total_amount,
                "paid_amount": paid_amount,
                "debt_amount": debt_amount,
                "paid_ratio": paid_ratio,
                "advance_amount": advance_amount,
                "overdue_stages": overdue_stages,
                "first_stage_start_date": first_stage_start_date,
            }
        )
    return payload


def permission_sections() -> list[tuple[str, str]]:
    return [(section_id, SECTION_LABELS[section_id]) for section_id in WEB_SECTION_IDS]


def parse_permissions(form: dict[str, str]) -> dict[str, dict[str, bool]]:
    permissions: dict[str, dict[str, bool]] = {}
    for section_id in WEB_SECTION_IDS:
        can_view = form.get(f"view_{section_id}") == "on"
        can_edit = form.get(f"edit_{section_id}") == "on"
        permissions[section_id] = {
            "can_view": can_view or can_edit,
            "can_edit": can_edit,
        }
    return permissions


def permission_summary(permissions: dict[str, dict[str, bool]]) -> str:
    enabled = [
        label
        for section_id, label in permission_sections()
        if permissions.get(section_id, {}).get("can_view")
    ]
    return ", ".join(enabled) if enabled else "Нет доступных разделов"


def is_procurement_user(current_user: dict | None) -> bool:
    return bool(
        current_user
        and current_user.get("effective_role_name", current_user.get("role_name")) in {"Отдел госзакупок", "procurement"}
    )


def is_supply_user(current_user: dict | None) -> bool:
    return bool(
        current_user
        and current_user.get("effective_role_name", current_user.get("role_name")) in {"Отдел снабжения", "supply"}
    )


def is_management_user(current_user: dict | None) -> bool:
    return bool(
        current_user
        and current_user.get("effective_role_name", current_user.get("role_name")) in {"Руководство компании", "management"}
    )


def can_edit_contract_stage_controls(current_user: dict | None) -> bool:
    return bool(
        current_user
        and (
            has_active_admin_mode(current_user)
            or is_management_user(current_user)
            or is_procurement_user(current_user)
        )
    )


def guard_contract_stage_controls(current_user: dict | None):
    return can_edit_contract_stage_controls(current_user)


def with_preview_role(current_user: dict | None, preview_role: str) -> dict | None:
    if current_user is None:
        return None
    effective_user = dict(current_user)
    preview_labels = dict(effective_user.get("preview_role_options", ROLE_PREVIEW_OPTIONS))
    effective_user["preview_role_name"] = preview_role
    effective_user["preview_role_label"] = preview_labels.get(preview_role, "")
    effective_user["effective_role_name"] = preview_role or current_user.get("role_name", "")
    return effective_user


def preview_role_code(label: str) -> str:
    normalized = label.strip()
    if not normalized:
        return ""
    folded = re.sub(r"[^a-zA-Zа-яА-Я0-9]+", " ", normalized).strip().lower()
    compact = folded.replace(" ", "")
    if folded in {"отдел госзакупок", "отдел гос закупок", "procurement"} or compact in {"отделгосзакупок", "procurement"}:
        return "procurement"
    if folded in {"отдел снабжения", "supply"} or compact in {"отделснабжения", "supply"}:
        return "supply"
    if folded in {"руководство компании", "management"} or compact in {"руководствокомпании", "management"}:
        return "management"
    if folded in {"админ", "bigboss", "admin"}:
        return ""
    return f"role_{hashlib.sha1(normalized.encode('utf-8')).hexdigest()[:10]}"


def preview_fallback_permissions(preview_role: str) -> dict[str, dict[str, bool]]:
    permissions = {
        section_id: {"can_view": False, "can_edit": False}
        for section_id in WEB_SECTION_IDS
    }
    if preview_role == "procurement":
        permissions["auctions"] = {"can_view": True, "can_edit": True}
        permissions["contracts"] = {"can_view": True, "can_edit": True}
    elif preview_role == "management":
        permissions["auctions"] = {"can_view": True, "can_edit": True}
        permissions["contracts"] = {"can_view": True, "can_edit": True}
    elif preview_role == "supply":
        permissions["auctions"] = {"can_view": True, "can_edit": True}
    return permissions


def resolve_preview_permissions(
    storage: Storage,
    owner_chat_id: int | None,
    preview_role: str,
) -> dict[str, dict[str, bool]]:
    if owner_chat_id is None or not preview_role:
        return {}
    matched_users = [
        user
        for user in storage.list_web_users(owner_chat_id)
        if not user.get("is_super_admin") and preview_role_code(user.get("role_name", "")) == preview_role
    ]
    if matched_users:
        return {
            section_id: dict(values)
            for section_id, values in matched_users[0].get("permissions", {}).items()
        }
    return preview_fallback_permissions(preview_role)


def preview_role_options(storage: Storage, owner_chat_id: int | None, current_user: dict | None) -> list[tuple[str, str]]:
    options: list[tuple[str, str]] = [("", "Админ")]
    if current_user is None or owner_chat_id is None or not current_user.get("is_super_admin"):
        return options
    seen = {""}
    for code, label in (
        ("management", "Руководство компании"),
        ("procurement", "Отдел госзакупок"),
        ("supply", "Отдел снабжения"),
    ):
        options.append((code, label))
        seen.add(code)
    for user in storage.list_web_users(owner_chat_id):
        if user.get("is_super_admin"):
            continue
        label = (user.get("role_name") or "").strip()
        code = preview_role_code(label)
        if not code or code in seen:
            continue
        options.append((code, label))
        seen.add(code)
    return options


def compute_role_notifications(storage: Storage, owner_chat_id: int | None, current_user: dict | None) -> dict[str, str | int | bool]:
    base = {
        "active": False,
        "count": 0,
        "title": "Уведомления",
        "primary": "Активных задач по этой роли сейчас нет.",
        "secondary": "",
        "tertiary": "",
        "items": [],
        "href": "",
    }
    if current_user is None or owner_chat_id is None:
        return base
    all_auctions = storage.list_auctions(owner_chat_id)
    active_auctions = [item for item in all_auctions if not is_auction_archived(item) and not is_auction_deleted(item)]
    if is_procurement_user(current_user):
        tasks = [item for item in active_auctions if item.submit_decision_status == "approved"]
        tasks.sort(key=lambda item: (item.bid_deadline, item.id))
        nearest = min(tasks, key=lambda item: item.bid_deadline, default=None)
        count = len(tasks)
        return {
            "active": count > 0,
            "count": count,
            "title": "Уведомления закупок",
            "primary": f"Необходимо подать заявок: {count}",
            "secondary": (
                f"Ближайший дедлайн подачи: {format_date(nearest.bid_deadline)}"
                if nearest is not None
                else "Сейчас нет активных задач по подаче."
            ),
            "tertiary": (
                f"До ближайшей подачи заявки осталось: {(nearest.bid_deadline - date.today()).days} дн."
                if nearest is not None
                else ""
            ),
            "items": [
                {
                    "title": item.title,
                    "deadline": format_date(item.bid_deadline),
                    "days_left": (item.bid_deadline - date.today()).days,
                }
                for item in tasks[:3]
            ],
            "href": f"/auctions?owner={owner_chat_id}&tab=active&task_view=1#auction-registry",
        }
    if is_supply_user(current_user):
        tasks = [item for item in active_auctions if item.estimate_status == "approved"]
        tasks.sort(key=lambda item: (item.bid_deadline, item.id))
        nearest = min(tasks, key=lambda item: item.bid_deadline, default=None)
        count = len(tasks)
        return {
            "active": count > 0,
            "count": count,
            "title": "Уведомления снабжения",
            "primary": f"Необходимо просчитать аукционов: {count}",
            "secondary": (
                f"Ближайший дедлайн просчета: {format_date(nearest.bid_deadline)}"
                if nearest is not None
                else "Сейчас нет активных задач по просчету."
            ),
            "tertiary": (
                f"До ближайшего дедлайна по просчету осталось: {(nearest.bid_deadline - date.today()).days} дн."
                if nearest is not None
                else ""
            ),
            "items": [
                {
                    "title": item.title,
                    "deadline": format_date(item.bid_deadline),
                    "days_left": (item.bid_deadline - date.today()).days,
                }
                for item in tasks[:3]
            ],
            "href": f"/auctions?owner={owner_chat_id}&tab=active&task_view=1#auction-registry",
        }
    if has_permission(current_user, "auctions", "edit"):
        decision_tasks = [item for item in active_auctions if item.submit_decision_status == "pending"]
        discount_tasks = [
            item
            for item in active_auctions
            if item.submit_decision_status == "approved" and item.max_discount_percent is None
        ]
        decision_tasks.sort(key=lambda item: (item.bid_deadline, item.id))
        discount_tasks.sort(key=lambda item: (item.bid_deadline, item.id))
        ordered_tasks = [("Требует решения", item) for item in decision_tasks] + [
            ("Нужно установить снижение", item) for item in discount_tasks
        ]
        nearest = min((item for _, item in ordered_tasks), key=lambda item: item.bid_deadline, default=None)
        return {
            "active": bool(ordered_tasks),
            "count": len(ordered_tasks),
            "title": "Уведомления руководства",
            "primary": f"Аукционов, требующих решения по подаче: {len(decision_tasks)}",
            "secondary": f"Аукционов, где нужно установить максимальное снижение: {len(discount_tasks)}",
            "tertiary": (
                f"До ближайшего дедлайна осталось: {(nearest.bid_deadline - date.today()).days} дн."
                if nearest is not None
                else ""
            ),
            "items": [
                {
                    "title": item.title,
                    "deadline": format_date(item.bid_deadline),
                    "days_left": (item.bid_deadline - date.today()).days,
                    "kind": kind,
                }
                for kind, item in ordered_tasks[:3]
            ],
            "href": f"/auctions?owner={owner_chat_id}&tab=active&task_view=1#auction-registry",
        }
    return base


def procurement_status_allowed(current_user: dict | None, auction, estimate_status: str, submit_decision_status: str, result_status: str) -> bool:
    if not is_procurement_user(current_user):
        return True
    if estimate_status != auction.estimate_status:
        return False
    submit_changed = submit_decision_status != auction.submit_decision_status
    result_changed = result_status != auction.result_status
    if submit_changed:
        if not (auction.submit_decision_status == "approved" and submit_decision_status == "submitted"):
            return False
    if not submit_changed and not result_changed:
        return True
    effective_submit = submit_decision_status
    if result_changed and effective_submit != "submitted":
        return False
    return True


def supply_status_allowed(current_user: dict | None, auction, estimate_status: str, submit_decision_status: str, result_status: str) -> bool:
    if not is_supply_user(current_user):
        return True
    if submit_decision_status != auction.submit_decision_status or result_status != auction.result_status:
        return False
    if estimate_status == auction.estimate_status:
        return True
    if auction.estimate_status not in {"approved", "calculated", "not_calculated"}:
        return False
    return estimate_status in {"calculated", "not_calculated"}


def management_status_allowed(current_user: dict | None, auction, estimate_status: str, submit_decision_status: str, result_status: str) -> bool:
    if not is_management_user(current_user):
        return True
    expected_result_status = auction.result_status
    if auction.submit_decision_status != "submitted":
        expected_result_status = "not_participated"
    elif expected_result_status == "not_participated":
        expected_result_status = "pending"
    requested_result_status = result_status
    if submit_decision_status != "submitted":
        requested_result_status = "not_participated"
    elif requested_result_status == "not_participated":
        requested_result_status = "pending"
    if requested_result_status != expected_result_status:
        return False
    estimate_changed = estimate_status != auction.estimate_status
    submit_changed = submit_decision_status != auction.submit_decision_status
    if not estimate_changed and not submit_changed:
        return True
    if estimate_changed and estimate_status not in {"pending", "approved", "rejected"}:
        return False
    if submit_changed and (auction.submit_decision_status == "submitted" or submit_decision_status == "submitted"):
        return False
    if submit_changed and submit_decision_status not in {"pending", "approved", "rejected"}:
        return False
    return True


def password_state_label(value: str) -> str:
    mapping = {
        "local_only": "Локальный вход без пароля",
        "pending_setup": "Пароль задаст сам",
    }
    return mapping.get(value, value)


def auction_chip(value: str, mapping: dict[str, tuple[str, str]], tooltip: str = "") -> str:
    label, css = mapping.get(value, ("Неизвестно", "chip"))
    tooltip_attr = f' data-tooltip="{escape(tooltip)}"' if tooltip else ""
    tooltip_class = " status-chip-tooltip" if tooltip else ""
    return f'<span class="{css}{tooltip_class}"{tooltip_attr}>{escape(label)}</span>'


def auction_current_chip(field_name: str, current_values: dict[str, str]) -> str:
    if field_name == "estimate_status":
        return auction_chip(current_values[field_name], AUCTION_ESTIMATE_META)
    if field_name == "submit_decision_status":
        return auction_chip(current_values[field_name], AUCTION_SUBMIT_DECISION_META)
    if field_name == "result_status":
        return auction_chip(current_values[field_name], AUCTION_RESULT_META)
    return '<span class="chip">Неизвестно</span>'


def estimate_chip_with_tooltip(item, current_values: dict[str, str]) -> str:
    tooltip = ""
    if current_values["estimate_status"] != "pending":
        tooltip = status_tooltip(item.estimate_status_updated_at, item.estimate_status_updated_by_name, show_unknown_author=True)
    return auction_chip(current_values["estimate_status"], AUCTION_ESTIMATE_META, tooltip)


def submit_chip_with_tooltip(item, current_values: dict[str, str]) -> str:
    tooltip = ""
    if current_values["submit_decision_status"] != "pending":
        tooltip = status_tooltip(item.submit_status_updated_at, item.submit_status_updated_by_name, show_unknown_author=True)
    return auction_chip(current_values["submit_decision_status"], AUCTION_SUBMIT_DECISION_META, tooltip)


def result_chip_with_tooltip(item, current_values: dict[str, str]) -> str:
    tooltip = ""
    if current_values["result_status"] in {"pending", "won", "recognized_winner", "lost", "rejected"}:
        tooltip = status_tooltip(item.result_status_updated_at, item.result_status_updated_by_name, show_unknown_author=True)
    return auction_chip(current_values["result_status"], AUCTION_RESULT_META, tooltip)


def auction_current_chip_with_tooltip(field_name: str, item, current_values: dict[str, str]) -> str:
    if field_name == "estimate_status":
        return estimate_chip_with_tooltip(item, current_values)
    if field_name == "submit_decision_status":
        return submit_chip_with_tooltip(item, current_values)
    if field_name == "result_status":
        return result_chip_with_tooltip(item, current_values)
    return auction_current_chip(field_name, current_values)


def normalized_auction_values(current_values: dict[str, str]) -> dict[str, str]:
    normalized = dict(current_values)
    if normalized["submit_decision_status"] != "submitted":
        normalized["result_status"] = "not_participated"
    elif normalized["result_status"] == "not_participated":
        normalized["result_status"] = "pending"
    return normalized


def result_summary(item) -> str:
    if item.result_status not in {"won", "recognized_winner", "lost"} or item.final_bid_amount is None or item.amount <= 0:
        return ""
    discount_percent = max(0.0, round(((item.amount - item.final_bid_amount) / item.amount) * 100, 2))
    return (
        f'<span class="result-meta">Цена: {escape(format_amount(item.final_bid_amount))}</span>'
        f'<span class="result-meta">Снижение: {escape(format_discount_percent(discount_percent))}</span>'
    )


def estimate_summary(item) -> str:
    if item.estimate_status != "calculated":
        return ""
    total_cost = sum(
        value or 0.0
        for value in (item.material_cost, item.work_cost, item.other_cost)
    )
    if total_cost <= 0:
        return ""
    tooltip = item.estimate_comment.strip()
    tooltip_attr = f' data-tooltip="{escape(tooltip)}"' if tooltip else ""
    tooltip_class = " auction-added-tooltip" if tooltip else ""
    return (
        f'<span class="result-meta result-meta-stack{tooltip_class}"{tooltip_attr}>'
        '<span>Итого затрат:</span>'
        f'<span>{escape(format_amount(total_cost))}</span>'
        '</span>'
    )


def advance_summary(item) -> str:
    if item.advance_percent is None or item.advance_percent <= 0:
        return '<div class="deadline-meta">Без аванса</div>'
    return f'<div class="deadline-meta">Аванс: {escape(format_percent(item.advance_percent))}</div>'


def contract_advance_summary(advance_percent: float | None, advance_amount: float) -> str:
    if advance_percent is None or advance_percent <= 0:
        return '<span class="contract-table-subtle">—</span>'
    return (
        '<div class="contract-advance-stack">'
        f'<span>Аванс: {escape(format_percent(advance_percent))}</span>'
        f'<span>{escape(format_amount(advance_amount))}</span>'
        '</div>'
    )


def added_date_meta(item) -> str:
    added_date = item.created_at.date()
    css_class = "auction-added-date is-new" if (datetime.now() - item.created_at) <= timedelta(days=1) else "auction-added-date"
    creator_name = item.created_by_name or "Автор неизвестен"
    tooltip_attrs = f' data-tooltip="Добавил: {escape(creator_name)}"'
    return f'<span class="{css_class} auction-added-tooltip"{tooltip_attrs}>Добавлен: {escape(format_date(added_date))}</span>'


def status_tooltip(updated_at: datetime | None, author_name: str, *, show_unknown_author: bool = False) -> str:
    if updated_at is None and not author_name and not show_unknown_author:
        return ""
    lines: list[str] = []
    if updated_at is not None:
        lines.append(f"Установлено: {format_datetime(updated_at)}")
    elif show_unknown_author:
        lines.append("Установлено: неизвестно")
    if author_name == "Система":
        lines.append("Установлено системой")
    elif author_name:
        lines.append(f"Автор: {author_name}")
    elif show_unknown_author:
        lines.append("Автор: неизвестен")
    return "\n".join(lines)


def stage_status_chip(stage) -> str:
    tooltip = ""
    if stage.status != "not_started":
        tooltip = status_tooltip(stage.status_updated_at, stage.status_updated_by_name, show_unknown_author=True)
    return auction_chip(stage.status, STATUS_META, tooltip)


def stage_payment_chip(stage) -> str:
    tooltip = ""
    if stage.payment_status != "unpaid":
        tooltip = status_tooltip(stage.payment_status_updated_at, stage.payment_status_updated_by_name, show_unknown_author=True)
    return auction_chip(stage.payment_status, STAGE_PAYMENT_META, tooltip)


def stage_invoice_chip(is_issued: bool, issued_at: datetime | None, issued_by_name: str) -> str:
    tooltip = status_tooltip(issued_at, issued_by_name, show_unknown_author=True) if is_issued else ""
    label = "✓" if is_issued else "○"
    css_class = "chip ok" if is_issued else "chip"
    return f'<span class="status-chip-tooltip" data-tooltip="{escape(tooltip)}"><span class="{css_class}">{label}</span></span>' if tooltip else f'<span class="{css_class}">{label}</span>'


def render_stage_invoice_form(owner_chat_id: int, contract_id: int, stage, current_user: dict | None, invoice_kind: str, active_tab: str = "detail") -> str:
    is_advance = invoice_kind == "advance"
    is_issued = stage.advance_invoice_issued if is_advance else stage.final_invoice_issued
    issued_at = stage.advance_invoice_issued_at if is_advance else stage.final_invoice_issued_at
    issued_by_name = stage.advance_invoice_issued_by_name if is_advance else stage.final_invoice_issued_by_name
    display = stage_invoice_chip(is_issued, issued_at, issued_by_name)
    if not can_edit_contract_stage_controls(current_user):
        return display
    action = f"/contracts/stages/{stage.id}/invoice-status?owner={owner_chat_id}&contract_id={contract_id}"
    return f"""
    <details class="status-menu">
      <summary>{display}</summary>
      <div class="status-popover">
        <form class="status-option-list" method="post" action="{action}">
          <input type="hidden" name="tab" value="{escape(active_tab)}">
          <input type="hidden" name="invoice_kind" value="{invoice_kind}">
          <button class="chip ok status-option" type="submit" name="issued" value="1">Счет выставлен</button>
          <button class="chip status-option" type="submit" name="issued" value="0">Счет не выставлен</button>
        </form>
      </div>
    </details>
    """


def render_stage_status_form(owner_chat_id: int, contract_id: int, stage, current_user: dict | None, active_tab: str = "detail") -> str:
    if not can_edit_contract_stage_controls(current_user):
        return stage_status_chip(stage)
    options = [
        ("not_started", "Не приступили"),
        ("in_progress", "В работе"),
        ("completed", "Выполнен"),
        ("closed_iis", "Закрыт на ИИС"),
    ]
    buttons = "".join(
        f'<button class="{STATUS_META[value][1]} status-option" type="submit" name="status" value="{value}">{escape(label)}</button>'
        for value, label in options
    )
    return f"""
    <details class="status-menu">
      <summary>{stage_status_chip(stage)}</summary>
      <div class="status-popover">
        <form class="status-option-list" method="post" action="/contracts/stages/{stage.id}/status?owner={owner_chat_id}&contract_id={contract_id}">
          <input type="hidden" name="tab" value="{escape(active_tab)}">
          {buttons}
        </form>
      </div>
    </details>
    """


def render_stage_payment_form(owner_chat_id: int, contract_id: int, stage, current_user: dict | None, active_tab: str = "detail") -> str:
    if not can_edit_contract_stage_controls(current_user):
        return stage_payment_chip(stage)
    options = [
        ("unpaid", "Не оплачено"),
        ("paid", "Оплачен"),
    ]
    buttons = "".join(
        f'<button class="{STAGE_PAYMENT_META[value][1]} status-option" type="submit" name="payment_status" value="{value}">{escape(label)}</button>'
        for value, label in options
    )
    return f"""
    <details class="status-menu">
      <summary>{stage_payment_chip(stage)}</summary>
      <div class="status-popover">
        <form class="status-option-list" method="post" action="/contracts/stages/{stage.id}/payment-status?owner={owner_chat_id}&contract_id={contract_id}">
          <input type="hidden" name="tab" value="{escape(active_tab)}">
          {buttons}
        </form>
      </div>
    </details>
    """


def render_stage_deadline_form(owner_chat_id: int, contract_id: int, stage, current_user: dict | None, active_tab: str = "detail") -> str:
    tooltip_attrs = ""
    if stage.start_date is not None:
        tooltip_attrs = f' class="deadline-value auction-added-tooltip" data-tooltip="Старт работ: {escape(format_date(stage.start_date))}"'
    else:
        tooltip_attrs = ' class="deadline-value"'
    if not can_edit_contract_stage_controls(current_user):
        return f'<span{tooltip_attrs}>{escape(format_date(stage.end_date))}</span>'
    return f"""
    <details class="status-menu">
      <summary><span{tooltip_attrs}>{escape(format_date(stage.end_date))}</span></summary>
      <div class="status-popover">
        <form class="form-grid" method="post" action="/contracts/stages/{stage.id}/deadline?owner={owner_chat_id}&contract_id={contract_id}">
          <input type="hidden" name="tab" value="{escape(active_tab)}">
          <div class="field">
            <label>Старт работ</label>
            <input type="date" name="start_date" value="{stage.start_date.isoformat() if stage.start_date is not None else ''}">
          </div>
          <div class="field">
            <label>Новый дедлайн</label>
            <input type="date" name="end_date" value="{stage.end_date.isoformat()}" required>
          </div>
          <button class="submit-btn" type="submit">Сохранить сроки</button>
        </form>
      </div>
    </details>
    """


def render_stage_amount_form(owner_chat_id: int, contract_id: int, stage, current_user: dict | None, active_tab: str = "detail") -> str:
    if not can_edit_contract_stage_controls(current_user):
        return format_amount(stage.amount)
    return f"""
    <details class="status-menu">
      <summary><span class="deadline-value">{format_amount(stage.amount)}</span></summary>
      <div class="status-popover">
        <form class="form-grid" method="post" action="/contracts/stages/{stage.id}/amount?owner={owner_chat_id}&contract_id={contract_id}">
          <input type="hidden" name="tab" value="{escape(active_tab)}">
          <div class="field">
            <label>Сумма этапа</label>
            <input type="text" name="amount" value="{escape(format_amount_input(stage.amount))}" data-money-input="1" required>
          </div>
          <button class="submit-btn" type="submit">Сохранить сумму</button>
        </form>
      </div>
    </details>
    """


def render_contract_advance_card(owner_chat_id: int, contract, payload: dict, current_user: dict | None) -> str:
    current_label = format_percent(contract.advance_percent) if contract.advance_percent else "Без аванса"
    current_note = format_amount(payload["advance_amount"]) if contract.advance_percent else "Аванс не указан"
    if not can_edit_contract_stage_controls(current_user):
        return f"""
      <article class="card stat-card">
        <div class="stat-label">Аванс</div>
        <div class="stat-value">{current_label}</div>
        <div class="stat-note">{current_note}</div>
      </article>
        """
    current_value = escape(format_amount_input(contract.advance_percent)) if contract.advance_percent else ""
    checked_attr = 'checked' if contract.advance_percent else ''
    return f"""
      <article class="card stat-card">
        <div class="stat-label">Аванс</div>
        <details class="status-menu">
          <summary>
            <div class="stat-value">{current_label}</div>
            <div class="stat-note">{current_note}</div>
          </summary>
          <div class="status-popover">
            <form class="form-grid" method="post" action="/contracts/{contract.id}/settings?owner={owner_chat_id}">
              <label class="advance-toggle">
                <input class="toggle-checkbox" type="checkbox" name="has_advance" value="1" {checked_attr}> У контракта есть аванс
              </label>
              <div class="field">
                <label>Процент аванса</label>
                <input type="text" name="advance_percent" value="{current_value}" placeholder="Например, 30">
              </div>
              <button class="submit-btn" type="submit">Сохранить аванс</button>
            </form>
          </div>
        </details>
      </article>
    """


def render_contract_signed_date_chip(owner_chat_id: int, contract, current_user: dict | None) -> str:
    label = f"Заключен: {format_date(contract.signed_date)}" if contract.signed_date is not None else "Еще не подписан"
    if not can_edit_contract_stage_controls(current_user):
        return f'<span class="chip">{escape(label)}</span>'
    checked_attr = "checked" if contract.signed_date is None else ""
    return f"""
    <details class="status-menu">
      <summary><span class="chip">{escape(label)}</span></summary>
      <div class="status-popover">
        <form class="form-grid" method="post" action="/contracts/{contract.id}/signed-date?owner={owner_chat_id}">
          <label class="advance-toggle">
            <input class="toggle-checkbox" type="checkbox" name="is_unsigned" value="1" {checked_attr}> Контракт еще не подписан
          </label>
          <div class="field signed-date-field{" is-hidden" if contract.signed_date is None else ""}">
            <label>Дата заключения контракта</label>
            <input type="date" name="signed_date" value="{contract.signed_date.isoformat() if contract.signed_date is not None else ''}">
          </div>
          <button class="submit-btn" type="submit">Сохранить дату</button>
        </form>
      </div>
    </details>
    """


def render_auction_status_form(
    owner_chat_id: int,
    auction_id: int,
    active_field: str,
    item,
    current_values: dict[str, str],
    options: list[tuple[str, str]],
    mapping: dict[str, tuple[str, str]],
    active_tab: str,
) -> str:
    if active_field == "result_status":
        if current_values["submit_decision_status"] != "submitted":
            options = [("not_participated", "Не участвовали")]
            if current_values["result_status"] != "not_participated":
                current_values = {**current_values, "result_status": "not_participated"}
        else:
            options = [item for item in options if item[0] != "not_participated"]
            if current_values["result_status"] == "not_participated":
                current_values = {**current_values, "result_status": "pending"}
    hidden_inputs = []
    for field_name in ("estimate_status", "submit_decision_status", "result_status"):
        if field_name == active_field:
            continue
        hidden_inputs.append(
            f'<input type="hidden" name="{field_name}" value="{escape(current_values[field_name])}">'
        )
    if active_field != "estimate_status":
        if item.material_cost is not None:
            hidden_inputs.append(
                f'<input type="hidden" name="material_cost" value="{escape(format_amount_input(item.material_cost))}">'
            )
        if item.work_cost is not None:
            hidden_inputs.append(
                f'<input type="hidden" name="work_cost" value="{escape(format_amount_input(item.work_cost))}">'
            )
        if item.other_cost is not None:
            hidden_inputs.append(
                f'<input type="hidden" name="other_cost" value="{escape(format_amount_input(item.other_cost))}">'
            )
        if item.estimate_comment:
            hidden_inputs.append(
                f'<input type="hidden" name="estimate_comment" value="{escape(item.estimate_comment)}">'
            )
    if active_field != "result_status" and item.final_bid_amount is not None:
        hidden_inputs.append(
            f'<input type="hidden" name="final_bid_amount" value="{escape(format_amount_input(item.final_bid_amount))}">'
        )
    option_buttons = []
    for value, label in options:
        chip_class = mapping.get(value, ("", "chip"))[1]
        option_buttons.append(
            f"""
            <button
              class="{chip_class} status-option"
              type="submit"
              data-field="{escape(active_field)}"
              data-value="{escape(value)}"
              name="{escape(active_field)}"
              value="{escape(value)}"
            >
              {escape(label)}
            </button>
            """
        )
    return f"""
    <details class="status-menu">
      <summary>{auction_current_chip_with_tooltip(active_field, item, current_values)}</summary>
      <form class="status-popover" method="post" action="/auctions/{auction_id}/status?owner={owner_chat_id}&tab={escape(active_tab)}">
        {''.join(hidden_inputs)}
        {''.join(option_buttons)}
      </form>
    </details>
    """


def render_estimate_cost_field(
    field_name: str,
    label: str,
    value: str,
    is_skipped: bool,
    needs_costs: bool,
    placeholder: str,
    skip_label: str,
) -> str:
    return f"""
        <div class="field estimate-cost-field{" is-hidden" if not needs_costs else ""}" data-estimate-field="{escape(field_name)}">
          <label>{escape(label)}</label>
          <div class="estimate-skip-row">
            <label class="estimate-skip-box">
              <input class="toggle-checkbox" type="checkbox" name="skip_{escape(field_name)}" value="1" {"checked" if is_skipped else ""}>
            </label>
            <span class="estimate-skip-label">{escape(skip_label)}</span>
          </div>
          <div class="estimate-cost-input" {"hidden" if is_skipped else ""}>
            <input type="text" name="{escape(field_name)}" value="{escape(value)}" placeholder="{escape(placeholder)}" data-money-input="1" {"required" if needs_costs and not is_skipped else ""}>
          </div>
        </div>
    """


def render_estimate_form(owner_chat_id: int, item, current_values: dict[str, str], active_tab: str) -> str:
    hidden_inputs = []
    for field_name in ("submit_decision_status", "result_status"):
        hidden_inputs.append(
            f'<input type="hidden" name="{field_name}" value="{escape(current_values[field_name])}">'
        )
    quick_buttons = []
    calculate_buttons = []
    current_estimate = current_values["estimate_status"]
    for value, label in AUCTION_ESTIMATE_OPTIONS:
        chip_class = AUCTION_ESTIMATE_META.get(value, ("", "chip"))[1]
        active_class = " is-active" if current_estimate == value else ""
        if value == "calculated":
            calculate_buttons.append(
                f"""
                <button
                  class="estimate-picker {chip_class}{active_class}"
                  type="button"
                  data-estimate-value="{escape(value)}"
                >
                  {escape(label)}
                </button>
                """
            )
        else:
            quick_buttons.append(
                f"""
                <button
                  class="{chip_class} status-option estimate-quick{active_class}"
                  type="button"
                  data-estimate-value="{escape(value)}"
                >
                  {escape(label)}
                </button>
                """
            )
    material_value = format_amount_input(item.material_cost) if item.material_cost is not None else ""
    work_value = format_amount_input(item.work_cost) if item.work_cost is not None else ""
    other_value = format_amount_input(item.other_cost) if item.other_cost is not None else ""
    comment_value = item.estimate_comment or ""
    needs_costs = current_estimate == "calculated"
    provided_values = [
        value
        for value in (item.material_cost, item.work_cost, item.other_cost)
        if value is not None
    ]
    costs_complete = bool(provided_values) and all(value > 0 for value in provided_values)
    return f"""
    <details class="status-menu result-menu">
      <summary>
        <span class="discount-value">
          {estimate_chip_with_tooltip(item, current_values)}
          {estimate_summary(item)}
        </span>
      </summary>
      <form class="status-popover estimate-form" method="post" action="/auctions/{item.id}/status?owner={owner_chat_id}&tab={escape(active_tab)}">
        {''.join(hidden_inputs)}
        <input type="hidden" name="estimate_status" value="{escape(current_estimate)}">
        <div class="result-options">
          {''.join(quick_buttons)}
        </div>
        <div class="result-options">
          {''.join(calculate_buttons)}
        </div>
        <div class="result-helper">Для статуса "Просчитан" заполните только те суммы, которые реально посчитаны. Остальные можно отметить как «не просчитано».</div>
        {render_estimate_cost_field("material_cost", "Стоимость материалов, ₽", material_value, item.material_cost is None, needs_costs, "Например, 3 250 000,00", "Материалы не просчитаны")}
        {render_estimate_cost_field("work_cost", "Стоимость работ, ₽", work_value, item.work_cost is None, needs_costs, "Например, 1 400 000,00", "Работы не просчитаны")}
        {render_estimate_cost_field("other_cost", "Прочие расходы, ₽", other_value, item.other_cost is None, needs_costs, "Например, 250 000,00", "Прочие расходы не просчитаны")}
        <div class="result-error{" is-visible" if needs_costs and not costs_complete else ""}">Заполните хотя бы одно поле расчета. Остальные можно отметить как «не просчитано».</div>
        <div class="field estimate-cost-field{" is-hidden" if not needs_costs else ""}">
          <label>Комментарий</label>
          <textarea name="estimate_comment" placeholder="Например, что заложено в расчет">{escape(comment_value)}</textarea>
        </div>
        <div class="action-row">
          <button class="submit-btn" type="submit" {"disabled" if needs_costs and not costs_complete else ""}>Сохранить расчет</button>
        </div>
      </form>
    </details>
    """


def render_result_form(owner_chat_id: int, item, current_values: dict[str, str], active_tab: str) -> str:
    options = AUCTION_RESULT_OPTIONS
    current_values = dict(current_values)
    if current_values["submit_decision_status"] != "submitted":
        options = [("not_participated", "Не участвовали")]
        if current_values["result_status"] != "not_participated":
            current_values["result_status"] = "not_participated"
    else:
        options = [option for option in options if option[0] != "not_participated"]
        if current_values["result_status"] == "not_participated":
            current_values["result_status"] = "pending"
        if item.final_bid_amount is None and current_values["result_status"] == "recognized_winner":
            current_values["result_status"] = "won"
    hidden_inputs = []
    for field_name in ("estimate_status", "submit_decision_status"):
        hidden_inputs.append(
            f'<input type="hidden" name="{field_name}" value="{escape(current_values[field_name])}">'
        )
    if item.material_cost is not None:
        hidden_inputs.append(
            f'<input type="hidden" name="material_cost" value="{escape(format_amount_input(item.material_cost))}">'
        )
    if item.work_cost is not None:
        hidden_inputs.append(
            f'<input type="hidden" name="work_cost" value="{escape(format_amount_input(item.work_cost))}">'
        )
    if item.other_cost is not None:
        hidden_inputs.append(
            f'<input type="hidden" name="other_cost" value="{escape(format_amount_input(item.other_cost))}">'
        )
    quick_buttons = []
    edit_buttons = []
    current_result = current_values["result_status"]
    for value, label in options:
        chip_class = AUCTION_RESULT_META.get(value, ("", "chip"))[1]
        active_class = " is-active" if current_result == value else ""
        if value == "recognized_winner" and item.final_bid_amount is None and current_result != "recognized_winner":
            continue
        if value in {"won", "lost"}:
            edit_buttons.append(
                f"""
                <button
                  class="result-picker {chip_class}{active_class}"
                  type="button"
                  data-result-value="{escape(value)}"
                >
                  {escape(label)}
                </button>
                """
            )
        else:
            quick_buttons.append(
                f"""
                <button
                  class="{chip_class} status-option result-quick{active_class}"
                  type="button"
                  data-result-value="{escape(value)}"
                >
                  {escape(label)}
                </button>
                """
            )
    final_amount_value = format_amount_input(item.final_bid_amount) if item.final_bid_amount is not None else ""
    needs_final_price = current_result in {"won", "lost"}
    final_price_placeholder = f"Стартовая цена: {format_amount(item.amount)}"
    helper = ""
    if item.submit_decision_status == "submitted":
        helper = '<div class="result-helper">Для статусов "Выигран" и "Проигран" укажите финальную цену аукциона. "Признан победителем" доступен после сохранения статуса "Выигран".</div>'
    summary_html = result_summary(item)
    return f"""
    <details class="status-menu result-menu">
      <summary>
        <span class="discount-value">
          {result_chip_with_tooltip(item, current_values)}
          {summary_html}
        </span>
      </summary>
      <form class="status-popover result-form" method="post" action="/auctions/{item.id}/status?owner={owner_chat_id}&tab={escape(active_tab)}">
        {''.join(hidden_inputs)}
        <input type="hidden" name="result_status" value="{escape(current_result)}">
        <div class="result-options">
          {''.join(quick_buttons)}
        </div>
        <div class="result-options">
          {''.join(edit_buttons)}
        </div>
        {helper}
        <div class="field result-price-field{" is-hidden" if not needs_final_price else ""}">
          <label>Цена аукциона, ₽</label>
          <input type="text" name="final_bid_amount" value="{escape(final_amount_value)}" placeholder="{escape(final_price_placeholder)}" data-money-input="1" {"required" if needs_final_price else ""}>
          <div class="result-error{" is-visible" if needs_final_price and not final_amount_value else ""}">Введите цену победы на аукционе.</div>
        </div>
        <div class="action-row">
          <button class="submit-btn" type="submit" {"disabled" if needs_final_price and not final_amount_value else ""}>Сохранить итог</button>
        </div>
      </form>
    </details>
    """


def render_discount_form(
    owner_chat_id: int,
    item,
    auction_id: int,
    amount: float,
    current_discount: float | None,
    current_min_amount: float | None,
    is_locked: bool,
    is_required: bool,
    active_tab: str,
) -> str:
    current_label = '<span class="chip">Не установлено</span>'
    percent_value = ""
    min_amount_value = ""
    tooltip = ""
    if current_discount is not None and current_min_amount is not None:
        tooltip = status_tooltip(item.max_discount_updated_at, item.max_discount_updated_by_name, show_unknown_author=True) if item is not None else ""
        tooltip_attr = f' data-tooltip="{escape(tooltip)}"' if tooltip else ""
        tooltip_class = " status-chip-tooltip" if tooltip else ""
        current_label = (
            f'<span class="discount-percent{tooltip_class}"{tooltip_attr}>{escape(format_discount_percent(current_discount))}</span>'
            f'<span class="discount-amount">{escape(format_amount(current_min_amount))}</span>'
        )
        percent_value = str(current_discount).replace(".", ",")
        min_amount_value = format_amount_input(current_min_amount)
    elif is_required:
        current_label = '<span class="chip danger">Не установлено</span><span class="deadline-meta">Установите максимальное<br>снижение</span>'
    if is_locked:
        return f'<span class="discount-value">{current_label}</span>'
    return f"""
    <details class="status-menu discount-menu">
      <summary><span class="discount-value">{current_label}</span></summary>
      <form
        class="status-popover discount-form"
        method="post"
        action="/auctions/{auction_id}/discount?owner={owner_chat_id}&tab={escape(active_tab)}"
        data-base-amount="{amount}"
      >
        <div class="field">
          <label>Макс. снижение, %</label>
          <input type="text" name="discount_percent" value="{escape(percent_value)}" placeholder="Например, 17,5">
        </div>
        <div class="field">
          <label>Минимальная сумма, ₽</label>
          <input type="text" name="min_amount" value="{escape(min_amount_value)}" placeholder="Например, 15325000,00" data-money-input="1">
        </div>
        <div class="action-row">
          <button class="submit-btn" type="submit">Установить максимальное снижение</button>
          <button class="secondary-btn" type="submit" name="clear_discount" value="1">Очистить</button>
        </div>
      </form>
    </details>
    """


def render_deadline_form(owner_chat_id: int, auction_id: int, current_date: date, note: str, is_danger: bool, active_tab: str) -> str:
    note_block = ""
    if note:
        note_class = "deadline-meta danger" if is_danger else "deadline-meta"
        note_block = f'<div class="{note_class}">{escape(note)}</div>'
    return f"""
    <details class="status-menu">
      <summary>
        <div>{format_date(current_date)}</div>
        {note_block}
      </summary>
      <form class="status-popover deadline-form" method="post" action="/auctions/{auction_id}/deadline?owner={owner_chat_id}&tab={escape(active_tab)}">
        <div class="field">
          <label>Крайняя дата подачи</label>
          <input type="date" name="bid_deadline" value="{current_date.isoformat()}">
        </div>
        <div class="action-row">
          <button class="submit-btn" type="submit">Сохранить дату</button>
        </div>
      </form>
    </details>
    """


def render_deadline_display(current_date: date, note: str, is_danger: bool) -> str:
    note_block = ""
    if note:
        note_class = "deadline-meta danger" if is_danger else "deadline-meta"
        note_block = f'<div class="{note_class}">{escape(note)}</div>'
    return f"""
    <div>
      <div>{format_date(current_date)}</div>
      {note_block}
    </div>
    """


def render_amount_form(owner_chat_id: int, auction_id: int, item, active_tab: str) -> str:
    return f"""
    <details class="status-menu">
      <summary><span class="amount-value">{format_amount(item.amount)}</span>{advance_summary(item)}</summary>
      <form class="status-popover amount-form" method="post" action="/auctions/{auction_id}/amount?owner={owner_chat_id}&tab={escape(active_tab)}">
        <div class="field">
          <label>Сумма аукциона, ₽</label>
          <input type="text" name="amount" value="{format_amount_input(item.amount)}" data-money-input="1">
        </div>
        <label class="advance-toggle">
          <input class="toggle-checkbox" type="checkbox" name="has_advance" value="1" {"checked" if item.advance_percent is not None else ""}> У аукциона есть аванс
        </label>
        <div class="field advance-field{" is-hidden" if item.advance_percent is None else ""}">
          <label>Процент аванса</label>
          <input type="text" name="advance_percent" value="{escape('' if item.advance_percent is None else str(item.advance_percent).replace('.', ','))}" placeholder="Например, 30">
        </div>
        <div class="action-row">
          <button class="submit-btn" type="submit">Сохранить сумму</button>
        </div>
      </form>
    </details>
    """


def render_amount_display(item) -> str:
    return (
        f'<span class="amount-value">{format_amount(item.amount)}</span>'
        f'{advance_summary(item)}'
    )


def render_auction_details_form(owner_chat_id: int, item, active_tab: str, row_number: int) -> str:
    source_link = (
        f'<a href="{escape(item.source_url)}" target="_blank" rel="noreferrer">{escape(item.auction_number)}</a>'
        if item.source_url
        else escape(item.auction_number)
    )
    return f"""
    <details class="status-menu lot-menu">
      <summary>
        <div class="auction-head">
          <div class="auction-seq">#{item.registry_position}</div>
          <div class="auction-number">№ {source_link}</div>
        </div>
        <div class="timeline-title">{escape(item.title)}</div>
      </summary>
      <form class="status-popover lot-form" method="post" action="/auctions/{item.id}/details?owner={owner_chat_id}&tab={escape(active_tab)}">
        <div class="field">
          <label>Номер аукциона</label>
          <input type="text" name="auction_number" value="{escape(item.auction_number)}" inputmode="numeric" pattern="[0-9]+" required>
        </div>
        <div class="field">
          <label>Название аукциона</label>
          <input type="text" name="title" value="{escape(item.title)}" required>
        </div>
        <div class="field">
          <label>Город</label>
          <input type="text" name="city" value="{escape(item.city)}" required>
        </div>
        <div class="field">
          <label>Ссылка на аукцион</label>
          <input type="text" name="source_url" value="{escape(item.source_url)}" placeholder="https://zakupki.gov.ru/...">
        </div>
        <label class="advance-toggle">
          <input class="toggle-checkbox" type="checkbox" name="has_advance" value="1" {"checked" if item.advance_percent is not None else ""}>
          У аукциона есть аванс
        </label>
        <div class="field advance-field{" is-hidden" if item.advance_percent is None else ""}">
          <label>Процент аванса</label>
          <input type="text" name="advance_percent" value="{escape('' if item.advance_percent is None else str(item.advance_percent).replace('.', ','))}" placeholder="Например, 30">
        </div>
        <div class="field">
          <label>Дата добавления в реестр</label>
          <input type="date" name="created_date" value="{item.created_at.date().isoformat()}">
        </div>
        <div class="action-row">
          <button class="submit-btn" type="submit">Сохранить аукцион</button>
        </div>
      </form>
    </details>
    """


def render_auction_details_display(item, row_number: int) -> str:
    source_link = (
        f'<a href="{escape(item.source_url)}" target="_blank" rel="noreferrer">{escape(item.auction_number)}</a>'
        if item.source_url
        else escape(item.auction_number)
    )
    return f"""
    <div>
      <div class="auction-head">
        <div class="auction-seq">#{item.registry_position}</div>
        <div class="auction-number">№ {source_link}</div>
      </div>
      <div class="timeline-title">{escape(item.title)}</div>
    </div>
    """


def render_discount_display(
    item,
    amount: float,
    current_discount: float | None,
    current_min_amount: float | None,
    is_required: bool,
) -> str:
    if current_discount is not None and current_min_amount is not None:
        tooltip = status_tooltip(item.max_discount_updated_at, item.max_discount_updated_by_name, show_unknown_author=True)
        tooltip_attr = f' data-tooltip="{escape(tooltip)}"' if tooltip else ""
        tooltip_class = " status-chip-tooltip" if tooltip else ""
        return (
            f'<span class="discount-value">'
            f'<span class="discount-percent{tooltip_class}"{tooltip_attr}>{escape(format_discount_percent(current_discount))}</span>'
            f'<span class="discount-amount">{escape(format_amount(current_min_amount))}</span>'
            f'</span>'
        )
    if is_required:
        return '<span class="discount-value"><span class="chip danger">Не установлено</span><span class="deadline-meta">Установите максимальное<br>снижение</span></span>'
    return '<span class="discount-value"><span class="chip">Не установлено</span></span>'


def render_submit_for_procurement(owner_chat_id: int, item, current_values: dict[str, str], active_tab: str) -> str:
    if item.submit_decision_status != "approved":
        return submit_chip_with_tooltip(item, current_values)
    hidden_inputs = [
        f'<input type="hidden" name="estimate_status" value="{escape(current_values["estimate_status"])}">',
        f'<input type="hidden" name="result_status" value="{escape(current_values["result_status"])}">',
    ]
    if item.material_cost is not None:
        hidden_inputs.append(
            f'<input type="hidden" name="material_cost" value="{escape(format_amount_input(item.material_cost))}">'
        )
    if item.work_cost is not None:
        hidden_inputs.append(
            f'<input type="hidden" name="work_cost" value="{escape(format_amount_input(item.work_cost))}">'
        )
    if item.other_cost is not None:
        hidden_inputs.append(
            f'<input type="hidden" name="other_cost" value="{escape(format_amount_input(item.other_cost))}">'
        )
    return f"""
    <details class="status-menu">
      <summary>{submit_chip_with_tooltip(item, current_values)}</summary>
      <form class="status-popover" method="post" action="/auctions/{item.id}/status?owner={owner_chat_id}&tab={escape(active_tab)}">
        {''.join(hidden_inputs)}
        <button class="{AUCTION_SUBMIT_DECISION_META['submitted'][1]} status-option" type="submit" name="submit_decision_status" value="submitted">
          {AUCTION_SUBMIT_DECISION_META['submitted'][0]}
        </button>
      </form>
    </details>
    """


def render_estimate_for_supply(owner_chat_id: int, item, current_values: dict[str, str], active_tab: str) -> str:
    if item.estimate_status not in {"approved", "calculated", "not_calculated"}:
        return estimate_chip_with_tooltip(item, current_values) + estimate_summary(item)
    hidden_inputs = [
        f'<input type="hidden" name="submit_decision_status" value="{escape(current_values["submit_decision_status"])}">',
        f'<input type="hidden" name="result_status" value="{escape(current_values["result_status"])}">',
    ]
    if item.material_cost is not None:
        hidden_inputs.append(
            f'<input type="hidden" name="material_cost" value="{escape(format_amount_input(item.material_cost))}">'
        )
    if item.work_cost is not None:
        hidden_inputs.append(
            f'<input type="hidden" name="work_cost" value="{escape(format_amount_input(item.work_cost))}">'
        )
    if item.other_cost is not None:
        hidden_inputs.append(
            f'<input type="hidden" name="other_cost" value="{escape(format_amount_input(item.other_cost))}">'
        )
    if item.estimate_comment:
        hidden_inputs.append(
            f'<input type="hidden" name="estimate_comment" value="{escape(item.estimate_comment)}">'
        )
    if item.final_bid_amount is not None:
        hidden_inputs.append(
            f'<input type="hidden" name="final_bid_amount" value="{escape(format_amount_input(item.final_bid_amount))}">'
        )
    material_value = format_amount_input(item.material_cost) if item.material_cost is not None and current_values['estimate_status'] == 'calculated' else ""
    work_value = format_amount_input(item.work_cost) if item.work_cost is not None and current_values['estimate_status'] == 'calculated' else ""
    other_value = format_amount_input(item.other_cost) if item.other_cost is not None and current_values['estimate_status'] == 'calculated' else ""
    provided_values = [
        value
        for value in (item.material_cost, item.work_cost, item.other_cost)
        if value is not None
    ]
    costs_complete = bool(provided_values) and all(value > 0 for value in provided_values)
    return f"""
    <details class="status-menu estimate-menu">
      <summary>
        <span class="discount-value">
          {estimate_chip_with_tooltip(item, current_values)}
          {estimate_summary(item)}
        </span>
      </summary>
      <form class="status-popover estimate-form" method="post" action="/auctions/{item.id}/status?owner={owner_chat_id}&tab={escape(active_tab)}">
        {''.join(hidden_inputs)}
        <input type="hidden" name="estimate_status" value="{escape(current_values["estimate_status"])}">
        <div class="result-options">
          <button class="estimate-picker {AUCTION_ESTIMATE_META['calculated'][1]}{' is-active' if current_values['estimate_status'] == 'calculated' else ''}" type="button" data-estimate-value="calculated">Просчитан</button>
          <button class="estimate-picker {AUCTION_ESTIMATE_META['not_calculated'][1]}{' is-active' if current_values['estimate_status'] == 'not_calculated' else ''}" type="button" data-estimate-value="not_calculated">Не просчитан</button>
        </div>
        {render_estimate_cost_field("material_cost", "Стоимость материалов, ₽", material_value, item.material_cost is None, current_values['estimate_status'] == 'calculated', "Введите сумму материалов", "Материалы не просчитаны")}
        {render_estimate_cost_field("work_cost", "Стоимость работ, ₽", work_value, item.work_cost is None, current_values['estimate_status'] == 'calculated', "Введите стоимость работ", "Работы не просчитаны")}
        {render_estimate_cost_field("other_cost", "Прочие расходы, ₽", other_value, item.other_cost is None, current_values['estimate_status'] == 'calculated', "Введите прочие расходы", "Прочие расходы не просчитаны")}
        <div class="result-error{' is-visible' if current_values['estimate_status'] == 'calculated' and not costs_complete else ''}">Заполните хотя бы одно поле расчета. Остальные можно отметить как «не просчитано».</div>
        <div class="field estimate-cost-field{' is-hidden' if current_values['estimate_status'] != 'calculated' else ''}">
          <label>Комментарий</label>
          <textarea name="estimate_comment" placeholder="Например, что входит в расчет">{escape(item.estimate_comment or '')}</textarea>
        </div>
        <div class="action-row">
          <button class="submit-btn" type="submit" {'disabled' if current_values['estimate_status'] == 'calculated' and not costs_complete else ''}>Сохранить расчет</button>
        </div>
      </form>
    </details>
    """


def render_estimate_for_management(owner_chat_id: int, item, current_values: dict[str, str], active_tab: str) -> str:
    if item.estimate_status == "calculated":
        hidden_inputs = [
            f'<input type="hidden" name="submit_decision_status" value="{escape(current_values["submit_decision_status"])}">',
            f'<input type="hidden" name="result_status" value="{escape(current_values["result_status"])}">',
            '<input type="hidden" name="estimate_status" value="calculated">',
        ]
        if item.estimate_comment:
            hidden_inputs.append(
                f'<input type="hidden" name="estimate_comment" value="{escape(item.estimate_comment)}">'
            )
        if item.final_bid_amount is not None:
            hidden_inputs.append(
                f'<input type="hidden" name="final_bid_amount" value="{escape(format_amount_input(item.final_bid_amount))}">'
            )
        material_value = format_amount_input(item.material_cost) if item.material_cost is not None else ""
        work_value = format_amount_input(item.work_cost) if item.work_cost is not None else ""
        other_value = format_amount_input(item.other_cost) if item.other_cost is not None else ""
        comment_value = item.estimate_comment or ""
        provided_values = [
            value
            for value in (item.material_cost, item.work_cost, item.other_cost)
            if value is not None
        ]
        costs_complete = bool(provided_values) and all(value > 0 for value in provided_values)
        return f"""
        <details class="status-menu estimate-menu">
          <summary>
            <span class="discount-value">
              {estimate_chip_with_tooltip(item, current_values)}
              {estimate_summary(item)}
            </span>
          </summary>
          <form class="status-popover estimate-form" method="post" action="/auctions/{item.id}/status?owner={owner_chat_id}&tab={escape(active_tab)}">
            {''.join(hidden_inputs)}
            {render_estimate_cost_field("material_cost", "Стоимость материалов, ₽", material_value, item.material_cost is None, True, "Введите сумму материалов", "Материалы не просчитаны")}
            {render_estimate_cost_field("work_cost", "Стоимость работ, ₽", work_value, item.work_cost is None, True, "Введите стоимость работ", "Работы не просчитаны")}
            {render_estimate_cost_field("other_cost", "Прочие расходы, ₽", other_value, item.other_cost is None, True, "Введите прочие расходы", "Прочие расходы не просчитаны")}
            <div class="result-error{' is-visible' if not costs_complete else ''}">Заполните хотя бы одно поле расчета. Остальные можно отметить как «не просчитано».</div>
            <div class="field estimate-cost-field">
              <label>Комментарий</label>
              <textarea name="estimate_comment" placeholder="Например, что входит в расчет">{escape(comment_value)}</textarea>
            </div>
            <div class="action-row">
              <button class="submit-btn" type="submit" {'disabled' if not costs_complete else ''}>Сохранить расчет</button>
            </div>
          </form>
        </details>
        """
    return render_auction_status_form(
        owner_chat_id,
        item.id,
        "estimate_status",
        item,
        current_values,
        [("pending", "Не решено"), ("approved", "Считать"), ("rejected", "Не считать")],
        AUCTION_ESTIMATE_META,
        active_tab,
    )


def render_submit_for_management(owner_chat_id: int, item, current_values: dict[str, str], active_tab: str) -> str:
    if item.submit_decision_status == "submitted":
        return submit_chip_with_tooltip(item, current_values)
    return render_auction_status_form(
        owner_chat_id,
        item.id,
        "submit_decision_status",
        item,
        current_values,
        [("pending", "Не решено"), ("approved", "Подавать заявку"), ("rejected", "Не подаемся")],
        AUCTION_SUBMIT_DECISION_META,
        active_tab,
    )


def render_auction_delete_actions(owner_chat_id: int, item, active_tab: str, current_user: dict | None) -> str:
    if current_user is None:
        return ""
    if active_tab == "deleted":
        if not has_permission(current_user, "auctions", "view"):
            return ""
    elif is_procurement_user(current_user) or is_supply_user(current_user) or is_management_user(current_user) or not has_permission(current_user, "auctions", "edit"):
        return ""
    delete_icon = """
    <svg class="icon-trash" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M4 7h16" />
      <path d="M10 11v6" />
      <path d="M14 11v6" />
      <path d="M9 4h6l1 2H8l1-2Z" />
      <path d="M7 7h10l-1 12a2 2 0 0 1-2 2h-4a2 2 0 0 1-2-2L7 7Z" />
    </svg>
    """
    restore_icon = """
    <svg class="icon-restore" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M9 7H5v4" />
      <path d="M5 11a7 7 0 1 0 2-4" />
    </svg>
    """
    if active_tab == "deleted":
        purge_action = ""
        if has_active_admin_mode(current_user):
            purge_action = f"""
            <form class="auction-delete-form" method="post" action="/auctions/{item.id}/purge?owner={owner_chat_id}&tab={escape(active_tab)}">
              <button class="icon-btn danger" type="submit" title="Удалить навсегда">{delete_icon}</button>
            </form>
            """
        return f"""
        <form class="auction-delete-form" method="post" action="/auctions/{item.id}/restore?owner={owner_chat_id}&tab={escape(active_tab)}">
          <button class="icon-btn" type="submit" title="Вернуть в реестр">{restore_icon}</button>
        </form>
        {purge_action}
        """
    return f"""
    <form class="auction-delete-form" method="post" action="/auctions/{item.id}/delete?owner={owner_chat_id}&tab={escape(active_tab)}">
      <button class="icon-btn danger" type="submit" title="Переместить в удаленные">{delete_icon}</button>
    </form>
    """


def render_auction_row(item, owner_chat_id: int, active_tab: str, row_number: int, current_user: dict | None = None) -> str:
    current_values = normalized_auction_values({
        "estimate_status": item.estimate_status,
        "submit_decision_status": item.submit_decision_status,
        "result_status": item.result_status,
    })
    procurement_user = is_procurement_user(current_user)
    supply_user = is_supply_user(current_user)
    management_user = is_management_user(current_user)
    restricted_user = procurement_user or supply_user or management_user
    days_left = (item.bid_deadline - date.today()).days
    show_deadline_note = days_left > 0
    is_deadline_danger = (
        item.submit_decision_status not in {"submitted", "rejected"}
        and 0 < days_left <= 2
    )
    if not show_deadline_note:
        deadline_note = ""
    else:
        deadline_note = f"Осталось {days_left} дн."
    lot_cell = render_auction_details_display(item, row_number) if restricted_user else render_auction_details_form(owner_chat_id, item, active_tab, row_number)
    deadline_cell = render_deadline_display(item.bid_deadline, deadline_note, is_deadline_danger) if restricted_user else render_deadline_form(owner_chat_id, item.id, item.bid_deadline, deadline_note, is_deadline_danger, active_tab)
    amount_cell = render_amount_display(item) if restricted_user else render_amount_form(owner_chat_id, item.id, item, active_tab)
    if procurement_user:
        estimate_cell = estimate_chip_with_tooltip(item, current_values) + estimate_summary(item)
    elif supply_user:
        estimate_cell = render_estimate_for_supply(owner_chat_id, item, current_values, active_tab)
    elif management_user:
        estimate_cell = render_estimate_for_management(owner_chat_id, item, current_values, active_tab)
    else:
        estimate_cell = render_estimate_form(owner_chat_id, item, current_values, active_tab)
    if procurement_user:
        submit_cell = render_submit_for_procurement(owner_chat_id, item, current_values, active_tab)
    elif supply_user:
        submit_cell = submit_chip_with_tooltip(item, current_values)
    elif management_user:
        submit_cell = render_submit_for_management(owner_chat_id, item, current_values, active_tab)
    else:
        submit_cell = render_auction_status_form(owner_chat_id, item.id, "submit_decision_status", item, current_values, AUCTION_SUBMIT_OPTIONS, AUCTION_SUBMIT_DECISION_META, active_tab)
    discount_cell = render_discount_display(item, item.amount, item.max_discount_percent, item.min_bid_amount, item.submit_decision_status == "submitted" and item.max_discount_percent is None) if (procurement_user or supply_user) else render_discount_form(owner_chat_id, item, item.id, item.amount, item.max_discount_percent, item.min_bid_amount, item.submit_decision_status in {"pending", "rejected"}, item.submit_decision_status == "submitted" and item.max_discount_percent is None, active_tab)
    result_cell = render_result_form(owner_chat_id, item, current_values, active_tab) if procurement_user else result_chip_with_tooltip(item, current_values) + result_summary(item) if (supply_user or management_user) else render_result_form(owner_chat_id, item, current_values, active_tab)
    row_actions = render_auction_delete_actions(owner_chat_id, item, active_tab, current_user)
    return f"""
    <tr id="auction-{item.id}" data-auction-row="{item.id}">
      <td>
        {lot_cell}
        <div class="auction-lot-meta">
          <span class="contract-meta">{escape(item.city)}</span>
          <span class="auction-row-actions">
            {added_date_meta(item)}
            {row_actions}
          </span>
        </div>
      </td>
      <td class="nowrap">{deadline_cell}</td>
      <td class="nowrap">{amount_cell}</td>
      <td><div class="auction-cell-center">{estimate_cell}</div></td>
      <td><div class="auction-cell-center">{submit_cell}</div></td>
      <td><div class="auction-cell-center">{discount_cell}</div></td>
      <td><div class="auction-cell-center">{result_cell}</div></td>
    </tr>
    """


def render_auction_rows(auctions, owner_chat_id: int, active_tab: str, current_user: dict | None = None) -> str:
    return "".join(
        render_auction_row(item, owner_chat_id, active_tab, row_number, current_user)
        for row_number, item in enumerate(auctions, start=1)
    )


SECTIONS = [
    ("contracts", "Контракты", "/contracts"),
    ("auctions", "Аукционы", "/auctions"),
    ("payables", "Кредиторка", "/payables"),
    ("expenses", "Расходы компании", "/expenses"),
    ("payroll", "Зарплата", "/payroll"),
    ("finance", "Финансовый анализ", "/finance-analysis"),
    ("access", "Доступы", "/access"),
]

SECTION_LABELS = {section_id: label for section_id, label, _ in SECTIONS}


SECTION_HERO = {
    "contracts": (
        "Контракты",
        "Раздел для строительных контрактов: этапы, оплаты, дедлайны, долги и общая картина по каждому объекту.",
    ),
    "auctions": (
        "Аукционы",
        "Здесь постепенно соберем воронку тендеров: анализ закупок, статусы участия, шансы на победу и плановые суммы.",
    ),
    "payables": (
        "Кредиторка",
        "Реестр кредиторской задолженности: кому должны, по какому документу, на какой объект, до какого срока и что уже закрыто оплатой.",
    ),
    "expenses": (
        "Расходы компании",
        "Отдельный блок для учета постоянных и проектных расходов, чтобы видеть не только выручку, но и реальную денежную нагрузку.",
    ),
    "payroll": (
        "Зарплата",
        "Модуль для сотрудников, начислений, выплат, авансов и связки зарплат с объектами и этапами.",
    ),
    "finance": (
        "Финансовый анализ",
        "Сводный аналитический слой: cashflow, долг, маржинальность, динамика оплат и управленческие выводы по бизнесу.",
    ),
    "access": (
        "Доступы",
        "Здесь администратор управляет web-пользователями, их ролями и правами по каждому разделу будущей CRM.",
    ),
}


def layout(
    title: str,
    body: str,
    owners: Iterable[int],
    current_owner: int | None,
    active_section: str,
    current_user: dict | None = None,
) -> str:
    current_preview_options = current_user.get("preview_role_options", ROLE_PREVIEW_OPTIONS) if current_user else ROLE_PREVIEW_OPTIONS
    sidebar_notes = ""
    if has_active_admin_mode(current_user):
        sidebar_notes = """
      <div class="sidebar-note">
        Контракты уже рабочие. Доступы тоже начинаем собирать всерьез, чтобы дальше не городить роли поверх хаоса.
      </div>
      <div class="sidebar-note">
        Локальный прототип. Дальше можно вынести это в общий backend и подключить вместе с Telegram.
      </div>
        """
    visible_sections = [
        (section_id, label, href)
        for section_id, label, href in SECTIONS
        if current_user is None or has_permission(current_user, section_id, "view")
    ]
    nav_links = "".join(
        f'<a class="nav-link{" active" if section_id == active_section else ""}" href="{href}">{label}</a>'
        for section_id, label, href in visible_sections
    )
    hero_title, hero_copy = SECTION_HERO.get(active_section, SECTION_HERO["contracts"])
    notification_panel = ""
    if current_user:
        notification = current_user.get("role_notifications", {})
        count = int(notification.get("count", 0) or 0)
        badge = f'<span class="notification-badge">{count}</span>' if count > 0 else ""
        items_html = "".join(
            f'''
            <div class="notification-task">
              <div class="notification-task-title">{escape(str(item.get("title", "")))}</div>
              {
                f'<div class="notification-task-kind">{escape(str(item.get("kind", "")))}</div>'
                if item.get("kind")
                else ""
              }
              <div class="notification-task-meta">До {escape(str(item.get("deadline", "")))} · осталось {escape(str(item.get("days_left", "")))} дн.</div>
            </div>
            '''
            for item in notification.get("items", [])
        )
        footer_html = (
            f'<a class="notification-link" href="{escape(str(notification.get("href", "/auctions")))}">Перейти к задачам</a>'
            if notification.get("href") and notification.get("count", 0)
            else ""
        )
        notification_panel = f"""
        <details class="notification-menu">
          <summary class="notification-btn{' active' if notification.get('active') else ''}" title="Уведомления">
            <svg class="notification-icon" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
              <path d="M12 4a4 4 0 0 0-4 4v1.3c0 .8-.24 1.58-.68 2.24L6 13.5V15h12v-1.5l-1.32-1.96A4 4 0 0 1 16 9.3V8a4 4 0 0 0-4-4Z" />
              <path d="M10 18a2 2 0 0 0 4 0" />
            </svg>
            {badge}
          </summary>
          <div class="notification-popover">
            <div class="notification-title">{escape(str(notification.get("title", "Уведомления")))}</div>
            <div class="notification-line">{escape(str(notification.get("primary", "")))}</div>
            {
              f'<div class="notification-line">{escape(str(notification.get("secondary", "")))}</div>'
              if notification.get("secondary")
              else ""
            }
            {
              f'<div class="notification-line">{escape(str(notification.get("tertiary", "")))}</div>'
              if notification.get("tertiary")
              else ""
            }
            {f'<div class="notification-task-list">{items_html}</div>' if items_html else ""}
            {footer_html}
          </div>
        </details>
        """
    user_panel = (
        f"""
        <div class="current-user-card">
          <div class="current-user-top">
            <div class="current-user-name">{escape(current_user["full_name"])}</div>
            {notification_panel}
          </div>
          <div class="current-user-email">{escape(current_user["login"])}</div>
          {
            f'''
          <div class="preview-note">Тестовый режим роли: {escape(current_user["preview_role_label"])}</div>
          '''
            if current_user.get("preview_role_name")
            else ""
          }
          {
            f'''
          <form class="preview-form" method="post" action="/role-preview">
            <input type="hidden" name="next_path" value="">
            <label class="current-user-email">Режим просмотра роли</label>
            <select name="preview_role">
              {
                ''.join(
                    f'<option value="{escape(value)}" {"selected" if current_user.get("preview_role_name", "") == value else ""}>{escape(label)}</option>'
                    for value, label in current_preview_options
                )
              }
            </select>
            <button class="preview-btn" type="submit">Применить режим</button>
          </form>
          '''
            if current_user.get("is_super_admin")
            else ""
          }
          <a class="logout-link" href="/logout">Выйти</a>
        </div>
        """
        if current_user
        else """
        <div class="current-user-card">
          <div class="current-user-name">Авторизация</div>
          <div class="current-user-email">Войдите, чтобы видеть данные</div>
        </div>
        """
    )
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <style>
    :root {{
      --bg: #f4efe7;
      --paper: #fffaf2;
      --ink: #16232f;
      --muted: #6f7a82;
      --line: #d7ccbc;
      --brand: #1d5c63;
      --brand-2: #d97706;
      --ok: #2b9348;
      --warn: #b7791f;
      --danger: #b83232;
      --card-shadow: 0 20px 45px rgba(22, 35, 47, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      color: var(--ink);
      overflow-x: hidden;
      background:
        radial-gradient(circle at top left, rgba(217, 119, 6, 0.16), transparent 22%),
        radial-gradient(circle at top right, rgba(29, 92, 99, 0.18), transparent 28%),
        linear-gradient(180deg, #f6f0e8 0%, #efe8de 100%);
    }}
    a {{ color: inherit; text-decoration: none; }}
    .shell {{
      width: min(1440px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 22px 0 40px;
      display: grid;
      grid-template-columns: 280px minmax(0, 1fr);
      gap: 22px;
    }}
    .sidebar {{
      background: linear-gradient(180deg, rgba(22,35,47,0.98), rgba(29,92,99,0.96));
      color: white;
      border-radius: 28px;
      padding: 24px 18px;
      box-shadow: var(--card-shadow);
      position: sticky;
      top: 18px;
      align-self: start;
      min-height: calc(100vh - 48px);
      display: grid;
      grid-template-rows: auto auto 1fr auto;
      gap: 18px;
    }}
    .brand-mark {{
      font-size: 12px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      opacity: 0.7;
    }}
    .brand-title {{
      font-size: 28px;
      font-weight: 800;
      line-height: 0.95;
      margin-top: 8px;
    }}
    .brand-sub {{
      color: rgba(255,255,255,0.72);
      font-size: 14px;
      line-height: 1.5;
      margin-top: 10px;
    }}
    .nav {{
      display: grid;
      gap: 8px;
    }}
    .nav-link {{
      display: block;
      padding: 14px 14px;
      border-radius: 18px;
      color: rgba(255,255,255,0.8);
      font-weight: 600;
      border: 1px solid transparent;
      background: rgba(255,255,255,0.04);
    }}
    .nav-link.active {{
      color: white;
      background: rgba(255,255,255,0.14);
      border-color: rgba(255,255,255,0.16);
    }}
    .sidebar-note {{
      padding: 16px;
      border-radius: 20px;
      background: rgba(255,255,255,0.08);
      color: rgba(255,255,255,0.76);
      font-size: 13px;
      line-height: 1.5;
    }}
    .content {{
      min-width: 0;
    }}
    .hero {{
      background: linear-gradient(135deg, rgba(29, 92, 99, 0.98), rgba(22, 35, 47, 0.98));
      color: white;
      border-radius: 28px;
      padding: 28px;
      box-shadow: var(--card-shadow);
      display: grid;
      gap: 18px;
      position: relative;
      z-index: 5;
      overflow: visible;
    }}
    .hero-top {{
      display: flex;
      gap: 16px;
      justify-content: space-between;
      align-items: flex-start;
      flex-wrap: wrap;
    }}
    .eyebrow {{
      font-size: 12px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      opacity: 0.72;
      margin-bottom: 10px;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(32px, 5vw, 52px);
      line-height: 0.96;
      max-width: 760px;
    }}
    .hero-copy {{
      max-width: 760px;
      color: rgba(255, 255, 255, 0.78);
      font-size: 16px;
      line-height: 1.5;
    }}
    .owner-switch {{
      display: flex;
      justify-content: flex-end;
      align-items: flex-start;
    }}
    .current-user-card {{
      min-width: 240px;
      padding: 12px 14px;
      border-radius: 18px;
      border: 1px solid rgba(255,255,255,0.22);
      background: rgba(255,255,255,0.08);
      text-align: right;
      backdrop-filter: blur(10px);
    }}
    .current-user-top {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
    }}
    .current-user-name {{
      font-size: 15px;
      font-weight: 700;
    }}
    .current-user-email {{
      margin-top: 4px;
      font-size: 12px;
      color: rgba(255,255,255,0.82);
    }}
    .preview-note {{
      margin-top: 6px;
      font-size: 12px;
      color: #fff3c4;
    }}
    .preview-form {{
      margin-top: 10px;
      display: grid;
      gap: 8px;
    }}
    .preview-form select {{
      width: 100%;
      border-radius: 12px;
      border: 1px solid rgba(255,255,255,0.22);
      background: rgba(255,255,255,0.12);
      color: white;
      padding: 8px 10px;
      font: inherit;
    }}
    .preview-form option {{
      color: var(--ink);
    }}
    .preview-btn {{
      border: 1px solid rgba(255,255,255,0.22);
      border-radius: 12px;
      background: rgba(255,255,255,0.12);
      color: white;
      padding: 8px 10px;
      font: inherit;
      cursor: pointer;
    }}
    .logout-link {{
      display: inline-block;
      margin-top: 10px;
      font-size: 12px;
      border-bottom: 1px dashed rgba(255,255,255,0.45);
    }}
    .notification-menu {{
      position: relative;
      display: inline-block;
      flex: 0 0 auto;
      z-index: 80;
    }}
    .notification-menu[open] {{
      z-index: 120;
    }}
    .notification-menu summary {{
      list-style: none;
    }}
    .notification-menu summary::-webkit-details-marker {{
      display: none;
    }}
    .notification-btn {{
      width: 40px;
      height: 40px;
      border-radius: 999px;
      border: 1px solid rgba(255,255,255,0.18);
      background: rgba(255,255,255,0.10);
      color: white;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      position: relative;
      cursor: pointer;
    }}
    .notification-btn.active {{
      background: rgba(255, 214, 102, 0.22);
      border-color: rgba(255, 214, 102, 0.6);
      color: #fff6cc;
      box-shadow: 0 0 0 3px rgba(255, 214, 102, 0.14);
    }}
    .notification-icon {{
      width: 18px;
      height: 18px;
      fill: none;
      stroke: currentColor;
      stroke-width: 1.8;
      stroke-linecap: round;
      stroke-linejoin: round;
    }}
    .notification-badge {{
      min-width: 18px;
      height: 18px;
      padding: 0 5px;
      border-radius: 999px;
      background: #ffd666;
      color: #5f4300;
      font-size: 11px;
      font-weight: 800;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      position: absolute;
      top: -4px;
      right: -4px;
    }}
    .notification-popover {{
      position: absolute;
      top: calc(100% + 10px);
      right: 0;
      width: 290px;
      padding: 14px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(255,250,242,0.98);
      box-shadow: var(--card-shadow);
      display: grid;
      gap: 8px;
      text-align: left;
      color: var(--ink);
      z-index: 160;
    }}
    .notification-title {{
      font-size: 14px;
      font-weight: 800;
    }}
    .notification-line {{
      font-size: 13px;
      line-height: 1.45;
      color: var(--muted);
    }}
    .notification-task-list {{
      display: grid;
      gap: 8px;
      margin-top: 4px;
    }}
    .notification-task {{
      padding: 10px 12px;
      border-radius: 14px;
      background: rgba(255,255,255,0.78);
      border: 1px solid var(--line);
    }}
    .notification-task-title {{
      font-size: 13px;
      font-weight: 700;
      color: var(--ink);
      line-height: 1.35;
    }}
    .notification-task-kind {{
      margin-top: 4px;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--brand);
      font-weight: 700;
    }}
    .notification-task-meta {{
      margin-top: 4px;
      font-size: 12px;
      color: var(--muted);
      line-height: 1.35;
    }}
    .notification-link {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      margin-top: 4px;
      padding: 10px 12px;
      border-radius: 14px;
      background: linear-gradient(135deg, var(--brand), #18434c);
      color: white;
      font-size: 13px;
      font-weight: 700;
      text-decoration: none;
    }}
    .auth-wrap {{
      min-height: 58vh;
      display: grid;
      place-items: center;
    }}
    .auth-card {{
      width: min(560px, 100%);
      padding: 26px;
      border-radius: 28px;
      background: linear-gradient(180deg, rgba(255,255,255,0.82), rgba(253,248,239,0.98));
      border: 1px solid var(--line);
      box-shadow: var(--card-shadow);
      display: grid;
      gap: 18px;
    }}
    .auth-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
    }}
    .auth-note {{
      font-size: 14px;
      color: var(--muted);
      line-height: 1.6;
    }}
    .cta-row {{
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .ghost-btn {{
      background: rgba(255,255,255,0.1);
      color: white;
      padding: 12px 16px;
      border-radius: 16px;
      font-size: 14px;
      border: 1px solid rgba(255,255,255,0.14);
    }}
    .page {{
      margin-top: 22px;
      display: grid;
      gap: 22px;
    }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 14px;
    }}
    .stats-contracts {{
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }}
    .card {{
      background: var(--paper);
      border: 1px solid rgba(215, 204, 188, 0.9);
      border-radius: 24px;
      box-shadow: var(--card-shadow);
    }}
    .stat-card {{
      padding: 18px 18px 20px;
      position: relative;
      overflow: visible;
    }}
    .stat-card::after {{
      content: "";
      position: absolute;
      inset: auto -30px -30px auto;
      width: 120px;
      height: 120px;
      background: linear-gradient(135deg, rgba(29,92,99,0.12), rgba(217,119,6,0.08));
      border-radius: 50%;
    }}
    .stat-label {{
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      color: var(--muted);
      margin-bottom: 10px;
    }}
    .stat-value {{
      font-size: clamp(1.85rem, 2vw, 2.75rem);
      font-weight: 700;
      line-height: 1.02;
      letter-spacing: -0.03em;
      margin-bottom: 6px;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }}
    .stat-note {{
      color: var(--muted);
      font-size: 14px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: minmax(0, 1.55fr) minmax(320px, 0.9fr);
      gap: 22px;
    }}
    .panel {{
      padding: 22px;
    }}
    .panel-head {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      margin-bottom: 18px;
      flex-wrap: wrap;
    }}
    .panel-title {{
      font-size: 22px;
      font-weight: 700;
      margin: 0;
    }}
    .panel-sub {{
      color: var(--muted);
      font-size: 14px;
    }}
    .contract-list {{
      display: grid;
      gap: 14px;
    }}
    .contract-item {{
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 20px;
      background: linear-gradient(180deg, rgba(255,255,255,0.72), rgba(253,248,239,0.96));
    }}
    .contract-top {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      flex-wrap: wrap;
      margin-bottom: 14px;
    }}
    .contract-name {{
      font-size: 22px;
      font-weight: 700;
      margin-bottom: 6px;
    }}
    .contract-meta {{
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
    }}
    .contract-money {{
      text-align: right;
      min-width: 210px;
    }}
    .money-big {{
      font-size: 22px;
      font-weight: 700;
      margin-bottom: 4px;
    }}
    .progress-wrap {{
      display: grid;
      gap: 8px;
      margin: 14px 0;
    }}
    .progress-track {{
      width: 100%;
      height: 12px;
      border-radius: 999px;
      background: #eadfce;
      overflow: hidden;
    }}
    .progress-bar {{
      height: 100%;
      border-radius: 999px;
      background: linear-gradient(90deg, var(--brand), #44a2a0);
    }}
    .progress-meta {{
      display: flex;
      justify-content: space-between;
      gap: 8px;
      font-size: 14px;
      color: var(--muted);
      flex-wrap: wrap;
    }}
    .contract-table-link {{
      color: inherit;
      text-decoration: none;
      display: block;
    }}
    .contract-table-subtle {{
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
      margin-top: 4px;
    }}
    .payroll-amount {{
      font-weight: 600;
      color: var(--ink);
      white-space: nowrap;
    }}
    .payroll-amount.is-paid {{
      color: var(--ok);
    }}
    .payroll-amount.is-partial {{
      color: var(--warn);
    }}
    .payroll-amount.is-alert {{
      color: var(--danger);
    }}
    .payroll-payment-note {{
      line-height: 1.28;
    }}
    .payroll-payment-note.danger {{
      color: var(--danger);
    }}
    .payroll-col-head {{
      font-size: 12px;
      line-height: 1.25;
      color: var(--muted);
      margin-top: 4px;
      font-weight: 500;
      text-transform: none;
      letter-spacing: normal;
      white-space: normal;
    }}
    .payroll-col-deadline {{
      font-size: 12px;
      line-height: 1.25;
      color: var(--muted);
      margin-top: 2px;
      font-weight: 600;
      text-transform: none;
      letter-spacing: normal;
      white-space: normal;
    }}
    .payroll-col-deadline.danger {{
      color: var(--danger);
    }}
    .payroll-balance {{
      font-weight: 600;
      white-space: nowrap;
      color: var(--warn);
    }}
    .payroll-balance.ok {{
      color: var(--ok);
    }}
    .payroll-balance.danger {{
      color: var(--danger);
    }}
    .payroll-payment-field.is-hidden {{
      display: none;
    }}
    .payroll-table th:nth-child(4),
    .payroll-table td:nth-child(4),
    .payroll-table th:nth-child(6),
    .payroll-table td:nth-child(6),
    .payroll-table th:nth-child(8),
    .payroll-table td:nth-child(8) {{
      border-left: 1px solid var(--line);
    }}
    .payroll-table th:not(:nth-child(2)),
    .payroll-table td:not(:nth-child(2)) {{
      text-align: center;
    }}
    .payroll-table td:not(:nth-child(2)) .status-menu,
    .payroll-table td:not(:nth-child(2)) .payroll-payment-note,
    .payroll-table td:not(:nth-child(2)) .payroll-balance,
    .payroll-table td:not(:nth-child(2)) .chip,
    .payroll-table td:not(:nth-child(2)) .payroll-amount {{
      margin-left: auto;
      margin-right: auto;
      text-align: center;
    }}
    .contract-advance-stack {{
      display: grid;
      gap: 4px;
      font-size: 13px;
      color: var(--ink);
    }}
    .contract-table tbody tr:hover {{
      background: rgba(255,255,255,0.42);
    }}
    .stage-builder-card {{
      display: grid;
      gap: 12px;
    }}
    .info-row {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 14px;
    }}
    .chip {{
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 13px;
      background: #f0e6d7;
      color: #4f5a62;
      white-space: nowrap;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border: 1px solid transparent;
    }}
    .chip.warn {{ background: #fff2d8; color: #8b5e00; }}
    .chip.accent {{ background: #ffe5cf; color: #b95a00; }}
    .chip.danger {{ background: #f9dede; color: #922b2b; }}
    .chip.ok {{ background: #e2f4e8; color: #1c6a38; }}
    .status-chip {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 13px;
      border-radius: 999px;
      padding: 7px 10px;
      background: #f1e8da;
    }}
    .timeline {{
      display: grid;
      gap: 12px;
    }}
    .timeline-item {{
      display: grid;
      grid-template-columns: 88px minmax(0, 1fr);
      gap: 12px;
      align-items: start;
      padding-bottom: 12px;
      border-bottom: 1px dashed var(--line);
    }}
    .timeline-item:last-child {{ border-bottom: 0; padding-bottom: 0; }}
    .timeline-date {{
      font-size: 13px;
      color: var(--muted);
      font-weight: 700;
    }}
    .timeline-title {{
      font-size: 16px;
      font-weight: 700;
      margin-bottom: 4px;
    }}
    .table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    .auction-table {{
      table-layout: fixed;
      width: 100%;
    }}
    .table th, .table td {{
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: middle;
      min-width: 0;
    }}
    .nowrap {{
      white-space: nowrap;
    }}
    .table th {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.1em;
    }}
    .empty {{
      padding: 24px;
      border: 1px dashed var(--line);
      border-radius: 20px;
      color: var(--muted);
      background: rgba(255,255,255,0.4);
    }}
    .detail-hero {{
      display: grid;
      grid-template-columns: minmax(0, 1.3fr) minmax(280px, 0.8fr);
      gap: 18px;
      align-items: stretch;
    }}
    .detail-box {{
      padding: 20px;
      border-radius: 22px;
      background: linear-gradient(135deg, rgba(29,92,99,0.96), rgba(34,87,122,0.92));
      color: white;
      min-height: 220px;
    }}
    .detail-side {{
      display: grid;
      gap: 12px;
    }}
    .mini-card {{
      padding: 18px;
      border-radius: 20px;
      background: var(--paper);
      border: 1px solid var(--line);
      box-shadow: var(--card-shadow);
    }}
    .mini-value {{
      font-size: 24px;
      font-weight: 700;
      margin-top: 10px;
    }}
    .section-stack {{
      display: grid;
      gap: 22px;
    }}
    .form-grid {{
      display: grid;
      gap: 12px;
    }}
    .payable-create-grid {{
      grid-template-columns: repeat(4, minmax(0, 1fr));
      align-items: start;
    }}
    .field.span-2 {{
      grid-column: span 2;
    }}
    .field.span-4 {{
      grid-column: 1 / -1;
    }}
    .payable-create-submit {{
      grid-column: 1 / -1;
    }}
    .field {{
      display: grid;
      gap: 6px;
    }}
    .field label {{
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      color: var(--muted);
    }}
    .field input, .field textarea {{
      width: 100%;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.75);
      padding: 12px 14px;
      font: inherit;
      color: var(--ink);
    }}
    .field textarea {{
      min-height: 92px;
      resize: vertical;
    }}
    @media (max-width: 720px) {{
      .payable-create-grid {{
        grid-template-columns: 1fr;
      }}
      .field.span-2,
      .field.span-4,
      .payable-create-submit {{
        grid-column: auto;
      }}
    }}
    .copy-field {{
      display: flex;
      gap: 10px;
      align-items: stretch;
    }}
    .copy-field input {{
      flex: 1;
    }}
    .copy-btn {{
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255,255,255,0.82);
      color: var(--ink);
      min-width: 46px;
      padding: 0 12px;
      font-size: 18px;
      cursor: pointer;
    }}
    .copy-btn:hover {{
      background: #efe5d8;
    }}
    .submit-btn {{
      border: 0;
      border-radius: 16px;
      background: linear-gradient(135deg, var(--brand), #18434c);
      color: white;
      padding: 12px 16px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }}
    .autocomplete-wrap {{
      position: relative;
    }}
    .autocomplete-list {{
      position: absolute;
      top: calc(100% + 6px);
      left: 0;
      right: 0;
      z-index: 35;
      display: none;
      max-height: 220px;
      overflow: auto;
      padding: 6px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(255,250,242,0.98);
      box-shadow: var(--card-shadow);
      gap: 4px;
    }}
    .autocomplete-list.is-open {{
      display: grid;
    }}
    .autocomplete-option {{
      width: 100%;
      border: none;
      background: rgba(255,255,255,0.78);
      border-radius: 10px;
      padding: 10px 12px;
      text-align: left;
      font: inherit;
      color: var(--ink);
      cursor: pointer;
    }}
    .autocomplete-option[hidden] {{
      display: none;
    }}
    .flash {{
      padding: 14px 16px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: #fff7df;
      color: #684b00;
    }}
    .flash.ok {{
      background: #eaf7ee;
      color: #1d5b34;
    }}
    .access-stack {{
      display: grid;
      gap: 18px;
    }}
    .user-card {{
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 22px;
      background: linear-gradient(180deg, rgba(255,255,255,0.78), rgba(253,248,239,0.98));
      display: grid;
      gap: 16px;
    }}
    .user-head {{
      display: flex;
      justify-content: space-between;
      gap: 14px;
      flex-wrap: wrap;
      align-items: flex-start;
    }}
    .user-name {{
      font-size: 22px;
      font-weight: 700;
      margin-bottom: 6px;
    }}
    .user-name-menu {{
      display: inline-block;
      margin-bottom: 6px;
    }}
    .user-name-menu summary {{
      list-style: none;
    }}
    .user-name-menu summary::-webkit-details-marker {{
      display: none;
    }}
    .user-name-trigger {{
      border: 0;
      padding: 0;
      background: transparent;
      color: var(--ink);
      font: inherit;
      font-size: 22px;
      font-weight: 700;
      cursor: pointer;
      text-align: left;
    }}
    .user-name-trigger:hover {{
      color: var(--brand);
    }}
    .name-popover {{
      margin-top: 10px;
      min-width: 280px;
      max-width: 360px;
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: #fffaf2;
      box-shadow: var(--card-shadow);
      display: grid;
      gap: 10px;
    }}
    .user-meta {{
      color: var(--muted);
      font-size: 14px;
      line-height: 1.6;
    }}
    .badge-row {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }}
    .badge {{
      border-radius: 999px;
      padding: 7px 11px;
      font-size: 13px;
      background: #edf3f3;
      color: #27565b;
    }}
    .badge.warn {{ background: #fff2d8; color: #8b5e00; }}
    .badge.danger {{ background: #f9dede; color: #922b2b; }}
    .permissions-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 10px;
    }}
    .permission-box {{
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 12px;
      background: rgba(255,255,255,0.72);
      display: grid;
      gap: 10px;
    }}
    .settings-menu {{
      display: inline-block;
    }}
    .settings-trigger {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255,255,255,0.82);
      color: var(--ink);
      padding: 10px 14px;
      font: inherit;
      font-weight: 600;
      cursor: pointer;
    }}
    .settings-popover {{
      margin-top: 10px;
      min-width: 320px;
      max-width: 720px;
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: #fffaf2;
      box-shadow: var(--card-shadow);
      display: grid;
      gap: 12px;
    }}
    .settings-divider {{
      height: 1px;
      background: var(--line);
      opacity: 0.8;
    }}
    .permission-box strong {{
      font-size: 14px;
    }}
    .check-row {{
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      font-size: 14px;
      color: var(--muted);
      align-items: center;
    }}
    .check-row label {{
      display: inline-flex;
      gap: 8px;
      align-items: center;
      cursor: pointer;
    }}
    .toggle-checkbox {{
      width: 18px;
      height: 18px;
      accent-color: var(--brand);
      flex: 0 0 18px;
    }}
    .advance-toggle {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      font-size: 14px;
      color: var(--ink);
      margin: 2px 0 4px;
      cursor: pointer;
    }}
    .estimate-skip-label {{
      font-size: 14px;
      line-height: 1.3;
      color: var(--ink);
      text-transform: none;
      letter-spacing: normal;
      font-weight: 500;
    }}
    .estimate-skip-row {{
      display: inline-flex;
      align-items: center;
      gap: 14px;
      margin: 2px 0 4px;
    }}
    .estimate-skip-box {{
      display: inline-flex;
      align-items: center;
      cursor: pointer;
      margin: 0;
      flex: 0 0 18px;
    }}
    .estimate-skip-box .toggle-checkbox {{
      margin: 0;
    }}
    .advance-field.is-hidden {{
      display: none;
    }}
    .signed-date-field.is-hidden {{
      display: none;
    }}
    .action-row {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }}
    .secondary-btn {{
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255,255,255,0.82);
      color: var(--ink);
      padding: 10px 14px;
      font: inherit;
      font-weight: 600;
      cursor: pointer;
    }}
    .secondary-btn.danger {{
      color: #fff;
      border-color: rgba(184,50,50,0.35);
      background: var(--danger);
      box-shadow: 0 14px 32px rgba(184,50,50,0.18);
    }}
    .secondary-btn.danger:hover {{
      color: #fff;
      background: #a62e2e;
    }}
    .auction-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 6px;
      min-width: 0;
    }}
    .auction-number {{
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 0;
      margin-left: auto;
      text-align: right;
      min-width: 0;
      overflow-wrap: anywhere;
    }}
    .auction-number a {{
      color: inherit;
      text-decoration: none;
      border-bottom: 1px dashed rgba(111,122,130,0.5);
    }}
    .auction-lot-meta {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-top: 8px;
      min-width: 0;
    }}
    .auction-row-actions {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      margin-left: auto;
      min-width: 0;
    }}
    .icon-btn {{
      padding: 0;
      border: none;
      background: transparent;
      color: rgba(31, 38, 41, 0.62);
      display: inline-flex;
      align-items: center;
      justify-content: center;
      cursor: pointer;
      line-height: 1;
    }}
    .icon-btn.danger {{
      color: rgba(31, 38, 41, 0.62);
    }}
    .icon-btn:hover {{
      transform: translateY(-1px);
      color: rgba(31, 38, 41, 0.92);
    }}
    .icon-trash {{
      width: 20px;
      height: 20px;
      display: block;
      fill: none;
      stroke: currentColor;
      stroke-width: 1.7;
      stroke-linecap: round;
      stroke-linejoin: round;
    }}
    .icon-restore {{
      width: 20px;
      height: 20px;
      display: block;
      fill: none;
      stroke: currentColor;
      stroke-width: 1.8;
      stroke-linecap: round;
      stroke-linejoin: round;
    }}
    .payables-action-col {{
      width: 1%;
      white-space: nowrap;
      text-align: center;
      padding-left: 8px;
      padding-right: 8px;
    }}
    .auction-added-date {{
      font-size: 12px;
      color: var(--muted);
      white-space: nowrap;
      text-align: right;
    }}
    .auction-added-date.is-new {{
      color: var(--ok);
      font-weight: 700;
    }}
    .auction-added-tooltip {{
      position: relative;
      cursor: default;
    }}
    .auction-added-tooltip[data-tooltip]:hover::after {{
      content: attr(data-tooltip);
      position: absolute;
      right: 0;
      bottom: calc(100% + 8px);
      min-width: 150px;
      max-width: 220px;
      padding: 8px 10px;
      border-radius: 12px;
      background: rgba(22, 35, 47, 0.96);
      color: #fff;
      font-size: 12px;
      font-weight: 400;
      line-height: 1.35;
      white-space: normal;
      text-align: center;
      box-shadow: 0 12px 28px rgba(22, 35, 47, 0.2);
      z-index: 40;
      pointer-events: none;
    }}
    .auction-seq {{
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
      white-space: nowrap;
    }}
    .status-form {{
      margin: 0;
    }}
    .status-menu {{
      position: relative;
      display: inline-block;
    }}
    .status-menu summary {{
      list-style: none;
      cursor: pointer;
    }}
    .status-menu summary::-webkit-details-marker {{
      display: none;
    }}
    .status-menu[open] summary .chip {{
      box-shadow: 0 0 0 2px rgba(29,92,99,0.14);
    }}
    .status-popover {{
      position: absolute;
      top: calc(100% + 8px);
      left: 0;
      z-index: 20;
      min-width: 170px;
      padding: 10px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: rgba(255,250,242,0.98);
      box-shadow: var(--card-shadow);
      display: grid;
      gap: 8px;
    }}
    .status-option {{
      width: 100%;
      text-align: center;
      cursor: pointer;
      appearance: none;
      border: none;
      font: inherit;
      padding: 12px 16px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }}
    .discount-menu summary {{
      display: inline-block;
    }}
    .lot-menu {{
      display: block;
    }}
    .lot-menu summary {{
      display: block;
    }}
    .lot-form {{
      min-width: 320px;
    }}
    .result-form {{
      min-width: 320px;
    }}
    .estimate-form {{
      min-width: 320px;
    }}
    .result-price-field.is-hidden {{
      display: none;
    }}
    .estimate-cost-field.is-hidden {{
      display: none;
    }}
    .result-options {{
      display: grid;
      gap: 8px;
    }}
    .result-picker,
    .estimate-picker {{
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 8px 10px;
      border-radius: 14px;
      border: 1px solid var(--line);
      cursor: pointer;
      text-align: center;
      font: inherit;
      width: 100%;
      background: rgba(255,255,255,0.78);
    }}
    .result-picker.is-active,
    .estimate-picker.is-active,
    .status-option.is-active {{
      box-shadow: 0 0 0 3px rgba(29,92,99,0.32);
    }}
    .submit-btn[disabled] {{
      opacity: 0.45;
      cursor: not-allowed;
    }}
    .result-helper {{
      font-size: 12px;
      color: var(--muted);
      line-height: 1.4;
    }}
    .result-error {{
      display: none;
      font-size: 12px;
      color: var(--danger);
      font-weight: 700;
      line-height: 1.35;
    }}
    .result-error.is-visible {{
      display: block;
    }}
    .discount-value {{
      font-size: 14px;
      color: var(--ink);
      display: inline-grid;
      gap: 2px;
      text-align: center;
      justify-items: center;
      line-height: 1.25;
    }}
    .discount-percent {{
      font-weight: 700;
    }}
    .discount-amount {{
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }}
    .discount-placeholder {{
      color: var(--muted);
    }}
    .discount-alert {{
      display: inline-block;
      color: var(--danger);
      font-weight: 700;
      white-space: normal;
      line-height: 1.15;
      max-width: 92px;
      text-align: center;
    }}
    .result-meta {{
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
    }}
    .result-meta-stack {{
      display: inline-grid;
      gap: 2px;
      justify-items: center;
      white-space: normal;
      text-align: center;
      line-height: 1.2;
    }}
    .status-chip-tooltip {{
      position: relative;
      cursor: pointer;
    }}
    .status-chip-tooltip[data-tooltip]:hover::after {{
      content: attr(data-tooltip);
      position: absolute;
      left: 50%;
      bottom: calc(100% + 8px);
      transform: translateX(-50%);
      min-width: 170px;
      max-width: 240px;
      padding: 8px 10px;
      border-radius: 12px;
      background: rgba(22, 35, 47, 0.96);
      color: #fff;
      font-size: 12px;
      font-weight: 400;
      line-height: 1.35;
      white-space: normal;
      text-align: center;
      box-shadow: 0 12px 28px rgba(22, 35, 47, 0.2);
      z-index: 40;
      pointer-events: none;
    }}
    .discount-form {{
      min-width: 280px;
    }}
    .tab-row {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    .tab-btn {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 9px 14px;
      border-radius: 999px;
      border: 1px solid var(--line);
      color: var(--muted);
      text-decoration: none;
      background: rgba(255,255,255,0.75);
      font-weight: 600;
    }}
    .tab-btn.active {{
      color: var(--ink);
      background: rgba(232,246,238,0.95);
      border-color: rgba(29,92,99,0.18);
      box-shadow: 0 10px 24px rgba(29,92,99,0.10);
    }}
    .tab-count {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 24px;
      height: 24px;
      padding: 0 8px;
      border-radius: 999px;
      background: rgba(29,92,99,0.08);
      color: var(--ink);
      font-size: 12px;
      font-weight: 700;
    }}
    .deadline-meta {{
      margin-top: 4px;
      font-size: 12px;
      color: var(--muted);
      white-space: nowrap;
    }}
    .deadline-meta.danger {{
      color: var(--danger);
      font-weight: 700;
    }}
    .auction-table td:nth-child(2),
    .auction-table td:nth-child(3),
    .auction-table td:nth-child(4),
    .auction-table td:nth-child(5),
    .auction-table td:nth-child(6),
    .auction-table td:nth-child(7),
    .auction-table td:nth-child(8) {{
      text-align: center;
    }}
    .auction-table th:nth-child(2),
    .auction-table th:nth-child(3),
    .auction-table th:nth-child(4),
    .auction-table th:nth-child(5),
    .auction-table th:nth-child(6),
    .auction-table th:nth-child(7),
    .auction-table th:nth-child(8) {{
      text-align: center;
    }}
    .auction-table td:first-child,
    .auction-table th:first-child {{
      width: 28%;
    }}
    .auction-table td:nth-child(2),
    .auction-table th:nth-child(2) {{
      width: 10%;
    }}
    .auction-table td:nth-child(3),
    .auction-table th:nth-child(3) {{
      width: 10%;
    }}
    .auction-table td:nth-child(4),
    .auction-table th:nth-child(4) {{
      width: 13%;
    }}
    .auction-table td:nth-child(5),
    .auction-table th:nth-child(5) {{
      width: 13%;
    }}
    .auction-table td:nth-child(6),
    .auction-table th:nth-child(6) {{
      width: 13%;
    }}
    .auction-table td:nth-child(7),
    .auction-table th:nth-child(7) {{
      width: 13%;
    }}
    .auction-table td:nth-child(6) .status-popover,
    .auction-table td:nth-child(7) .status-popover {{
      left: auto;
      right: 0;
    }}
    .auction-cell-center {{
      width: 100%;
      display: grid;
      justify-items: center;
      align-content: center;
      gap: 2px;
      text-align: center;
    }}
    .auction-cell-center > .status-menu {{
      display: block;
      width: fit-content;
      margin: 0 auto;
    }}
    .auction-cell-center .chip {{
      max-width: 100%;
    }}
    .auction-cell-center .discount-value {{
      display: grid;
      width: 100%;
      justify-items: center;
    }}
    tr[data-auction-row].is-saving {{
      opacity: 0.55;
      transition: opacity 0.18s ease;
    }}
    @media (max-width: 960px) {{
      .shell {{
        width: min(100vw - 20px, 1440px);
        grid-template-columns: 1fr;
      }}
      .sidebar {{
        min-height: 0;
        position: static;
      }}
      .grid, .detail-hero {{
        grid-template-columns: 1fr;
      }}
      .contract-money {{
        text-align: left;
        min-width: 0;
      }}
    }}
    @media (max-width: 1320px) {{
      .stats-contracts {{
        grid-template-columns: repeat(3, minmax(0, 1fr));
      }}
    }}
    @media (max-width: 960px) {{
      .stats-contracts {{
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <aside class="sidebar">
      <div>
        <div class="brand-mark">ООО СК "ФЕЛИС ГРУПП"</div>
        <div class="brand-title">СИСТЕМА\nУПРАВЛЕНИЯ\nБИЗНЕСОМ</div>
        <div class="brand-sub">
          Начинаем с раздела контрактов и постепенно собираем полноценную строительную CRM.
        </div>
      </div>
      <nav class="nav">{nav_links}</nav>
      {sidebar_notes}
    </aside>
    <div class="content">
      <header class="hero">
        <div class="hero-top">
        <div>
          <div class="eyebrow">CRM Draft</div>
          <h1>{escape(hero_title)}</h1>
          <div class="hero-copy">{escape(hero_copy)}</div>
        </div>
        <div class="owner-switch">
          {user_panel}
        </div>
      </div>
      </header>
      <main class="page">{body}</main>
    </div>
  </div>
</body>
<script>
function copyText(inputId) {{
  const input = document.getElementById(inputId);
  if (!input) {{
    return;
  }}
  const value = input.value;
  if (navigator.clipboard && window.isSecureContext) {{
    navigator.clipboard.writeText(value).catch(() => {{
      input.focus();
      input.select();
      document.execCommand("copy");
    }});
    return;
  }}
  input.focus();
  input.select();
  document.execCommand("copy");
}}

function buildContractStageFields(form, count) {{
  const container = form ? form.querySelector('[data-stage-container]') : null;
  if (!container) {{
    return;
  }}
  const normalized = Math.max(1, Number(count || 1));
  let html = '';
  for (let index = 1; index <= normalized; index += 1) {{
    html += `
      <div class="permission-box stage-builder-card">
        <strong>Этап ${{index}}</strong>
        <div class="stats" style="grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));">
          <div class="field">
            <label>Сумма этапа</label>
            <input type="text" name="stage_amount_${{index}}" data-money-input="1" required>
          </div>
          <div class="field">
            <label>Старт работ по этапу</label>
            <input type="date" name="stage_start_date_${{index}}">
          </div>
          <div class="field">
            <label>Дедлайн этапа</label>
            <input type="date" name="stage_end_date_${{index}}" required>
          </div>
        </div>
        <div class="field">
          <label>Примечание к этапу</label>
          <textarea name="stage_notes_${{index}}" placeholder="Что входит в этап"></textarea>
        </div>
      </div>
    `;
  }}
  container.innerHTML = html;
}}

document.addEventListener("click", (event) => {{
  const estimateQuick = event.target.closest('.estimate-form .estimate-quick');
  if (estimateQuick) {{
    const form = estimateQuick.closest('.estimate-form');
    const hiddenEstimate = form ? form.querySelector('input[name="estimate_status"]') : null;
    const costInputs = form ? form.querySelectorAll('input[name="material_cost"], input[name="work_cost"], input[name="other_cost"]') : [];
    if (hiddenEstimate) {{
      hiddenEstimate.value = estimateQuick.dataset.estimateValue || "";
    }}
    costInputs.forEach((input) => input.value = "");
    window.sessionStorage.setItem("auctionScrollY", String(window.scrollY));
    if (form) {{
      form.submit();
    }}
    return;
  }}
  const resultQuick = event.target.closest('.result-form .result-quick');
  if (resultQuick) {{
    const form = resultQuick.closest('.result-form');
    const hiddenResult = form ? form.querySelector('input[name="result_status"]') : null;
    const priceInput = form ? form.querySelector('input[name="final_bid_amount"]') : null;
    if (hiddenResult) {{
      hiddenResult.value = resultQuick.dataset.resultValue || "";
    }}
    if (priceInput) {{
      priceInput.value = "";
    }}
    window.sessionStorage.setItem("auctionScrollY", String(window.scrollY));
    if (form) {{
      form.submit();
    }}
    return;
  }}
  const optionButton = event.target.closest(".status-option");
  if (optionButton) {{
    window.sessionStorage.setItem("auctionScrollY", String(window.scrollY));
    return;
  }}
  const activeElement = document.activeElement;
  if (activeElement && activeElement.closest(".status-popover")) {{
    return;
  }}
  const selection = window.getSelection ? window.getSelection() : null;
  if (selection && String(selection).trim()) {{
    return;
  }}
  document.querySelectorAll(".status-menu[open]").forEach((menu) => {{
    if (!menu.contains(event.target)) {{
      menu.removeAttribute("open");
    }}
  }});
}});

document.addEventListener("click", (event) => {{
  const estimateButton = event.target.closest('.estimate-form .estimate-picker');
  if (!estimateButton) {{
    return;
  }}
  const form = estimateButton.closest('.estimate-form');
  if (!form) {{
    return;
  }}
  const hiddenEstimate = form.querySelector('input[name="estimate_status"]');
  const costFields = form.querySelectorAll('.estimate-cost-field');
  const costInputs = form.querySelectorAll('input[name="material_cost"], input[name="work_cost"], input[name="other_cost"]');
  const submitButton = form.querySelector('button[type="submit"]');
  const errorBox = form.querySelector('.result-error');
  const nextValue = estimateButton.dataset.estimateValue || "";
  if (hiddenEstimate) {{
    hiddenEstimate.value = nextValue;
  }}
  form.querySelectorAll('.estimate-picker').forEach((button) => {{
    button.classList.toggle('is-active', button === estimateButton);
  }});
  const needsCost = nextValue === "calculated";
  if (!needsCost) {{
    costInputs.forEach((input) => input.value = "");
    form.querySelectorAll('input[name^="skip_"]').forEach((checkbox) => {{
      checkbox.checked = false;
    }});
  }}
  updateEstimateFormState(form);
  if (needsCost) {{
    const firstVisibleInput = form.querySelector('.estimate-cost-field:not(.is-hidden) .estimate-cost-input:not(.is-hidden) input[type="text"]');
    if (firstVisibleInput) {{
      window.setTimeout(() => firstVisibleInput.focus(), 0);
    }}
  }}
}});

document.addEventListener("submit", (event) => {{
  const previewForm = event.target.closest(".preview-form");
  if (previewForm) {{
    const nextInput = previewForm.querySelector('input[name="next_path"]');
    if (nextInput) {{
      nextInput.value = `${{window.location.pathname}}${{window.location.search}}`;
    }}
  }}
  if (event.target.closest(".discount-form, .amount-form, .deadline-form, .lot-form, .result-form, .estimate-form, .auction-delete-form, .payroll-amount-form, .payroll-payment-form")) {{
    window.sessionStorage.setItem("auctionScrollY", String(window.scrollY));
  }}
  if (event.target.closest(".payables-filter-form")) {{
    window.sessionStorage.setItem("auctionScrollY", String(window.scrollY));
  }}
}});

document.addEventListener("click", (event) => {{
  const payablesLink = event.target.closest('a[data-keep-payables-scroll="1"]');
  if (!payablesLink) {{
    return;
  }}
  window.sessionStorage.setItem("auctionScrollY", String(window.scrollY));
}});

document.addEventListener("change", (event) => {{
  const counterpartySelect = event.target.closest('.payable-counterparty-select');
  if (!counterpartySelect) {{
    return;
  }}
  const targetId = counterpartySelect.dataset.targetInput || "";
  const targetInput = targetId ? document.getElementById(targetId) : null;
  if (targetInput && counterpartySelect.value) {{
    targetInput.value = counterpartySelect.value;
  }}
}});

document.addEventListener("focusin", (event) => {{
  const input = event.target.closest('.counterparty-autocomplete-input');
  if (!input) {{
    return;
  }}
  const wrap = input.closest('.autocomplete-wrap');
  const list = wrap ? wrap.querySelector('.autocomplete-list') : null;
  if (!list) {{
    return;
  }}
  const query = input.value.trim().toLowerCase();
  let visible = 0;
  list.querySelectorAll('.autocomplete-option').forEach((option) => {{
    const matches = !query || option.dataset.valueLower.includes(query);
    option.hidden = !matches;
    if (matches) {{
      visible += 1;
    }}
  }});
  list.classList.toggle('is-open', visible > 0);
}});

document.addEventListener("input", (event) => {{
  const input = event.target.closest('.counterparty-autocomplete-input');
  if (!input) {{
    return;
  }}
  const wrap = input.closest('.autocomplete-wrap');
  const list = wrap ? wrap.querySelector('.autocomplete-list') : null;
  if (!list) {{
    return;
  }}
  const query = input.value.trim().toLowerCase();
  let visible = 0;
  list.querySelectorAll('.autocomplete-option').forEach((option) => {{
    const matches = !query || option.dataset.valueLower.includes(query);
    option.hidden = !matches;
    if (matches) {{
      visible += 1;
    }}
  }});
  list.classList.toggle('is-open', visible > 0);
}});

document.addEventListener("input", (event) => {{
  const dueDaysInput = event.target.closest('form[action^="/payables/new"] input[name="due_days"]');
  if (!dueDaysInput) {{
    return;
  }}
  updatePayableDueFields(dueDaysInput.closest('form'), 'days');
}});

document.addEventListener("change", (event) => {{
  const dueDateInput = event.target.closest('form[action^="/payables/new"] input[name="due_date"]');
  if (dueDateInput) {{
    updatePayableDueFields(dueDateInput.closest('form'), 'date');
    return;
  }}
  const documentDateInput = event.target.closest('form[action^="/payables/new"] input[name="document_date"]');
  if (documentDateInput) {{
    updatePayableDueFields(documentDateInput.closest('form'), 'document');
  }}
}});

document.addEventListener("click", (event) => {{
  const option = event.target.closest('.autocomplete-option');
  if (option) {{
    const wrap = option.closest('.autocomplete-wrap');
    const input = wrap ? wrap.querySelector('.counterparty-autocomplete-input') : null;
    const list = wrap ? wrap.querySelector('.autocomplete-list') : null;
    if (input) {{
      input.value = option.dataset.value || "";
    }}
    if (list) {{
      list.classList.remove('is-open');
    }}
    return;
  }}
  if (!event.target.closest('.autocomplete-wrap')) {{
    document.querySelectorAll('.autocomplete-list.is-open').forEach((list) => {{
      list.classList.remove('is-open');
    }});
  }}
}});

function updatePayableDueFields(form, source) {{
  if (!form) {{
    return;
  }}
  const documentDateInput = form.querySelector('input[name="document_date"]');
  const dueDateInput = form.querySelector('input[name="due_date"]');
  const dueDaysInput = form.querySelector('input[name="due_days"]');
  if (!dueDateInput || !dueDaysInput) {{
    return;
  }}
  const today = new Date();
  const todayIso = new Date(today.getFullYear(), today.getMonth(), today.getDate()).toISOString().slice(0, 10);
  const baseValue = (documentDateInput && documentDateInput.value) ? documentDateInput.value : todayIso;
  const baseDate = new Date(`${{baseValue}}T00:00:00`);
  if (Number.isNaN(baseDate.getTime())) {{
    return;
  }}
  if (source === 'days') {{
    const rawDays = dueDaysInput.value.trim();
    if (!rawDays) {{
      dueDateInput.value = "";
      return;
    }}
    const days = Number(rawDays);
    if (!Number.isFinite(days) || days <= 0) {{
      return;
    }}
    const dueDate = new Date(baseDate);
    dueDate.setDate(dueDate.getDate() + Math.round(days));
    dueDateInput.value = dueDate.toISOString().slice(0, 10);
    return;
  }}
  if (source === 'date') {{
    if (!dueDateInput.value) {{
      dueDaysInput.value = "";
      return;
    }}
    const dueDate = new Date(`${{dueDateInput.value}}T00:00:00`);
    if (Number.isNaN(dueDate.getTime())) {{
      return;
    }}
    const diffMs = dueDate.getTime() - baseDate.getTime();
    const diffDays = Math.round(diffMs / 86400000);
    dueDaysInput.value = diffDays > 0 ? String(diffDays) : "";
    return;
  }}
  if (source === 'document' && dueDateInput.value) {{
    updatePayableDueFields(form, 'date');
  }}
}}

document.addEventListener("input", (event) => {{
  const form = event.target.closest(".discount-form");
  if (!form) {{
    return;
  }}
  const baseAmount = Number(form.dataset.baseAmount || "0");
  const percentInput = form.querySelector('input[name="discount_percent"]');
  const amountInput = form.querySelector('input[name="min_amount"]');
  if (!percentInput || !amountInput) {{
    return;
  }}
  const active = event.target;
  const normalize = (value) => Number(String(value).replace(/\\s+/g, "").replace(",", "."));
  const formatPercent = (value) => String(Math.round(value * 100) / 100).replace(".", ",");
  const formatMoney = (value) => {{
    const fixed = (Math.round(value * 100) / 100).toFixed(2);
    const [whole, frac] = fixed.split(".");
    return whole.replace(/\\B(?=(\\d{{3}})+(?!\\d))/g, " ") + "," + frac;
  }};
  if (active === percentInput) {{
    const percent = normalize(percentInput.value);
    if (!Number.isFinite(percent) || !Number.isFinite(baseAmount)) {{
      return;
    }}
    const minAmount = baseAmount * (1 - percent / 100);
    amountInput.value = formatMoney(minAmount);
  }} else if (active === amountInput) {{
    const rawAmount = amountInput.value.replace(/[^\\d,\\.\\s]/g, "");
    if (rawAmount !== amountInput.value) {{
      amountInput.value = rawAmount;
    }}
    const minAmount = normalize(rawAmount);
    if (!Number.isFinite(minAmount) || !Number.isFinite(baseAmount) || baseAmount === 0) {{
      return;
    }}
    const percent = ((baseAmount - minAmount) / baseAmount) * 100;
    percentInput.value = formatPercent(percent);
  }}
}});

document.addEventListener("change", (event) => {{
  const checkbox = event.target.closest('input[name="has_advance"]');
  if (!checkbox) {{
    return;
  }}
  const form = checkbox.closest("form");
  const field = form ? form.querySelector(".advance-field") : null;
  const percentInput = form ? form.querySelector('input[name="advance_percent"]') : null;
  if (field) {{
    field.classList.toggle("is-hidden", !checkbox.checked);
  }}
  if (percentInput && !checkbox.checked) {{
    percentInput.value = "";
  }}
}});

document.addEventListener("change", (event) => {{
  const checkbox = event.target.closest('input[name="is_unsigned"]');
  if (!checkbox) {{
    return;
  }}
  const form = checkbox.closest("form");
  const field = form ? form.querySelector(".signed-date-field") : null;
  const dateInput = form ? form.querySelector('input[name="signed_date"]') : null;
  if (field) {{
    field.classList.toggle("is-hidden", checkbox.checked);
  }}
  if (dateInput) {{
    dateInput.required = !checkbox.checked;
    if (checkbox.checked) {{
      dateInput.value = "";
    }}
  }}
}});

document.addEventListener("change", (event) => {{
  const checkbox = event.target.closest('input[name="is_paid"]');
  if (!checkbox) {{
    return;
  }}
  const form = checkbox.closest("form");
  updatePaymentFormState(form);
}});

document.addEventListener("input", (event) => {{
  const stageCountInput = event.target.closest('[data-stage-count-input]');
  if (!stageCountInput) {{
    return;
  }}
  const form = stageCountInput.closest('.contract-create-form');
  if (!form) {{
    return;
  }}
  buildContractStageFields(form, stageCountInput.value);
}});

document.addEventListener("blur", (event) => {{
  const amountInput = event.target.closest('input[data-money-input], .discount-form input[name="min_amount"]');
  if (!amountInput) {{
    return;
  }}
  const raw = amountInput.value.replace(/[^\\d,\\.\\s]/g, "");
  if (!raw.trim()) {{
    return;
  }}
  const normalized = Number(String(raw).replace(/\\s+/g, "").replace(",", "."));
  if (!Number.isFinite(normalized)) {{
    return;
  }}
  const fixed = (Math.round(normalized * 100) / 100).toFixed(2);
  const [whole, frac] = fixed.split(".");
  amountInput.value = whole.replace(/\\B(?=(\\d{{3}})+(?!\\d))/g, " ") + "," + frac;
}}, true);

document.addEventListener("click", (event) => {{
  const resultButton = event.target.closest('.result-form .result-picker');
  if (!resultButton) {{
    return;
  }}
  const form = resultButton.closest('.result-form');
  if (!form) {{
    return;
  }}
  const hiddenResult = form.querySelector('input[name="result_status"]');
  const priceField = form.querySelector('.result-price-field');
  const priceInput = form.querySelector('input[name="final_bid_amount"]');
  const submitButton = form.querySelector('button[type="submit"]');
  const errorBox = form.querySelector('.result-error');
  const nextValue = resultButton.dataset.resultValue || "";
  if (hiddenResult) {{
    hiddenResult.value = nextValue;
  }}
  form.querySelectorAll('.result-picker').forEach((button) => {{
    button.classList.toggle('is-active', button === resultButton);
  }});
  const needsPrice = nextValue === "won" || nextValue === "lost";
  if (priceField) {{
    priceField.classList.toggle('is-hidden', !needsPrice);
  }}
  if (priceInput) {{
    priceInput.required = Boolean(needsPrice);
    if (!needsPrice) {{
      priceInput.value = "";
    }} else {{
      priceInput.value = "";
      window.setTimeout(() => priceInput.focus(), 0);
    }}
  }}
  if (errorBox) {{
    errorBox.classList.toggle('is-visible', Boolean(needsPrice));
  }}
  if (submitButton) {{
    submitButton.disabled = Boolean(needsPrice);
  }}
}});

document.addEventListener("input", (event) => {{
  const estimateInput = event.target.closest('.estimate-form input[name="material_cost"], .estimate-form input[name="work_cost"], .estimate-form input[name="other_cost"]');
  if (estimateInput) {{
    const form = estimateInput.closest('.estimate-form');
    if (form) {{
      updateEstimateFormState(form);
    }}
    return;
  }}
  const resultPrice = event.target.closest('.result-form input[name="final_bid_amount"]');
  if (!resultPrice) {{
    return;
  }}
  const form = resultPrice.closest('.result-form');
  const submitButton = form ? form.querySelector('button[type="submit"]') : null;
  const selected = form ? form.querySelector('.result-form input[name="result_status"]') : null;
  const errorBox = form ? form.querySelector('.result-error') : null;
  const needsPrice = selected && (selected.value === "won" || selected.value === "lost");
  const cleaned = resultPrice.value.replace(/\\s+/g, "").replace(",", ".").trim();
  const hasValue = cleaned !== "" && Number(cleaned) > 0;
  if (submitButton) {{
    submitButton.disabled = Boolean(needsPrice && !hasValue);
  }}
  if (errorBox) {{
    errorBox.classList.toggle('is-visible', Boolean(needsPrice && !hasValue));
  }}
}});

function updateEstimateFormState(form) {{
  if (!form) {{
    return;
  }}
  const selected = form.querySelector('input[name="estimate_status"]');
  const needsCost = selected && selected.value === "calculated";
  const submitButton = form.querySelector('button[type="submit"]');
  const errorBox = form.querySelector('.result-error');
  let hasAtLeastOneCost = false;
  let allVisibleCostsValid = true;
  form.querySelectorAll('.estimate-cost-field[data-estimate-field]').forEach((field) => {{
    field.classList.toggle('is-hidden', !needsCost);
    const checkbox = field.querySelector('input[type="checkbox"][name^="skip_"]');
    const inputWrap = field.querySelector('.estimate-cost-input');
    const input = field.querySelector('input[type="text"][data-money-input]');
    const skipped = Boolean(checkbox && checkbox.checked);
    if (inputWrap) {{
      inputWrap.hidden = !needsCost || skipped;
    }}
    if (input) {{
      input.required = Boolean(needsCost && !skipped);
      if (skipped) {{
        input.value = "";
      }}
      if (needsCost && !skipped) {{
        const cleaned = input.value.replace(/\\s+/g, "").replace(",", ".").trim();
        if (cleaned !== "" && Number(cleaned) > 0) {{
          hasAtLeastOneCost = true;
        }} else {{
          allVisibleCostsValid = false;
        }}
      }}
    }}
  }});
  const canSubmit = !needsCost || (hasAtLeastOneCost && allVisibleCostsValid);
  if (submitButton) {{
    submitButton.disabled = !canSubmit;
  }}
  if (errorBox) {{
    errorBox.classList.toggle('is-visible', !canSubmit && Boolean(needsCost));
  }}
}}

function updatePaymentFormState(form) {{
  if (!form) {{
    return;
  }}
  const checkbox = form.querySelector('input[name="is_paid"]');
  if (!checkbox) {{
    return;
  }}
  const plannedInput = form.querySelector('input[name="planned_amount"], input[name="amount"]');
  const paidAmountInput = form.querySelector('input[name="paid_amount"]');
  const paidDateInput = form.querySelector('input[name="paid_date"]');
  form.querySelectorAll(".payroll-payment-field").forEach((field) => {{
    field.classList.toggle("is-hidden", !checkbox.checked);
  }});
  if (paidAmountInput) {{
    paidAmountInput.required = checkbox.checked;
    if (checkbox.checked) {{
      if (!paidAmountInput.value.trim() && plannedInput) {{
        paidAmountInput.value = plannedInput.value;
      }}
    }} else {{
      paidAmountInput.value = "";
    }}
  }}
  if (paidDateInput) {{
    paidDateInput.required = checkbox.checked;
    if (checkbox.checked) {{
      if (!paidDateInput.value) {{
        paidDateInput.value = new Date().toISOString().slice(0, 10);
      }}
    }} else {{
      paidDateInput.value = "";
    }}
  }}
}}

document.addEventListener("change", (event) => {{
  const skipCheckbox = event.target.closest('.estimate-form input[type="checkbox"][name^="skip_"]');
  if (!skipCheckbox) {{
    return;
  }}
  const form = skipCheckbox.closest('.estimate-form');
  if (form) {{
    updateEstimateFormState(form);
  }}
}});

window.addEventListener("load", () => {{
  document.querySelectorAll('.contract-create-form').forEach((form) => {{
    const stageCountInput = form.querySelector('[data-stage-count-input]');
    buildContractStageFields(form, stageCountInput ? stageCountInput.value : 1);
  }});
  document.querySelectorAll('.estimate-form').forEach((form) => {{
    updateEstimateFormState(form);
  }});
  document.querySelectorAll('.payroll-payment-form').forEach((form) => {{
    updatePaymentFormState(form);
  }});
  const saved = window.sessionStorage.getItem("auctionScrollY");
  if (saved !== null) {{
    window.sessionStorage.removeItem("auctionScrollY");
    window.setTimeout(() => {{
      window.scrollTo({{ top: Number(saved), behavior: "auto" }});
    }}, 0);
  }}
}});
</script>
</html>"""


def render_dashboard(storage: Storage, owner_chat_id: int) -> str:
    payload = contract_payload(storage, owner_chat_id)
    total_amount = sum(item["total_amount"] for item in payload)
    total_paid = sum(item["paid_amount"] for item in payload)
    total_debt = max(total_amount - total_paid, 0.0)
    total_advance = sum(item["advance_amount"] for item in payload)
    overdue_total = sum(item["overdue_stages"] for item in payload)
    upcoming = storage.upcoming_items(owner_chat_id, within_days=30)
    paid_ratio = (total_paid / total_amount) if total_amount > 0 else 0.0

    stats_html = f"""
    <section class="stats stats-contracts">
      <article class="card stat-card">
        <div class="stat-label">Контрактов</div>
        <div class="stat-value">{len(payload)}</div>
        <div class="stat-note">В реестре контрактов</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Общий объем</div>
        <div class="stat-value">{format_amount(total_amount)}</div>
        <div class="stat-note">Сумма всех контрактов</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Оплачено</div>
        <div class="stat-value">{format_amount(total_paid)}</div>
        <div class="stat-note">{format_percent(paid_ratio * 100)} от общего объема</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Долг заказчиков</div>
        <div class="stat-value">{format_amount(total_debt)}</div>
        <div class="stat-note">Неоплаченный остаток по контрактам</div>
      </article>
    </section>
    """

    contract_rows = []
    for index, item in enumerate(payload, start=1):
        contract = item["contract"]
        progress_width = round(item["paid_ratio"] * 100, 1)
        deadline_tooltip = ""
        if item["first_stage_start_date"] is not None:
            deadline_tooltip = f' class="auction-added-tooltip" data-tooltip="Старт работ: {escape(format_date(item["first_stage_start_date"]))}"'
        contract_rows.append(
            f"""
            <tr>
              <td>
                <a class="contract-table-link" href="/contracts/{contract.id}?owner={owner_chat_id}">
                  <div class="auction-head">
                    <div class="auction-seq">#{index}</div>
                  </div>
                  <div class="timeline-title">{escape(contract.title)}</div>
                </a>
                <div class="contract-table-subtle">{escape(contract.description) if contract.description else 'Описание пока не заполнено'}</div>
                <div class="contract-table-subtle" style="text-align:right;">{f'Заключен: {format_date(contract.signed_date)}' if contract.signed_date is not None else 'Еще не подписан'}</div>
              </td>
              <td class="nowrap" style="text-align:center;">
                <div{deadline_tooltip}>{format_date(contract.end_date)}</div>
              </td>
              <td class="nowrap">
                <div class="amount-value">{format_amount(item["total_amount"])}</div>
              </td>
              <td class="nowrap" style="text-align:center;">
                {format_percent(contract.advance_percent) if contract.advance_percent else '—'}
              </td>
              <td class="nowrap" style="text-align:center;">
                <div>{format_amount(item["paid_amount"])}</div>
                <div class="contract-table-subtle">Долг: {format_amount(item["debt_amount"])}</div>
                <div class="contract-table-subtle">{format_percent(progress_width)} оплачено</div>
              </td>
            </tr>
            """
        )
    contracts_table = "".join(contract_rows) or '<div class="empty">Контракты пока не добавлены.</div>'

    upcoming_items = []
    for item in upcoming[:8]:
        label = "Контракт" if item["entity_type"] == "contract" else "Этап"
        days_left = item["days_left"]
        if days_left < 0:
            due_label = f"Просрочено на {abs(days_left)} дн."
        elif days_left == 0:
            due_label = "Сегодня"
        else:
            due_label = f"Через {days_left} дн."
        upcoming_items.append(
            f"""
            <div class="timeline-item">
              <div class="timeline-date">{format_date(item["end_date"])}</div>
              <div>
                <div class="timeline-title">{label}: {escape(item["title"])}</div>
                <div class="contract-meta">{escape(due_label)}</div>
              </div>
            </div>
            """
        )
    upcoming_html = "".join(upcoming_items) or '<div class="empty">На ближайшие 30 дней дедлайнов нет.</div>'

    return f"""
    {stats_html}
    <section class="card panel" id="contract-registry">
      <div class="panel-head">
        <div>
          <h2 class="panel-title">Реестр контрактов</h2>
          <div class="panel-sub">Контракты, этапы, оплаты и аванс в одном рабочем реестре, по той же логике аккуратной панели, что и в аукционах.</div>
        </div>
        <div class="chip">Рабочий контур</div>
      </div>
      {
        f'''
        <table class="table contract-table">
          <thead>
            <tr>
              <th>Контракт</th>
              <th class="nowrap">Дедлайн</th>
              <th class="nowrap">Сумма</th>
              <th class="nowrap">Аванс</th>
              <th class="nowrap">Оплачено / долг</th>
            </tr>
          </thead>
          <tbody>{contracts_table}</tbody>
        </table>
        '''
        if payload
        else contracts_table
      }
    </section>
    <section class="card panel" style="margin-top: 22px;">
      <div class="panel-head">
        <div>
          <h2 class="panel-title">Добавить контракт</h2>
          <div class="panel-sub">Сразу фиксируем общую сумму, аванс и этапы: суммы и сроки по каждому этапу.</div>
        </div>
        <div class="chip">Новый контракт сразу собирается по этапам</div>
      </div>
      <form class="form-grid contract-create-form" method="post" action="/contracts/new?owner={owner_chat_id}">
        <div class="stats" style="grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));">
          <div class="field">
            <label>Название контракта</label>
            <input type="text" name="title" required>
          </div>
          <div class="field">
            <label>Общая сумма</label>
            <input type="text" name="total_amount" placeholder="15000000" data-money-input="1" required>
          </div>
          <div class="field">
            <label>Дата заключения контракта</label>
            <input type="date" name="signed_date" required>
          </div>
          <div class="field">
            <label>Количество этапов</label>
            <input type="number" name="stage_count" min="1" value="1" data-stage-count-input="1" required>
          </div>
          <label class="advance-toggle">
            <input class="toggle-checkbox" type="checkbox" name="has_advance" value="1"> У контракта есть аванс
          </label>
          <div class="field advance-field is-hidden">
            <label>Процент аванса</label>
            <input type="text" name="advance_percent" placeholder="Например, 30">
          </div>
        </div>
        <div class="field">
          <label>Описание</label>
          <textarea name="description" placeholder="Кратко о контракте"></textarea>
        </div>
        <div class="section-stack" data-stage-container="1">
          <div class="permission-box">
            <strong>Этап 1</strong>
            <div class="stats" style="grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));">
              <div class="field">
                <label>Сумма этапа</label>
                <input type="text" name="stage_amount_1" data-money-input="1" required>
              </div>
              <div class="field">
                <label>Старт работ по этапу</label>
                <input type="date" name="stage_start_date_1">
              </div>
              <div class="field">
                <label>Дедлайн этапа</label>
                <input type="date" name="stage_end_date_1" required>
              </div>
            </div>
            <div class="field">
              <label>Примечание к этапу</label>
              <textarea name="stage_notes_1" placeholder="Что входит в этап"></textarea>
            </div>
          </div>
        </div>
        <div class="action-row">
          <button class="submit-btn" type="submit">Создать контракт</button>
        </div>
      </form>
    </section>
    <section class="card panel" style="margin-top: 22px;">
      <div class="panel-head">
        <div>
          <h2 class="panel-title">Ближайшие сроки</h2>
          <div class="panel-sub">Контрактный календарь, который уже можно развивать дальше в полноценный контроль сроков.</div>
        </div>
      </div>
      <div class="timeline">{upcoming_html}</div>
    </section>
    """


def render_contract_detail(storage: Storage, owner_chat_id: int, contract_id: int, current_user: dict | None = None, flash_message: str = "") -> str:
    payload = next((item for item in contract_payload(storage, owner_chat_id) if item["contract"].id == contract_id), None)
    if payload is None:
        return '<div class="card panel"><div class="empty">Контракт не найден.</div></div>'

    contract = payload["contract"]
    advance_percent = contract.advance_percent or 0.0
    add_stage_button = ""
    if can_edit_contract_stage_controls(current_user):
        add_stage_button = f"""
        <details class="status-menu">
          <summary><span class="secondary-btn">Добавить этап</span></summary>
          <div class="status-popover">
            <form class="form-grid" method="post" action="/contracts/{contract.id}/stages/new?owner={owner_chat_id}">
              <div class="field">
                <label>Номер этапа</label>
                <input type="number" min="1" name="position" value="{len(payload["stages"]) + 1}" required>
              </div>
              <div class="field">
                <label>Старт работ по этапу</label>
                <input type="date" name="start_date">
              </div>
              <div class="field">
                <label>Дедлайн этапа</label>
                <input type="date" name="end_date" required>
              </div>
              <div class="field">
                <label>Сумма этапа</label>
                <input type="text" name="amount" placeholder="1500000" required>
              </div>
              <div class="field">
                <label>Примечание</label>
                <textarea name="notes" placeholder="Что входит в этап"></textarea>
              </div>
              <button class="submit-btn" type="submit">Добавить этап</button>
            </form>
          </div>
        </details>
        """
    stages_html = "".join(
        f"""
        <tr>
          <td>
            <div class="timeline-title">{escape(stage.name)}</div>
            {f'<div class="contract-table-subtle">{escape(stage.notes)}</div>' if stage.notes else ''}
          </td>
          <td>{render_stage_status_form(owner_chat_id, contract.id, stage, current_user)}</td>
          <td>{render_stage_deadline_form(owner_chat_id, contract.id, stage, current_user)}</td>
          <td>{render_stage_amount_form(owner_chat_id, contract.id, stage, current_user)}</td>
          <td>{format_amount(stage.amount * advance_percent / 100) if contract.advance_percent else 'Без аванса'}</td>
          <td class="nowrap" style="text-align:center;">{render_stage_invoice_form(owner_chat_id, contract.id, stage, current_user, "advance")}</td>
          <td>{format_amount(stage.amount - (stage.amount * advance_percent / 100)) if contract.advance_percent else format_amount(stage.amount)}</td>
          <td class="nowrap" style="text-align:center;">{render_stage_invoice_form(owner_chat_id, contract.id, stage, current_user, "final")}</td>
          <td>{render_stage_payment_form(owner_chat_id, contract.id, stage, current_user)}</td>
        </tr>
        """
        for stage in payload["stages"]
    ) or '<tr><td colspan="9">Этапов пока нет.</td></tr>'

    payments_html = "".join(
        f"""
        <tr>
          <td>{format_date(payment.payment_date)}</td>
          <td>{format_amount(payment.amount)}</td>
        </tr>
        """
        for payment in payload["payments"]
    ) or '<tr><td colspan="2">Оплат пока нет.</td></tr>'

    flash_html = f'<div class="flash">{escape(flash_message)}</div>' if flash_message else ""
    return f"""
    <section class="card panel" style="margin-top:22px;">
      <div class="panel-head">
        <div>
          <h2 class="panel-title">{escape(contract.title)}</h2>
          <div class="panel-sub">
            {escape(contract.description) if contract.description else 'Описание пока не заполнено.'}
          </div>
        </div>
        <a class="chip" href="/?owner={owner_chat_id}">← Назад к дашборду</a>
      </div>
      <div class="info-row">
        <span class="chip">ID: {contract.id}</span>
        {render_contract_signed_date_chip(owner_chat_id, contract, current_user)}
        <span class="chip">Старт работ: {format_date(payload["first_stage_start_date"]) if payload["first_stage_start_date"] is not None else "Не задан"}</span>
        <span class="chip">Дедлайн: {format_date(contract.end_date)}</span>
        <span class="chip">Этапов: {len(payload["stages"])}</span>
        <span class="chip">Оплат: {len(payload["payments"])}</span>
      </div>
    </section>
    <section class="stats">
      <article class="card stat-card">
        <div class="stat-label">Общая сумма</div>
        <div class="stat-value">{format_amount(payload["total_amount"])}</div>
        <div class="stat-note">Полная сумма контракта</div>
      </article>
      {render_contract_advance_card(owner_chat_id, contract, payload, current_user)}
      <article class="card stat-card">
        <div class="stat-label">Оплачено</div>
        <div class="stat-value">{format_amount(payload["paid_amount"])}</div>
        <div class="stat-note">{format_percent(payload["paid_ratio"] * 100)} от суммы контракта</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Долг</div>
        <div class="stat-value">{format_amount(payload["debt_amount"])}</div>
        <div class="stat-note">Неоплаченный остаток</div>
      </article>
    </section>
    {flash_html}
    <section class="card panel" style="margin-top:22px;">
      <div class="panel-head">
        <div>
          <h2 class="panel-title">Этапы контракта</h2>
          <div class="panel-sub">Рабочий реестр этапов по той же логике, что и в аукционах.</div>
        </div>
        {add_stage_button}
      </div>
      <table class="table contract-table">
        <thead>
          <tr>
            <th>Этап</th>
            <th>Статус</th>
            <th class="nowrap">Дедлайн</th>
            <th class="nowrap">Сумма этапа</th>
            <th class="nowrap">Аванс, ₽</th>
            <th class="nowrap">Счет на аванс</th>
            <th class="nowrap">Остаток</th>
            <th class="nowrap">Счет на остаток</th>
            <th class="nowrap">Оплата</th>
          </tr>
        </thead>
        <tbody>{stages_html}</tbody>
      </table>
    </section>
    <section class="grid" style="margin-top:22px;">
      <section class="card panel">
        <div class="panel-head">
          <div>
            <h2 class="panel-title">Оплаты</h2>
            <div class="panel-sub">Все платежи по контракту в одном месте</div>
          </div>
        </div>
        <table class="table">
          <thead>
            <tr>
              <th>Дата</th>
              <th>Сумма</th>
            </tr>
          </thead>
          <tbody>{payments_html}</tbody>
        </table>
      </section>
      <aside class="section-stack">
        <section class="card panel">
          <div class="panel-head">
            <div>
              <h2 class="panel-title">Добавить оплату</h2>
              <div class="panel-sub">Ввод платежа прямо из карточки</div>
            </div>
          </div>
          <form class="form-grid" method="post" action="/contracts/{contract.id}/payments/new?owner={owner_chat_id}">
            <div class="field">
              <label>Дата оплаты</label>
              <input type="text" name="payment_date" placeholder="31-07-2026" required>
            </div>
            <div class="field">
              <label>Сумма оплаты</label>
              <input type="text" name="amount" placeholder="500000" required>
            </div>
            <button class="submit-btn" type="submit">Добавить оплату</button>
          </form>
        </section>
      </aside>
    </section>
    """


def payroll_row_metrics(row) -> dict[str, float | str]:
    paid_total = row.advance_card_paid_amount + row.advance_cash_paid_amount + row.salary_paid_amount + row.bonus_paid_amount
    balance = round(row.accrued_amount - paid_total, 2)
    debt_amount = max(balance, 0.0)
    overpaid_amount = abs(min(balance, 0.0))
    if overpaid_amount > 0.009:
        status_label = "Переплата"
        status_class = "chip danger"
    elif row.accrued_amount > 0 and debt_amount <= 0.009:
        status_label = "Закрыто"
        status_class = "chip ok"
    elif paid_total > 0:
        status_label = "Частично выплачено"
        status_class = "chip warn"
    else:
        status_label = "Не выплачено"
        status_class = "chip"
    return {
        "paid_total": paid_total,
        "balance": balance,
        "debt_amount": debt_amount,
        "overpaid_amount": overpaid_amount,
        "status_label": status_label,
        "status_class": status_class,
    }


def payroll_deadline_for_kind(payroll_month: date, payment_kind: str) -> date:
    if payment_kind in {"advance_card", "advance_cash"}:
        return payroll_month.replace(day=20)
    if payroll_month.month == 12:
        return date(payroll_month.year + 1, 1, 5)
    return date(payroll_month.year, payroll_month.month + 1, 5)


def payroll_deadline_header(payroll_month: date, payment_kind: str, rows) -> str:
    labels = {
        "advance_card": "до 20 числа",
        "advance_cash": "до 20 числа",
        "salary": "до 5 числа след. месяца",
        "bonus": "до 5 числа след. месяца",
    }
    field_map = {
        "advance_card": ("advance_card_amount", "advance_card_paid_amount"),
        "advance_cash": ("advance_cash_amount", "advance_cash_paid_amount"),
        "salary": ("salary_amount", "salary_paid_amount"),
        "bonus": ("bonus_amount", "bonus_paid_amount"),
    }
    deadline = payroll_deadline_for_kind(payroll_month, payment_kind)
    today = datetime.now(VLADIVOSTOK_TZ).date()
    days_left = (deadline - today).days
    planned_field, paid_field = field_map[payment_kind]
    has_open_items = any(
        getattr(row, planned_field) > getattr(row, paid_field) + 0.009
        for row in rows
    )
    deadline_class = "payroll-col-deadline danger" if has_open_items and days_left <= 2 else "payroll-col-deadline"
    return (
        f'<div class="payroll-col-head">{escape(labels[payment_kind])}</div>'
        f'<div class="{deadline_class}">{format_date(deadline)}</div>'
    )


def payroll_expected_salary_amount(row) -> float:
    return round(max(row.accrued_amount - row.advance_card_amount - row.advance_cash_amount, 0.0), 2)


def render_payroll_amount_editor(owner_chat_id: int, payroll_month: date, row, field_name: str, label: str, value: float, current_user: dict | None) -> str:
    amount_html = format_amount(value) if abs(value) > 0.009 else "—"
    amount_class = "payroll-amount"
    if not has_permission(current_user, "payroll", "edit"):
        return f'<span class="{amount_class}">{amount_html}</span>'
    return f"""
    <details class="status-menu">
      <summary><span class="{amount_class}">{amount_html}</span></summary>
      <div class="status-popover">
        <form class="form-grid payroll-amount-form" method="post" action="/payroll/entries/{row.employee_id}/amount?owner={owner_chat_id}&month={payroll_month.strftime('%Y-%m')}">
          <input type="hidden" name="field_name" value="{escape(field_name)}">
          <div class="field">
            <label>{escape(label)}</label>
            <input type="text" name="amount" value="{escape(format_amount_input(value))}" data-money-input="1" required>
          </div>
          <button class="submit-btn" type="submit">Сохранить</button>
        </form>
      </div>
    </details>
    """


def render_payroll_payment_editor(owner_chat_id: int, payroll_month: date, row, payment_kind: str, label: str, planned_amount: float, paid_amount: float, paid_date: date | None, current_user: dict | None) -> str:
    note_class = "contract-table-subtle payroll-payment-note"
    mismatch_note = ""
    form_value = planned_amount
    planned_display = format_amount(planned_amount) if planned_amount > 0.009 else "—"
    if payment_kind == "salary":
        expected_amount = payroll_expected_salary_amount(row)
        if abs(planned_amount - expected_amount) > 0.009:
            amount_class = "payroll-amount is-alert"
            mismatch_note = f'Должно быть:<br>{format_amount(expected_amount)}'
            form_value = expected_amount
        elif paid_amount > 0.009:
            if planned_amount > 0.009 and paid_amount + 0.009 < planned_amount:
                amount_class = "payroll-amount is-partial"
            else:
                amount_class = "payroll-amount is-paid"
        else:
            amount_class = "payroll-amount"
    elif paid_amount > 0.009:
        if planned_amount > 0.009 and paid_amount + 0.009 < planned_amount:
            amount_class = "payroll-amount is-partial"
        else:
            amount_class = "payroll-amount is-paid"
    else:
        amount_class = "payroll-amount"
    if paid_amount > 0.009:
        paid_note = (
            f'Выплачено:<br>{format_amount(paid_amount)}<br>{format_date(paid_date)}'
            if paid_date is not None
            else f'Выплачено:<br>{format_amount(paid_amount)}'
        )
    else:
        paid_note = "" if planned_amount <= 0.009 else "Не выплачено"
    display = f"""
    <div class="{amount_class}">{planned_display}</div>
    {f'<div class="{note_class} danger">{mismatch_note}</div>' if mismatch_note else ""}
    {f'<div class="{note_class}">{paid_note}</div>' if paid_note else ""}
    """
    if not has_permission(current_user, "payroll", "edit"):
        return display
    checked_attr = "checked" if paid_amount > 0.009 else ""
    return f"""
    <details class="status-menu">
      <summary>{display}</summary>
      <div class="status-popover">
        <form class="form-grid payroll-payment-form" method="post" action="/payroll/entries/{row.employee_id}/payment?owner={owner_chat_id}&month={payroll_month.strftime('%Y-%m')}">
          <input type="hidden" name="payment_kind" value="{escape(payment_kind)}">
          <div class="field">
            <label>{escape(label)} начислено</label>
            <input type="text" name="planned_amount" value="{escape(format_amount_input(form_value))}" data-money-input="1" required>
          </div>
          <label class="advance-toggle">
            <input class="toggle-checkbox" type="checkbox" name="is_paid" value="1" {checked_attr}> Выплата зафиксирована
          </label>
          <div class="field payroll-payment-field{' is-hidden' if paid_amount <= 0.009 else ''}">
            <label>Фактически выплачено</label>
            <input type="text" name="paid_amount" value="{escape(format_amount_input(paid_amount)) if paid_amount > 0.009 else ''}" data-money-input="1">
          </div>
          <div class="field payroll-payment-field{' is-hidden' if paid_amount <= 0.009 else ''}">
            <label>Дата выплаты</label>
            <input type="date" name="paid_date" value="{paid_date.isoformat() if paid_date is not None else ''}">
          </div>
          <button class="submit-btn" type="submit">Сохранить выплату</button>
        </form>
      </div>
    </details>
    """


def render_payroll_note_editor(owner_chat_id: int, payroll_month: date, row, current_user: dict | None) -> str:
    note_text = row.note.strip() or ("Добавить заметку" if has_permission(current_user, "payroll", "edit") else "")
    note_class = "contract-table-subtle"
    if not note_text:
        return ""
    if not has_permission(current_user, "payroll", "edit"):
        return f'<div class="{note_class}">{escape(note_text)}</div>'
    return f"""
    <details class="status-menu">
      <summary><div class="{note_class}">{escape(note_text)}</div></summary>
      <div class="status-popover">
        <form class="form-grid" method="post" action="/payroll/entries/{row.employee_id}/note?owner={owner_chat_id}&month={payroll_month.strftime('%Y-%m')}">
          <div class="field">
            <label>Заметка по выплате</label>
            <textarea name="note" placeholder="Например, договорились доплатить отдельно">{escape(row.note)}</textarea>
          </div>
          <button class="submit-btn" type="submit">Сохранить заметку</button>
        </form>
      </div>
    </details>
    """


def payable_metrics(entry) -> dict[str, float | str]:
    outstanding = round(max(entry.amount - entry.paid_amount, 0.0), 2)
    overpaid = round(max(entry.paid_amount - entry.amount, 0.0), 2)
    if overpaid > 0.009:
        return {
            "outstanding": outstanding,
            "overpaid": overpaid,
            "status_label": "Переплата",
            "status_class": "chip danger",
        }
    if entry.amount > 0 and outstanding <= 0.009:
        return {
            "outstanding": outstanding,
            "overpaid": overpaid,
            "status_label": "Закрыто",
            "status_class": "chip ok",
        }
    if entry.paid_amount > 0:
        return {
            "outstanding": outstanding,
            "overpaid": overpaid,
            "status_label": "Частично оплачено",
            "status_class": "chip warn",
        }
    return {
        "outstanding": outstanding,
        "overpaid": overpaid,
        "status_label": "Не оплачено",
        "status_class": "chip",
    }


def payable_query_suffix(owner_chat_id: int, active_tab: str = "active", counterparty_filter: str = "", sort_key: str = "", sort_order: str = "") -> str:
    suffix = f"?owner={owner_chat_id}&tab={active_tab}"
    if counterparty_filter:
        suffix += f"&counterparty={quote_plus(counterparty_filter)}"
    if sort_key and sort_order:
        suffix += f"&sort={quote_plus(sort_key)}&order={quote_plus(sort_order)}"
    return suffix


def normalize_payables_sort(sort_key: str, sort_order: str) -> tuple[str, str]:
    allowed = {"counterparty", "document_ref", "object_name", "comment", "amount", "paid_amount", "outstanding", "due_date"}
    if sort_key not in allowed or sort_order not in {"asc", "desc"}:
        return "", ""
    return sort_key, sort_order


def next_payables_sort(clicked_key: str, current_key: str, current_order: str) -> tuple[str, str]:
    if current_key != clicked_key:
        return clicked_key, "asc"
    if current_order == "asc":
        return clicked_key, "desc"
    return "", ""


def sort_payable_entries(entries, sort_key: str, sort_order: str):
    effective_key = sort_key or "created_at"
    effective_order = sort_order or "asc"
    reverse = effective_order == "desc"

    def key(entry):
        metrics = payable_metrics(entry)
        return {
            "counterparty": entry.counterparty.lower(),
            "document_ref": entry.document_ref.lower(),
            "object_name": entry.object_name.lower(),
            "comment": entry.comment.lower(),
            "amount": entry.amount,
            "paid_amount": entry.paid_amount,
            "outstanding": metrics["outstanding"],
            "due_date": entry.due_date,
            "created_at": entry.created_at,
        }[effective_key]

    return sorted(entries, key=key, reverse=reverse)


def render_payables_sort_link(owner_chat_id: int, active_tab: str, counterparty_filter: str, current_sort_key: str, current_sort_order: str, label: str, clicked_key: str) -> str:
    next_key, next_order = next_payables_sort(clicked_key, current_sort_key, current_sort_order)
    href = f"/payables{payable_query_suffix(owner_chat_id, active_tab, counterparty_filter, next_key, next_order)}"
    is_active = current_sort_key == clicked_key
    arrow = " ↑" if is_active and current_sort_order == "asc" else " ↓" if is_active and current_sort_order == "desc" else ""
    return f'<a class="contract-table-link{" active-sort" if is_active else ""}" data-keep-payables-scroll="1" href="{href}#payables-registry">{escape(label)}{arrow}</a>'


def render_payable_counterparty_editor(owner_chat_id: int, entry, current_user: dict | None, active_tab: str = "active", counterparty_filter: str = "", sort_key: str = "", sort_order: str = "", counterparty_options: list[str] | None = None) -> str:
    display = f'<strong>{escape(entry.counterparty)}</strong>'
    if not has_permission(current_user, "payables", "edit"):
        return display
    return f"""
    <details class="status-menu lot-menu">
      <summary>{display}</summary>
      <div class="status-popover lot-form">
        <form class="form-grid" method="post" action="/payables/{entry.id}/field{payable_query_suffix(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order)}">
          <input type="hidden" name="field_name" value="counterparty">
          <div class="field">
            <label>Контрагент</label>
            <div class="autocomplete-wrap">
              <input id="payable-counterparty-{entry.id}" class="counterparty-autocomplete-input" type="text" name="value" value="{escape(entry.counterparty)}" autocomplete="off" required>
              <div class="autocomplete-list">
                {"".join(f'<button class="autocomplete-option" type="button" data-value="{escape(name)}" data-value-lower="{escape(name.lower())}">{escape(name)}</button>' for name in (counterparty_options or []))}
              </div>
            </div>
          </div>
          <button class="submit-btn" type="submit">Сохранить</button>
        </form>
      </div>
    </details>
    """


def render_payable_document_form(owner_chat_id: int, entry, current_user: dict | None, active_tab: str = "active", counterparty_filter: str = "", sort_key: str = "", sort_order: str = "") -> str:
    doc_label = entry.document_ref.strip() or "Документ не указан"
    date_note = format_date(entry.document_date) if entry.document_date is not None else "Дата не указана"
    display = f"""
    <div class="contract-table-subtle" style="margin-top:0;">{escape(doc_label)}</div>
    <div class="contract-table-subtle">{escape(date_note)}</div>
    """
    if not has_permission(current_user, "payables", "edit"):
        return display
    return f"""
    <details class="status-menu lot-menu">
      <summary>{display}</summary>
      <div class="status-popover lot-form">
        <form class="form-grid" method="post" action="/payables/{entry.id}/field{payable_query_suffix(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order)}">
          <input type="hidden" name="field_name" value="document_ref">
          <div class="field">
            <label>Счет / документ</label>
            <input type="text" name="value" value="{escape(entry.document_ref)}" required>
          </div>
          <div class="field">
            <label>Дата документа</label>
            <input type="date" name="document_date" value="{entry.document_date.isoformat() if entry.document_date is not None else ''}">
          </div>
          <button class="submit-btn" type="submit">Сохранить</button>
        </form>
      </div>
    </details>
    """


def render_payable_text_cell_editor(owner_chat_id: int, entry, current_user: dict | None, field_name: str, label: str, value: str, placeholder: str, active_tab: str = "active", counterparty_filter: str = "", sort_key: str = "", sort_order: str = "") -> str:
    display = escape(value) if value else "—"
    if not has_permission(current_user, "payables", "edit"):
        return display
    input_html = (
        f'<textarea name="value" placeholder="{escape(placeholder)}">{escape(value)}</textarea>'
        if field_name == "comment"
        else f'<input type="text" name="value" value="{escape(value)}" placeholder="{escape(placeholder)}">'
    )
    return f"""
    <details class="status-menu lot-menu">
      <summary>{display}</summary>
      <div class="status-popover lot-form">
        <form class="form-grid" method="post" action="/payables/{entry.id}/field{payable_query_suffix(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order)}">
          <input type="hidden" name="field_name" value="{field_name}">
          <div class="field">
            <label>{label}</label>
            {input_html}
          </div>
          <button class="submit-btn" type="submit">Сохранить</button>
        </form>
      </div>
    </details>
    """


def render_payable_amount_editor(owner_chat_id: int, entry, current_user: dict | None, active_tab: str = "active", counterparty_filter: str = "", sort_key: str = "", sort_order: str = "") -> str:
    display = format_amount(entry.amount)
    if not has_permission(current_user, "payables", "edit"):
        return display
    return f"""
    <details class="status-menu lot-menu">
      <summary>{display}</summary>
      <div class="status-popover lot-form">
        <form class="form-grid" method="post" action="/payables/{entry.id}/field{payable_query_suffix(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order)}">
          <input type="hidden" name="field_name" value="amount">
          <div class="field">
            <label>Сумма, ₽</label>
            <input type="text" name="value" value="{escape(format_amount_input(entry.amount))}" data-money-input="1" required>
          </div>
          <button class="submit-btn" type="submit">Сохранить</button>
        </form>
      </div>
    </details>
    """


def render_payable_payment_editor(owner_chat_id: int, entry, current_user: dict | None, active_tab: str = "active", counterparty_filter: str = "", sort_key: str = "", sort_order: str = "") -> str:
    metrics = payable_metrics(entry)
    paid_note = format_date(entry.paid_date) if entry.paid_date is not None else ""
    display = f"""
    {f'<div class="payroll-amount{" is-partial" if metrics["outstanding"] > 0.009 else " is-paid"}">{format_amount(entry.paid_amount)}</div>' if entry.paid_amount > 0.009 else ''}
    {f'<div class="contract-table-subtle">{escape(paid_note)}</div>' if entry.paid_amount > 0.009 and paid_note else ''}
    <div><span class="{metrics["status_class"]}">{metrics["status_label"]}</span></div>
    """
    if not has_permission(current_user, "payables", "edit"):
        return display
    checked_attr = "checked" if entry.paid_amount > 0.009 else ""
    return f"""
    <details class="status-menu">
      <summary>{display}</summary>
      <div class="status-popover">
        <form class="form-grid payroll-payment-form" method="post" novalidate action="/payables/{entry.id}/payment{payable_query_suffix(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order)}">
          <div class="field">
            <label>Начислено к оплате</label>
            <input type="text" name="amount" value="{escape(format_amount_input(entry.amount))}" data-money-input="1" required>
          </div>
          <label class="advance-toggle">
            <input class="toggle-checkbox" type="checkbox" name="is_paid" value="1" {checked_attr}> Оплата зафиксирована
          </label>
          <div class="field payroll-payment-field{' is-hidden' if entry.paid_amount <= 0.009 else ''}">
            <label>Фактически оплачено</label>
            <input type="text" name="paid_amount" value="{escape(format_amount_input(entry.paid_amount)) if entry.paid_amount > 0.009 else ''}" data-money-input="1">
          </div>
          <div class="field payroll-payment-field{' is-hidden' if entry.paid_amount <= 0.009 else ''}">
            <label>Дата оплаты</label>
            <input type="date" name="paid_date" value="{entry.paid_date.isoformat() if entry.paid_date is not None else ''}">
          </div>
          <div class="action-row">
            <button class="submit-btn" type="submit">Сохранить оплату</button>
            {f'<button class="secondary-btn danger" type="submit" formnovalidate formaction="/payables/{entry.id}/payment/reset{payable_query_suffix(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order)}">Отменить оплату</button>' if entry.paid_amount > 0.009 else ''}
          </div>
        </form>
      </div>
    </details>
    """


def render_payable_due_cell(entry) -> str:
    metrics = payable_metrics(entry)
    today = datetime.now(VLADIVOSTOK_TZ).date()
    if metrics["outstanding"] <= 0.009:
        css_class = "contract-table-subtle"
    elif entry.due_date < today:
        css_class = "deadline-meta danger"
    else:
        css_class = "deadline-meta ok"
    return f'<span class="{css_class}">{escape(format_date(entry.due_date))}</span>'


def is_payable_archived(entry) -> bool:
    if is_payable_deleted(entry):
        return False
    return payable_metrics(entry)["outstanding"] <= 0.009


def is_payable_deleted(entry) -> bool:
    return entry.deleted_at is not None


def render_payable_delete_actions(owner_chat_id: int, entry, active_tab: str, current_user: dict | None, counterparty_filter: str = "", sort_key: str = "", sort_order: str = "") -> str:
    if current_user is None:
        return ""
    if active_tab == "deleted":
        if not has_permission(current_user, "payables", "view"):
            return ""
    elif not has_permission(current_user, "payables", "edit"):
        return ""
    delete_icon = """
    <svg class="icon-trash" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M4 7h16" />
      <path d="M10 11v6" />
      <path d="M14 11v6" />
      <path d="M9 4h6l1 2H8l1-2Z" />
      <path d="M7 7h10l-1 12a2 2 0 0 1-2 2h-4a2 2 0 0 1-2-2L7 7Z" />
    </svg>
    """
    restore_icon = """
    <svg class="icon-restore" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M9 7H5v4" />
      <path d="M5 11a7 7 0 1 0 2-4" />
    </svg>
    """
    if active_tab == "deleted":
        purge_action = ""
        if has_active_admin_mode(current_user):
            purge_action = f"""
            <form class="auction-delete-form" method="post" action="/payables/{entry.id}/purge{payable_query_suffix(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order)}">
              <button class="icon-btn danger" type="submit" title="Удалить навсегда">{delete_icon}</button>
            </form>
            """
        return f"""
        <form class="auction-delete-form" method="post" action="/payables/{entry.id}/restore{payable_query_suffix(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order)}">
          <button class="icon-btn" type="submit" title="Вернуть в реестр">{restore_icon}</button>
        </form>
        {purge_action}
        """
    return f"""
    <form class="auction-delete-form" method="post" action="/payables/{entry.id}/delete{payable_query_suffix(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order)}">
      <button class="icon-btn danger" type="submit" title="Переместить в удаленные">{delete_icon}</button>
    </form>
    """


def render_payables_section(storage: Storage, owner_chat_id: int, current_user: dict | None, active_tab: str = "active", flash_message: str = "", success: bool = False, counterparty_filter: str = "", sort_key: str = "", sort_order: str = "") -> str:
    storage.ensure_payables_seed(owner_chat_id)
    all_entries = storage.list_payables(owner_chat_id)
    active_entries = [entry for entry in all_entries if not is_payable_archived(entry) and not is_payable_deleted(entry)]
    archived_entries = [entry for entry in all_entries if is_payable_archived(entry) and not is_payable_deleted(entry)]
    deleted_entries = [entry for entry in all_entries if is_payable_deleted(entry)]
    source_entries = deleted_entries if active_tab == "deleted" else archived_entries if active_tab == "archive" else active_entries
    entries = [entry for entry in source_entries if not counterparty_filter or entry.counterparty == counterparty_filter]
    entries = sort_payable_entries(entries, sort_key, sort_order)
    total_outstanding = sum(payable_metrics(entry)["outstanding"] for entry in active_entries)
    overdue_count = sum(
        1 for entry in active_entries
        if payable_metrics(entry)["outstanding"] > 0.009 and entry.due_date < datetime.now(VLADIVOSTOK_TZ).date()
    )
    available_counterparties = sorted({entry.counterparty for entry in all_entries if entry.counterparty})
    counterparty_totals = {
        name: sum(payable_metrics(entry)["outstanding"] for entry in all_entries if entry.counterparty == name and not is_payable_deleted(entry))
        for name in available_counterparties
    }
    flash_html = f'<div class="flash{" ok" if success else ""}">{escape(flash_message)}</div>' if flash_message else ""
    add_section = ""
    if has_permission(current_user, "payables", "edit") and active_tab != "deleted":
        add_section = f"""
        <section class="card panel" style="margin-top:22px;">
          <div class="panel-head">
            <div>
              <h2 class="panel-title">Добавить задолженность</h2>
              <div class="panel-sub">Укажите контрагента, основание, объект, комментарий, сумму и срок оплаты.</div>
            </div>
          </div>
          <form class="form-grid payable-create-grid" method="post" action="/payables/new{payable_query_suffix(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order)}">
            <div class="field span-4">
              <label>Контрагент</label>
              <div class="autocomplete-wrap">
                <input id="new-payable-counterparty" class="counterparty-autocomplete-input" type="text" name="counterparty" placeholder="Например, ВЛ Снаб" autocomplete="off" required>
                <div class="autocomplete-list">
                  {"".join(f'<button class="autocomplete-option" type="button" data-value="{escape(name)}" data-value-lower="{escape(name.lower())}">{escape(name)}</button>' for name in available_counterparties)}
                </div>
              </div>
            </div>
            <div class="field span-2">
              <label>Счет / документ</label>
              <input type="text" name="document_ref" placeholder="№ 18" required>
            </div>
            <div class="field">
              <label>Дата документа</label>
              <input type="date" name="document_date">
            </div>
            <div class="field">
              <label>Объект</label>
              <input type="text" name="object_name" placeholder="Например, Строитель">
            </div>
            <div class="field span-2">
              <label>Сумма, ₽</label>
              <input type="text" name="amount" data-money-input="1" placeholder="114000" required>
            </div>
            <div class="field span-2">
              <label>Комментарий</label>
              <textarea name="comment" placeholder="Например, щебень"></textarea>
            </div>
            <div class="field span-2">
              <label>Срок оплаты</label>
              <input type="date" name="due_date">
            </div>
            <div class="field span-2">
              <label>Или срок, дней</label>
              <input type="number" name="due_days" min="1" step="1" placeholder="Например, 30">
            </div>
            <button class="submit-btn payable-create-submit" type="submit">Добавить в реестр</button>
          </form>
        </section>
        """
    filtered_total = sum(payable_metrics(entry)["outstanding"] for entry in entries)
    filtered_paid_total = sum(entry.paid_amount for entry in entries)
    rows_html = "".join(
        f"""
        <tr>
          <td>
            <span class="status-chip-tooltip" data-tooltip="Итого задолженность по поставщику: {escape(format_amount(counterparty_totals.get(entry.counterparty, 0.0)))}">
              {render_payable_counterparty_editor(owner_chat_id, entry, current_user, active_tab, counterparty_filter, sort_key, sort_order, available_counterparties)}
            </span>
            <div class="contract-table-subtle">Добавил: {escape(entry.created_by_name.strip() or 'Автор неизвестен')}</div>
          </td>
          <td>{render_payable_document_form(owner_chat_id, entry, current_user, active_tab, counterparty_filter, sort_key, sort_order)}</td>
          <td>{render_payable_text_cell_editor(owner_chat_id, entry, current_user, "object_name", "Объект", entry.object_name, "Например, Строитель", active_tab, counterparty_filter, sort_key, sort_order)}</td>
          <td>{render_payable_text_cell_editor(owner_chat_id, entry, current_user, "comment", "Комментарий", entry.comment, "Например, щебень", active_tab, counterparty_filter, sort_key, sort_order)}</td>
          <td class="nowrap" style="text-align:center;">{render_payable_amount_editor(owner_chat_id, entry, current_user, active_tab, counterparty_filter, sort_key, sort_order)}</td>
          <td>{render_payable_payment_editor(owner_chat_id, entry, current_user, active_tab, counterparty_filter, sort_key, sort_order)}</td>
          <td class="nowrap" style="text-align:center;">{format_amount(payable_metrics(entry)["outstanding"]) if payable_metrics(entry)["outstanding"] > 0.009 else '—'}</td>
          <td class="nowrap" style="text-align:center;">{render_payable_due_cell(entry)}</td>
          <td class="payables-action-col">{render_payable_delete_actions(owner_chat_id, entry, active_tab, current_user, counterparty_filter, sort_key, sort_order)}</td>
        </tr>
        """
        for entry in entries
    )
    register_html = f"""
    <section class="card panel" style="margin-top:18px;">
      <div class="panel-head">
        <div>
          <h2 class="panel-title">{"Удаленные строки кредиторки" if active_tab == "deleted" else "Архив кредиторки" if active_tab == "archive" else "Реестр кредиторки"}</h2>
          <div class="panel-sub">{"Строки, удаленные из реестра. Их можно вернуть, а окончательно очистить только админом." if active_tab == "deleted" else "Полностью закрытые задолженности перед поставщиками." if active_tab == "archive" else "Кому должны, по какому документу, за какой объект и до какого срока нужно закрыть оплату."}</div>
        </div>
        <div class="chip">{"Удалено строк" if active_tab == "deleted" else "Закрыто на сумму" if active_tab == "archive" else "Итого долг"}: {len(entries) if active_tab == "deleted" else format_amount(filtered_paid_total if active_tab == "archive" else filtered_total)}</div>
      </div>
      <table class="table contract-table">
        <thead>
          <tr>
            <th>{render_payables_sort_link(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order, "Контрагент", "counterparty")}</th>
            <th>{render_payables_sort_link(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order, "Основание", "document_ref")}</th>
            <th>{render_payables_sort_link(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order, "Объект", "object_name")}</th>
            <th>{render_payables_sort_link(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order, "Комментарий", "comment")}</th>
            <th class="nowrap">{render_payables_sort_link(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order, "Сумма", "amount")}</th>
            <th class="nowrap">{render_payables_sort_link(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order, "Оплата", "paid_amount")}</th>
            <th class="nowrap">{render_payables_sort_link(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order, "Остаток", "outstanding")}</th>
            <th class="nowrap">{render_payables_sort_link(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order, "Срок оплаты", "due_date")}</th>
            <th class="payables-action-col"></th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </section>
    """ if entries else f"""
    <section class="card panel" style="margin-top:18px;">
      <div class="panel-sub">{"По выбранному контрагенту в удаленных пока нет записей." if counterparty_filter and active_tab == "deleted" else "По выбранному контрагенту в архиве пока нет записей." if counterparty_filter and active_tab == "archive" else "По выбранному контрагенту в работе пока нет записей." if counterparty_filter else "Удаленные пока пусты." if active_tab == "deleted" else "Архив пока пуст. Как только задолженность будет закрыта, она окажется здесь." if active_tab == "archive" else "В работе пока нет записей. Добавьте первую задолженность, чтобы начать вести кредиторку."}</div>
    </section>
    """
    tab_links = f"""
    <div class="tab-row">
      <a class="tab-btn{" active" if active_tab == "active" else ""}" data-keep-payables-scroll="1" href="/payables{payable_query_suffix(owner_chat_id, 'active', counterparty_filter, sort_key, sort_order)}#payables-registry">
        В работе
        <span class="tab-count">{len(active_entries)}</span>
      </a>
      <a class="tab-btn{" active" if active_tab == "archive" else ""}" data-keep-payables-scroll="1" href="/payables{payable_query_suffix(owner_chat_id, 'archive', counterparty_filter, sort_key, sort_order)}#payables-registry">
        Архив
        <span class="tab-count">{len(archived_entries)}</span>
      </a>
      <a class="tab-btn{" active" if active_tab == "deleted" else ""}" data-keep-payables-scroll="1" href="/payables{payable_query_suffix(owner_chat_id, 'deleted', counterparty_filter, sort_key, sort_order)}#payables-registry">
        Удаленные
        <span class="tab-count">{len(deleted_entries)}</span>
      </a>
    </div>
    """
    deleted_toolbar = ""
    if active_tab == "deleted" and has_active_admin_mode(current_user) and deleted_entries:
        deleted_count = len(deleted_entries)
        deleted_toolbar = f"""
        <div class="toolbar" style="margin-top:12px;">
          <form class="auction-delete-form" method="post" action="/payables/purge-deleted{payable_query_suffix(owner_chat_id, active_tab, counterparty_filter, sort_key, sort_order)}" onsubmit="return confirm('Вы точно хотите удалить {deleted_count} объектов навсегда?');">
            <button class="secondary-btn danger" type="submit">Очистить корзину</button>
          </form>
        </div>
        """
    stats = f"""
    <section class="stats">
      <article class="card stat-card">
        <div class="stat-label">Контрагентов</div>
        <div class="stat-value">{len({entry.counterparty for entry in active_entries})}</div>
        <div class="stat-note">Сейчас в работе</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Позиции</div>
        <div class="stat-value">{len(active_entries)}</div>
        <div class="stat-note">Незакрытых обязательств</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Общий долг</div>
        <div class="stat-value">{format_amount(total_outstanding)}</div>
        <div class="stat-note">Осталось оплатить подрядчикам</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Просрочено</div>
        <div class="stat-value">{overdue_count}</div>
        <div class="stat-note">Позиций с просроченным сроком оплаты</div>
      </article>
    </section>
    """
    return f"""
    {stats}
    <section class="card panel" id="payables-registry" style="margin-top:22px;">
      <div class="panel-head">
        <div>
          <h2 class="panel-title">Реестр кредиторки</h2>
          <div class="panel-sub">Кому должны, по какому документу, за какой объект и до какого срока нужно закрыть оплату.</div>
        </div>
      </div>
      {tab_links}
      {deleted_toolbar}
      <form class="action-row payables-filter-form" method="get" action="/payables" style="justify-content: space-between; align-items: end; margin-bottom: 14px;">
        <input type="hidden" name="owner" value="{owner_chat_id}">
        <input type="hidden" name="tab" value="{active_tab}">
        {f'<input type="hidden" name="sort" value="{escape(sort_key)}">' if sort_key and sort_order else ''}
        {f'<input type="hidden" name="order" value="{escape(sort_order)}">' if sort_key and sort_order else ''}
        <div class="field" style="min-width: 280px; margin:0;">
          <label>Выбрать контрагента</label>
          <select name="counterparty">
            <option value="">Все контрагенты</option>
            {"".join(f'<option value="{escape(name)}"{" selected" if name == counterparty_filter else ""}>{escape(name)}</option>' for name in available_counterparties)}
          </select>
        </div>
        <div class="action-row" style="gap:10px;">
          <button class="secondary-btn" type="submit">Показать</button>
          {f'<a class="secondary-btn" data-keep-payables-scroll="1" href="/payables{payable_query_suffix(owner_chat_id, active_tab, "", sort_key, sort_order)}#payables-registry">Сбросить фильтр</a>' if counterparty_filter else ""}
        </div>
      </form>
      {flash_html}
    </section>
    {register_html}
    {add_section}
    """


def render_payroll_section(storage: Storage, owner_chat_id: int, current_user: dict | None, selected_month: date | None = None, flash_message: str = "", success: bool = False) -> str:
    storage.ensure_payroll_seed(owner_chat_id)
    months = storage.list_payroll_months(owner_chat_id)
    if not months:
        months = [date.today().replace(day=1)]
    if selected_month is None or selected_month not in months:
        selected_month = months[0]
    rows = storage.list_payroll_rows(owner_chat_id, selected_month)
    total_accrued = sum(row.accrued_amount for row in rows)
    total_paid = sum(payroll_row_metrics(row)["paid_total"] for row in rows)
    total_debt = sum(payroll_row_metrics(row)["debt_amount"] for row in rows)
    closed_count = sum(1 for row in rows if payroll_row_metrics(row)["status_label"] == "Закрыто")
    month_tabs = "".join(
        f'<a class="tab-btn{" active" if month == selected_month else ""}" href="/payroll?owner={owner_chat_id}&month={month.strftime("%Y-%m")}">{escape(format_month_label(month))}</a>'
        for month in months
    )
    stats = f"""
    <section class="stats">
      <article class="card stat-card">
        <div class="stat-label">Сотрудников</div>
        <div class="stat-value">{len(rows)}</div>
        <div class="stat-note">В реестре зарплаты за месяц</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Начислено</div>
        <div class="stat-value">{format_amount(total_accrued)}</div>
        <div class="stat-note">Общая сумма начислений за месяц</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Выплачено</div>
        <div class="stat-value">{format_amount(total_paid)}</div>
        <div class="stat-note">Уже выдано сотрудникам</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Долг по зарплате</div>
        <div class="stat-value">{format_amount(total_debt)}</div>
        <div class="stat-note">Осталось закрыть по месяцу · закрыто {closed_count}</div>
      </article>
    </section>
    """
    add_employee_button = ""
    if has_permission(current_user, "payroll", "edit"):
        add_employee_button = f"""
        <details class="status-menu">
          <summary><span class="secondary-btn">Добавить сотрудника</span></summary>
          <div class="status-popover">
            <form class="form-grid" method="post" action="/payroll/employees/new?owner={owner_chat_id}&month={selected_month.strftime('%Y-%m')}">
              <div class="field">
                <label>ФИО сотрудника</label>
                <input type="text" name="full_name" placeholder="Иван Иванов" required>
              </div>
              <div class="field">
                <label>Должность</label>
                <input type="text" name="role_title" placeholder="Снабженец" required>
              </div>
              <button class="submit-btn" type="submit">Добавить в систему</button>
            </form>
          </div>
        </details>
        """
    body_rows = []
    for index, row in enumerate(rows, start=1):
        meta = payroll_row_metrics(row)
        balance_display = (
            f'<span class="payroll-balance danger">Переплата {format_amount(meta["overpaid_amount"])}</span>'
            if meta["overpaid_amount"] > 0.009
            else f'<span class="payroll-balance{" ok" if meta["debt_amount"] <= 0.009 else ""}">{format_amount(meta["debt_amount"])}</span>'
        )
        role_html = f'<div class="contract-table-subtle">{escape(row.role_title)}</div>' if row.role_title else ""
        body_rows.append(
            f"""
            <tr>
              <td class="nowrap" style="text-align:center;">{index}</td>
              <td>
                <div class="timeline-title">{escape(row.full_name)}</div>
                {role_html}
                {render_payroll_note_editor(owner_chat_id, selected_month, row, current_user)}
              </td>
              <td>{render_payroll_amount_editor(owner_chat_id, selected_month, row, "accrued_amount", "Начислено", row.accrued_amount, current_user)}</td>
              <td>{render_payroll_payment_editor(owner_chat_id, selected_month, row, "advance_card", "Аванс карта", row.advance_card_amount, row.advance_card_paid_amount, row.advance_card_paid_date, current_user)}</td>
              <td>{render_payroll_payment_editor(owner_chat_id, selected_month, row, "advance_cash", "Аванс кэш", row.advance_cash_amount, row.advance_cash_paid_amount, row.advance_cash_paid_date, current_user)}</td>
              <td>{render_payroll_payment_editor(owner_chat_id, selected_month, row, "salary", "Зарплата", row.salary_amount, row.salary_paid_amount, row.salary_paid_date, current_user)}</td>
              <td>{render_payroll_payment_editor(owner_chat_id, selected_month, row, "bonus", "Премия", row.bonus_amount, row.bonus_paid_amount, row.bonus_paid_date, current_user)}</td>
              <td class="nowrap">{format_amount(meta["paid_total"])}</td>
              <td class="nowrap">{balance_display}</td>
              <td><span class="{meta["status_class"]}">{meta["status_label"]}</span></td>
            </tr>
            """
        )
    rows_html = "".join(body_rows) or '<tr><td colspan="10">Сотрудников пока нет.</td></tr>'
    flash_html = f'<div class="flash{" ok" if success else ""}">{escape(flash_message)}</div>' if flash_message else ""
    return f"""
    {stats}
    <section class="card panel" style="margin-top:22px;">
      <div class="panel-head">
        <div>
          <h2 class="panel-title">Реестр зарплаты</h2>
          <div class="panel-sub">Начисления, выплаты и остатки по сотрудникам в разрезе месяцев.</div>
        </div>
        {add_employee_button}
      </div>
      <div class="tab-row">{month_tabs}</div>
      {flash_html}
      <table class="table contract-table payroll-table" style="margin-top: 18px;">
        <thead>
          <tr>
            <th class="nowrap">№</th>
            <th>Сотрудник</th>
            <th class="nowrap">Начислено</th>
            <th class="nowrap">Аванс карта{payroll_deadline_header(selected_month, "advance_card", rows)}</th>
            <th class="nowrap">Аванс кэш{payroll_deadline_header(selected_month, "advance_cash", rows)}</th>
            <th class="nowrap">ЗП{payroll_deadline_header(selected_month, "salary", rows)}</th>
            <th class="nowrap">Премия{payroll_deadline_header(selected_month, "bonus", rows)}</th>
            <th class="nowrap">Выплачено</th>
            <th class="nowrap">Остаток</th>
            <th class="nowrap">Статус</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </section>
    """


def render_access_section(
    storage: Storage,
    owner_chat_id: int,
    base_url: str,
    flash_message: str = "",
    success: bool = False,
) -> str:
    users = storage.list_web_users(owner_chat_id)
    base_setup_url = f"{base_url.rstrip('/')}/setup-password?token="
    stats = f"""
    <section class="stats">
      <article class="card stat-card">
        <div class="stat-label">Пользователей</div>
        <div class="stat-value">{len(users)}</div>
        <div class="stat-note">В web-контуре CRM</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Активных</div>
        <div class="stat-value">{sum(1 for user in users if user["is_active"])}</div>
        <div class="stat-note">С правом входа и просмотра</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Редакторов</div>
        <div class="stat-value">{sum(1 for user in users if any(item["can_edit"] for item in user["permissions"].values()))}</div>
        <div class="stat-note">Есть хотя бы один раздел с изменением</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Главный доступ</div>
        <div class="stat-value">Админ</div>
        <div class="stat-note">Полные права закреплены за вами</div>
      </article>
    </section>
    """

    user_cards = []
    for user in users:
        permissions = user["permissions"]
        permission_boxes = []
        for section_id, label in permission_sections():
            item = permissions[section_id]
            permission_boxes.append(
                f"""
                <div class="permission-box">
                  <strong>{escape(label)}</strong>
                  <div class="check-row">
                    <label>
                      <input type="checkbox" name="view_{section_id}" {"checked" if item["can_view"] else ""} {"disabled" if user["is_super_admin"] else ""}>
                      Просмотр
                    </label>
                    <label>
                      <input type="checkbox" name="edit_{section_id}" {"checked" if item["can_edit"] else ""} {"disabled" if user["is_super_admin"] else ""}>
                      Редактирование
                    </label>
                  </div>
                </div>
                """
            )
        status_badges = [
            '<span class="badge">Полный доступ</span>' if user["is_super_admin"] else f'<span class="badge">{escape(user["role_name"])}</span>',
            f'<span class="badge{" warn" if user["password_state"] == "pending_setup" else ""}">{escape(password_state_label(user["password_state"]))}</span>',
            f'<span class="badge{" danger" if not user["is_active"] else ""}">{"Активен" if user["is_active"] else "Отключен"}</span>',
        ]
        action_buttons = (
            '<div class="action-row"><span class="chip">Этот пользователь закреплен как главный администратор</span></div>'
            if user["is_super_admin"]
            else f"""
            <div class="action-row">
              <button class="submit-btn" type="submit">Сохранить права</button>
              <button class="secondary-btn" type="submit" formaction="/access/users/{user["id"]}/toggle?owner={owner_chat_id}" formmethod="post">{"Запретить доступ" if user["is_active"] else "Вернуть доступ"}</button>
              <button class="secondary-btn danger" type="submit" formaction="/access/users/{user["id"]}/delete?owner={owner_chat_id}" formmethod="post">Удалить пользователя</button>
            </div>
            """
        )
        setup_token = storage.ensure_password_setup_token(user["id"], secrets.token_urlsafe(24))
        setup_link = f"{base_setup_url}{setup_token}"
        reset_button = f"""
        <button class="secondary-btn" type="submit" formaction="/access/users/{user["id"]}/reset-password?owner={owner_chat_id}" formmethod="post">
          Сбросить пароль и обновить ссылку
        </button>
        """
        settings_block = f"""
        <details class="status-menu settings-menu">
          <summary><span class="settings-trigger">Настроить доступы и пароль</span></summary>
          <div class="settings-popover">
            <div class="permissions-grid">{''.join(permission_boxes)}</div>
            <div class="settings-divider"></div>
            <div class="action-row">{reset_button}</div>
            <div class="field">
              <label>Ссылка для установки пароля</label>
              <div class="copy-field">
                <input id="setup-link-{user["id"]}" type="text" value="{escape(setup_link)}" readonly>
                <button class="copy-btn" type="button" onclick="copyText('setup-link-{user["id"]}')" title="Скопировать ссылку">⧉</button>
              </div>
            </div>
          </div>
        </details>
        """
        user_cards.append(
            f"""
            <article class="user-card">
              <div class="user-head">
                <div>
                  <details class="status-menu user-name-menu">
                    <summary><span class="user-name-trigger">{escape(user["full_name"])}</span></summary>
                    <div class="name-popover">
                      <div class="field">
                        <label>Имя пользователя</label>
                        <input type="text" name="full_name_visible" value="{escape(user["full_name"])}" form="user-form-{user["id"]}" required>
                      </div>
                      <div class="action-row">
                        <button class="submit-btn" type="submit" form="user-form-{user["id"]}" formaction="/access/users/{user["id"]}/update?owner={owner_chat_id}" formmethod="post">Сохранить имя</button>
                      </div>
                    </div>
                  </details>
                  <div class="user-meta">
                    Логин: {escape(user["login"])}<br>
                    Создан: {escape(user["created_at"][:10])}<br>
                    Разделы: {escape(permission_summary(permissions))}
                  </div>
                </div>
                <div class="badge-row">{''.join(status_badges)}</div>
              </div>
              <form id="user-form-{user["id"]}" class="form-grid" method="post" action="/access/users/{user["id"]}/update?owner={owner_chat_id}">
                <input type="hidden" name="full_name" value="{escape(user["full_name"])}">
                <div class="field">
                  <label>Роль</label>
                  <input type="text" name="role_name" value="{escape(user["role_name"])}" {"readonly" if user["is_super_admin"] else ""}>
                </div>
                {settings_block}
                {action_buttons}
              </form>
            </article>
            """
        )

    flash_html = ""
    if flash_message:
        flash_html = f'<div class="flash{" ok" if success else ""}">{escape(flash_message)}</div>'

    return f"""
    {stats}
    {flash_html}
    <section class="grid">
      <section class="card panel">
        <div class="panel-head">
          <div>
            <h2 class="panel-title">Пользователи и права</h2>
            <div class="panel-sub">Дальше этот раздел станет центром ролей, приглашений и управления доступом к модулям CRM.</div>
          </div>
          <div class="chip">Владелец контура: {owner_chat_id}</div>
        </div>
        <div class="access-stack">{''.join(user_cards)}</div>
      </section>
      <aside class="section-stack">
        <section class="card panel">
          <div class="panel-head">
            <div>
              <h2 class="panel-title">Добавить пользователя</h2>
              <div class="panel-sub">Администратор создает пользователя по логину, а затем передает ему отдельную ссылку на установку пароля.</div>
            </div>
          </div>
          <form class="form-grid" method="post" action="/access/users/new?owner={owner_chat_id}">
            <div class="field">
              <label>Имя пользователя</label>
              <input type="text" name="full_name" placeholder="Например, Илья Петров" required>
            </div>
            <div class="field">
              <label>Логин</label>
              <input type="text" name="login" placeholder="ivan.petrov" required>
            </div>
            <div class="field">
              <label>Роль</label>
              <input type="text" name="role_name" value="Менеджер проектов">
            </div>
            <div class="permissions-grid">
              {''.join(
                  f'''
                  <div class="permission-box">
                    <strong>{escape(label)}</strong>
                    <div class="check-row">
                      <label><input type="checkbox" name="view_{section_id}"> Просмотр</label>
                      <label><input type="checkbox" name="edit_{section_id}"> Редактирование</label>
                    </div>
                  </div>
                  '''
                  for section_id, label in permission_sections()
              )}
            </div>
            <button class="submit-btn" type="submit">Добавить пользователя</button>
          </form>
        </section>
        <section class="card panel">
          <div class="panel-head">
            <div>
              <h2 class="panel-title">Как это будет работать дальше</h2>
              <div class="panel-sub">Логика уже закладывается правильно</div>
            </div>
          </div>
        <div class="contract-meta">
          • Вы остаетесь главным администратором системы<br>
          • Пользователю задаем отдельный логин для входа<br>
          • По каждому разделу отдельно отмечаем просмотр и редактирование<br>
          • После создания система дает ссылку на установку пароля<br>
          • Администратор может в любой момент перевыпустить ссылку на пароль<br>
          • Доступ можно отключить, вернуть или полностью удалить
        </div>
      </section>
      </aside>
    </section>
    """


def render_auctions_section(
    storage: Storage,
    owner_chat_id: int,
    current_user: dict | None,
    active_tab: str = "active",
    flash_message: str = "",
    success: bool = False,
    task_view: bool = False,
) -> str:
    storage.ensure_demo_auctions(owner_chat_id)
    all_auctions = storage.list_auctions(owner_chat_id)
    active_auctions = [item for item in all_auctions if not is_auction_archived(item) and not is_auction_deleted(item)]
    archived_auctions = [item for item in all_auctions if is_auction_archived(item) and not is_auction_deleted(item)]
    deleted_auctions = [item for item in all_auctions if is_auction_deleted(item)]
    active_auctions.sort(key=lambda item: (item.bid_deadline, item.id))
    archived_auctions.sort(
        key=lambda item: (
            item.archived_at or item.created_at,
            item.id,
        ),
        reverse=True,
    )
    deleted_auctions.sort(
        key=lambda item: (
            item.deleted_at or item.created_at,
            item.id,
        ),
        reverse=True,
    )
    role_tasks = []
    if is_procurement_user(current_user):
        role_tasks = [item for item in active_auctions if item.submit_decision_status == "approved"]
        role_tasks.sort(key=lambda item: (item.bid_deadline, item.id))
    elif is_supply_user(current_user):
        role_tasks = [item for item in active_auctions if item.estimate_status == "approved"]
        role_tasks.sort(key=lambda item: (item.bid_deadline, item.id))
    elif has_permission(current_user, "auctions", "edit"):
        decision_tasks = [item for item in active_auctions if item.submit_decision_status == "pending"]
        discount_tasks = [
            item
            for item in active_auctions
            if item.submit_decision_status == "approved" and item.max_discount_percent is None
        ]
        decision_tasks.sort(key=lambda item: (item.bid_deadline, item.id))
        discount_tasks.sort(key=lambda item: (item.bid_deadline, item.id))
        role_tasks = decision_tasks + discount_tasks
    if task_view and active_tab == "active":
        auctions = role_tasks
    elif active_tab == "archive":
        auctions = archived_auctions
    elif active_tab == "deleted":
        auctions = deleted_auctions
    else:
        auctions = active_auctions
    total_amount = sum(item.amount for item in active_auctions)
    estimate_count = sum(1 for item in active_auctions if item.estimate_status == "approved")
    submit_decision_count = sum(1 for item in active_auctions if item.submit_decision_status == "approved")
    submitted_count = sum(1 for item in all_auctions if item.submit_decision_status == "submitted")
    won_count = sum(1 for item in all_auctions if item.result_status in {"won", "recognized_winner"})

    stats = f"""
    <section class="stats">
      <article class="card stat-card">
        <div class="stat-label">Аукционов</div>
        <div class="stat-value">{len(active_auctions)}</div>
        <div class="stat-note">Сейчас в работе</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Общий объем</div>
        <div class="stat-value">{format_amount(total_amount)}</div>
        <div class="stat-note">Сумма аукционов, которые сейчас в работе</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Подано / выиграно</div>
        <div class="stat-value">{submitted_count} / {won_count}</div>
        <div class="stat-note">Поданные заявки и уже выигранные аукционы</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Считаем / подаем</div>
        <div class="stat-value">{estimate_count} / {submit_decision_count}</div>
        <div class="stat-note">Решения руководства по рабочим аукционам</div>
      </article>
    </section>
    """

    rows_html = render_auction_rows(auctions, owner_chat_id, active_tab, current_user)
    flash_html = f'<div class="flash{" ok" if success else ""}">{escape(flash_message)}</div>' if flash_message else ""
    active_href = f"/auctions?owner={owner_chat_id}&tab=active{'&task_view=1' if task_view and active_tab == 'active' else ''}#auction-registry"
    tab_links = f"""
    <div class="tab-row">
      <a class="tab-btn{" active" if active_tab == "active" else ""}" href="{active_href}">
        В работе
        <span class="tab-count">{len(active_auctions)}</span>
      </a>
      <a class="tab-btn{" active" if active_tab == "archive" else ""}" href="/auctions?owner={owner_chat_id}&tab=archive#auction-registry">
        Архив
        <span class="tab-count">{len(archived_auctions)}</span>
      </a>
      <a class="tab-btn{" active" if active_tab == "deleted" else ""}" href="/auctions?owner={owner_chat_id}&tab=deleted#auction-registry">
        Удаленные
        <span class="tab-count">{len(deleted_auctions)}</span>
      </a>
    </div>
    """
    empty_message = (
        "Сейчас у этой роли нет активных задач."
        if active_tab == "active" and task_view
        else "Сейчас в работе нет аукционов. Все завершенные или отклоненные лоты уже ушли в архив."
        if active_tab == "active"
        else "Архив пока пуст. Как только по аукциону будет итог или решение не подавать, он окажется здесь."
        if active_tab == "archive"
        else "Удаленных аукционов пока нет."
    )
    task_toolbar = ""
    if task_view and active_tab == "active":
        task_label = (
            "Задачи отдела госзакупок"
            if is_procurement_user(current_user)
            else "Задачи отдела снабжения"
            if is_supply_user(current_user)
            else "Задачи руководства"
        )
        task_toolbar = f"""
        <div class="action-row" style="justify-content: space-between; margin-bottom: 16px;">
          <div class="chip warn">{task_label}</div>
          <a class="secondary-btn" href="/auctions?owner={owner_chat_id}&tab=active#auction-registry">Показать весь реестр</a>
        </div>
        """
    deleted_toolbar = ""
    if active_tab == "deleted" and has_active_admin_mode(current_user) and deleted_auctions:
        deleted_count = len(deleted_auctions)
        deleted_toolbar = f"""
        <div class="action-row" style="justify-content: flex-end; margin-bottom: 16px;">
          <form class="auction-delete-form" method="post" action="/auctions/purge-deleted?owner={owner_chat_id}&tab=deleted" onsubmit="return confirm('Вы точно хотите удалить {deleted_count} объектов навсегда?');">
            <button class="secondary-btn danger" type="submit">Очистить корзину</button>
          </form>
        </div>
        """
    add_auction_block = f"""
    <section class="card panel">
      <div class="panel-head">
        <div>
          <h2 class="panel-title">Добавить аукцион</h2>
          <div class="panel-sub">Первичная карточка лота, которую может занести любой участник команды.</div>
        </div>
        <div class="chip">Новая запись попадает в общий реестр</div>
      </div>
      <form class="form-grid" method="post" action="/auctions/new?owner={owner_chat_id}&tab={active_tab}">
        <div class="stats" style="grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));">
          <div class="field">
            <label>Номер аукциона</label>
            <input type="text" name="auction_number" placeholder="0145300000126000011" inputmode="numeric" pattern="[0-9]+" required>
          </div>
          <div class="field">
            <label>Название аукциона</label>
            <input type="text" name="title" placeholder="Капремонт школы №8" required>
          </div>
          <div class="field">
            <label>Город</label>
            <input type="text" name="city" placeholder="Владивосток" required>
          </div>
          <div class="field">
            <label>Подать до</label>
            <input type="text" name="bid_deadline" placeholder="31-03-2026" required>
          </div>
          <div class="field">
            <label>Сумма лота</label>
            <input type="text" name="amount" placeholder="25000000" required>
          </div>
          <label class="advance-toggle">
            <input class="toggle-checkbox" type="checkbox" name="has_advance" value="1"> У аукциона есть аванс
          </label>
          <div class="field advance-field is-hidden">
            <label>Процент аванса</label>
            <input type="text" name="advance_percent" placeholder="Например, 30">
          </div>
          <div class="field">
            <label>Ссылка</label>
            <input type="text" name="source_url" placeholder="https://zakupki.gov.ru/...">
          </div>
        </div>
        <div class="action-row">
          <button class="submit-btn" type="submit">Добавить аукцион</button>
        </div>
      </form>
    </section>
    """
    return f"""
    {stats}
    {flash_html}
    <section class="card panel" id="auction-registry">
      <div class="panel-head">
        <div>
          <h2 class="panel-title">Реестр аукционов</h2>
          <div class="panel-sub">Любой сотрудник добавляет лот, руководство акцептует его как «считать / подавать», затем фиксируем подачу и итог. Завершенные и неактуальные лоты уходят в архив.</div>
        </div>
        {tab_links}
      </div>
      {task_toolbar}
      {deleted_toolbar}
      {
        f'''
      <table class="table auction-table">
        <thead>
          <tr>
            <th>Лот</th>
            <th class="nowrap">Подача до</th>
            <th class="nowrap">Сумма</th>
            <th>Считать</th>
            <th>Заявка</th>
            <th>Макс. снижение</th>
            <th>Итог</th>
          </tr>
        </thead>
        <tbody data-auctions-body>{rows_html}</tbody>
      </table>
      '''
        if auctions
        else f'<div class="empty">{escape(empty_message)}</div>'
      }
    </section>
    {add_auction_block}
    <section class="stats">
      <section class="card panel">
        <div class="panel-head">
          <div>
            <h2 class="panel-title">Логика статусов</h2>
            <div class="panel-sub">Именно так сейчас лучше собирать цепочку.</div>
          </div>
        </div>
        <div class="contract-meta">
          • Считать: отдельное решение руководства, нужно ли вообще считать смету по лоту<br>
          • Заявка: теперь это одна колонка, где желтый статус означает «подавать заявку», а зеленый — «заявка подана»<br>
          • Макс. снижение: управленческий предел по проценту и минимальной сумме, ниже которой не идем<br>
          • Пока заявка не подана, итог автоматически остается в логике «Не участвовали»<br>
          • После статуса «Заявка подана» доступны варианты: «Ждем розыгрыш», «Выигран», «Признан победителем», «Проигран», «Заявка отклонена»
        </div>
      </section>
      <section class="card panel">
        <div class="panel-head">
          <div>
            <h2 class="panel-title">Следующий шаг</h2>
            <div class="panel-sub">Что логично добавить в следующей итерации</div>
          </div>
        </div>
        <div class="contract-meta">
          • Inline-переключение статусов прямо в таблице<br>
          • Ответственный сотрудник по каждому аукциону<br>
          • Комментарии и история работы по лоту<br>
          • Просроченные дедлайны подачи отдельным списком
        </div>
      </section>
    </section>
    """


def auctions_for_current_view(storage: Storage, owner_chat_id: int, current_user: dict | None, active_tab: str, task_view: bool) -> list:
    auctions = storage.list_auctions(owner_chat_id)
    active_auctions = [item for item in auctions if not is_auction_archived(item) and not is_auction_deleted(item)]
    archived_auctions = [item for item in auctions if is_auction_archived(item) and not is_auction_deleted(item)]
    deleted_auctions = [item for item in auctions if is_auction_deleted(item)]
    active_auctions.sort(key=lambda item: (item.bid_deadline, item.id))
    archived_auctions.sort(key=lambda item: (item.archived_at or item.created_at, item.id), reverse=True)
    deleted_auctions.sort(key=lambda item: (item.deleted_at or item.created_at, item.id), reverse=True)
    if task_view and active_tab == "active":
        if is_procurement_user(current_user):
            return [item for item in active_auctions if item.submit_decision_status == "approved"]
        if is_supply_user(current_user):
            return [item for item in active_auctions if item.estimate_status == "approved"]
        if has_permission(current_user, "auctions", "edit"):
            decision_tasks = [item for item in active_auctions if item.submit_decision_status == "pending"]
            discount_tasks = [
                item
                for item in active_auctions
                if item.submit_decision_status == "approved" and item.max_discount_percent is None
            ]
            decision_tasks.sort(key=lambda item: (item.bid_deadline, item.id))
            discount_tasks.sort(key=lambda item: (item.bid_deadline, item.id))
            return decision_tasks + discount_tasks
    if active_tab == "archive":
        return archived_auctions
    if active_tab == "deleted":
        return deleted_auctions
    return active_auctions


def render_placeholder_section(title: str, subtitle: str, bullets: list[str]) -> str:
    bullet_html = "".join(f"<li>{escape(item)}</li>" for item in bullets)
    return f"""
    <section class="card panel">
      <div class="panel-head">
        <div>
          <h2 class="panel-title">Что сюда логично включить</h2>
          <div class="panel-sub">{escape(subtitle)}</div>
        </div>
      </div>
      <div class="contract-meta">
        <ul style="margin:0; padding-left:18px; line-height:1.8;">{bullet_html}</ul>
      </div>
    </section>
    """


def read_post_data(environ) -> dict[str, str]:
    try:
        length = int(environ.get("CONTENT_LENGTH", "0") or "0")
    except ValueError:
        length = 0
    raw = environ["wsgi.input"].read(length).decode("utf-8")
    parsed = parse_qs(raw)
    return {key: values[0] for key, values in parsed.items()}


def redirect(start_response, location: str):
    start_response("303 See Other", [("Location", location)])
    return [b""]


def redirect_with_cookie(start_response, location: str, cookie_value: str | None = None):
    headers = [("Location", location)]
    if cookie_value is not None:
        headers.append(("Set-Cookie", f"{SESSION_COOKIE}={cookie_value}; Path=/; HttpOnly; SameSite=Lax"))
    start_response("303 See Other", headers)
    return [b""]


def redirect_with_cookies(start_response, location: str, cookies: list[str]):
    headers = [("Location", location)]
    for cookie in cookies:
        headers.append(("Set-Cookie", cookie))
    start_response("303 See Other", headers)
    return [b""]


def clear_session_cookie(start_response, location: str):
    start_response(
        "303 See Other",
        [
            ("Location", location),
            ("Set-Cookie", f"{SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"),
        ],
    )
    return [b""]


def has_permission(current_user: dict | None, section_id: str, mode: str = "view") -> bool:
    if current_user is None:
        return False
    if current_user.get("is_super_admin") and not current_user.get("preview_role_name"):
        return True
    permissions_source = current_user.get("preview_permissions") if current_user.get("preview_role_name") else current_user.get("permissions", {})
    permissions = permissions_source.get(section_id, {}) if permissions_source else {}
    return bool(permissions.get("can_edit" if mode == "edit" else "can_view"))


def has_active_admin_mode(current_user: dict | None) -> bool:
    return bool(
        current_user
        and current_user.get("is_super_admin")
        and not current_user.get("preview_role_name")
    )


def render_auth_body(storage: Storage, flash_message: str = "", setup_message: str = "", login_hint: str = "") -> str:
    hint_html = ""
    if login_hint:
        hint_html = (
            f"<div class=\"auth-note\">Ваш логин для входа: <strong>{escape(login_hint)}</strong></div>"
        )
    flash_html = f'<div class="flash">{escape(flash_message)}</div>' if flash_message else ""
    setup_html = f'<div class="flash ok">{escape(setup_message)}</div>' if setup_message else ""
    return f"""
    <div class="auth-wrap">
      <section class="auth-card">
        <div>
          <h2 class="panel-title">Авторизируйтесь, чтобы видеть данные</h2>
          <div class="panel-sub">Без входа разделы CRM не показывают рабочие данные. После авторизации откроются только те модули, на которые у пользователя есть права.</div>
        </div>
        {flash_html}
        {setup_html}
        {hint_html}
        <form class="form-grid" method="post" action="/login">
          <div class="field">
            <label>Логин</label>
            <input type="text" name="login" placeholder="Введите ваш логин" value="{escape(login_hint)}" required>
          </div>
          <div class="field">
            <label>Пароль</label>
            <input type="password" name="password" placeholder="Введите пароль" required>
          </div>
          <button class="submit-btn" type="submit">Войти</button>
        </form>
        <div class="auth-note">Пользователя создает администратор. После этого он отправляет сотруднику отдельную ссылку на установку пароля.</div>
      </section>
    </div>
    """


def render_password_setup_body(user: dict, token: str, flash_message: str = "") -> str:
    flash_html = f'<div class="flash">{escape(flash_message)}</div>' if flash_message else ""
    return f"""
    <div class="auth-wrap">
      <section class="auth-card">
        <div>
          <h2 class="panel-title">Создайте пароль</h2>
          <div class="panel-sub">Пользователь: {escape(user["full_name"])}<br>Логин: {escape(user["login"])}</div>
        </div>
        {flash_html}
        <form class="form-grid" method="post" action="/setup-password">
          <input type="hidden" name="token" value="{escape(token)}">
          <div class="field">
            <label>Новый пароль</label>
            <input type="password" name="password" placeholder="Придумайте пароль" required>
          </div>
          <div class="field">
            <label>Повторите пароль</label>
            <input type="password" name="password_confirm" placeholder="Еще раз" required>
          </div>
          <button class="submit-btn" type="submit">Сохранить пароль</button>
        </form>
      </section>
    </div>
    """


def render_forbidden_body(section_label: str) -> str:
    return f"""
    <div class="auth-wrap">
      <section class="auth-card">
        <div>
          <h2 class="panel-title">Недостаточно прав</h2>
          <div class="panel-sub">У текущего пользователя нет доступа к разделу «{escape(section_label)}».</div>
        </div>
        <div class="auth-note">Если доступ нужен, главный администратор должен открыть просмотр или редактирование в разделе «Доступы».</div>
      </section>
    </div>
    """


def app(environ, start_response):
    storage = Storage(os.getenv("DB_PATH", "contracts.db"))
    query = parse_qs(environ.get("QUERY_STRING", ""))
    current_auction_tab = query.get("tab", ["active"])[0]
    if current_auction_tab not in {"active", "archive", "deleted"}:
        current_auction_tab = "active"
    current_payables_tab = query.get("tab", ["active"])[0]
    if current_payables_tab not in {"active", "archive", "deleted"}:
        current_payables_tab = "active"
    current_payables_counterparty = query.get("counterparty", [""])[0].strip()
    current_payables_sort, current_payables_order = normalize_payables_sort(
        query.get("sort", [""])[0].strip(),
        query.get("order", [""])[0].strip(),
    )
    current_task_view = query.get("task_view", ["0"])[0] == "1"
    cookies = parse_cookies(environ)
    current_user = storage.get_web_user_by_session(cookies.get(SESSION_COOKIE, ""))
    preview_role = cookies.get(PREVIEW_ROLE_COOKIE, "") if current_user and current_user.get("is_super_admin") else ""
    current_owner = current_user["owner_chat_id"] if current_user else None
    current_preview_options = preview_role_options(storage, current_owner, current_user)
    valid_preview_roles = {item[0] for item in current_preview_options}
    if preview_role == "Отдел госзакупок":
        preview_role = "procurement"
    if preview_role == "Отдел снабжения":
        preview_role = "supply"
    if preview_role == "Руководство компании":
        preview_role = "management"
    if preview_role not in valid_preview_roles:
        preview_role = ""
    if current_user is not None:
        current_user["preview_role_options"] = current_preview_options
    current_user = with_preview_role(current_user, preview_role)
    if current_user is not None:
        current_user["preview_permissions"] = resolve_preview_permissions(storage, current_owner, preview_role)
    current_owner = current_user["owner_chat_id"] if current_user else None
    if current_user is not None:
        current_user["role_notifications"] = compute_role_notifications(storage, current_owner, current_user)
    owners = owner_options(storage)

    path = environ.get("PATH_INFO", "/")
    method = environ.get("REQUEST_METHOD", "GET").upper()

    if path == "/login" and method == "POST":
        form = read_post_data(environ)
        user = storage.get_web_user_by_login(form.get("login", ""))
        if user is None or not user["is_active"] or not verify_password(form.get("password", ""), user.get("password_hash", "")):
            body = render_auth_body(storage, "Неверный логин или пароль.")
            html = layout("Авторизация", body, owners, current_owner, "contracts", None)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        token = secrets.token_urlsafe(32)
        storage.create_web_session(user["id"], token)
        return redirect_with_cookie(start_response, "/contracts", token)

    if path == "/setup-password" and method == "POST":
        form = read_post_data(environ)
        token = form.get("token", "")
        password = form.get("password", "")
        confirm = form.get("password_confirm", "")
        user = storage.get_web_user_by_setup_token(token)
        if user is None or not user["is_active"]:
            body = render_auth_body(storage, "Ссылка установки пароля недействительна.")
            html = layout("Авторизация", body, owners, current_owner, "contracts", None)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        if password != confirm:
            body = render_password_setup_body(user, token, "Пароли не совпадают.")
            html = layout("Авторизация", body, owners, current_owner, "contracts", None)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        if len(password) < 6:
            body = render_password_setup_body(user, token, "Пароль должен быть не короче 6 символов.")
            html = layout("Авторизация", body, owners, current_owner, "contracts", None)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        storage.set_web_user_password(user["id"], hash_password(password))
        storage.consume_password_setup_token(token)
        body = render_auth_body(storage, "", "Пароль создан. Теперь можно войти.", user["login"])
        html = layout("Авторизация", body, owners, current_owner, "contracts", None)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path == "/setup-password" and method == "GET":
        token = query.get("token", [""])[0]
        user = storage.get_web_user_by_setup_token(token)
        if user is None or not user["is_active"]:
            body = render_auth_body(storage, "Ссылка установки пароля недействительна.")
            html = layout("Авторизация", body, owners, current_owner, "contracts", None)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        body = render_password_setup_body(user, token)
        html = layout("Создание пароля", body, owners, current_owner, "contracts", None)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path == "/logout":
        if cookies.get(SESSION_COOKIE):
            storage.delete_web_session(cookies.get(SESSION_COOKIE, ""))
        return redirect_with_cookies(
            start_response,
            "/contracts",
            [
                f"{SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax",
                f"{PREVIEW_ROLE_COOKIE}=; Path=/; Max-Age=0; SameSite=Lax",
            ],
        )

    if path == "/role-preview" and method == "POST":
        if current_user is None or not current_user.get("is_super_admin"):
            return redirect(start_response, "/contracts")
        form = read_post_data(environ)
        next_path = form.get("next_path", "").strip() or environ.get("HTTP_REFERER", "") or "/contracts"
        preview_role = form.get("preview_role", "")
        if preview_role not in {item[0] for item in current_user.get("preview_role_options", ROLE_PREVIEW_OPTIONS)}:
            preview_role = ""
        cookie = f"{PREVIEW_ROLE_COOKIE}={preview_role}; Path=/; SameSite=Lax"
        if not preview_role:
            cookie = f"{PREVIEW_ROLE_COOKIE}=; Path=/; Max-Age=0; SameSite=Lax"
        return redirect_with_cookies(start_response, next_path, [cookie])

    if current_user is None:
        body = render_auth_body(storage)
        html = layout("Авторизация", body, owners, current_owner, "contracts", None)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    storage.ensure_default_web_admin(current_owner)

    def guard(section_id: str, mode: str = "view"):
        if has_permission(current_user, section_id, mode):
            return None
        body = render_forbidden_body(SECTION_LABELS.get(section_id, section_id))
        html = layout("Доступ запрещен", body, owners, current_owner, section_id, current_user)
        start_response("403 Forbidden", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path == "/contracts/new" and method == "POST":
        denied = guard("contracts", "edit")
        if denied:
            return denied
        form = read_post_data(environ)
        try:
            title = form["title"].strip()
            description = form.get("description", "").strip()
            total_amount = parse_amount(form["total_amount"])
            signed_date = parse_date(form["signed_date"])
            stage_count = int(form.get("stage_count", "1"))
            has_advance = form.get("has_advance") == "1"
            advance_percent = parse_optional_number(form.get("advance_percent", "")) if has_advance else None
            if not title:
                raise ValueError("Название контракта обязательно")
            if total_amount <= 0:
                raise ValueError("Общая сумма контракта должна быть больше 0")
            if stage_count < 1:
                raise ValueError("Нужно указать хотя бы один этап")
            if has_advance:
                if advance_percent is None:
                    raise ValueError("Укажите процент аванса")
                if advance_percent <= 0 or advance_percent > 100:
                    raise ValueError("Процент аванса должен быть от 0,01 до 100")
            stage_specs = []
            stage_total = 0.0
            for index in range(1, stage_count + 1):
                amount = parse_amount(form.get(f"stage_amount_{index}", ""))
                if amount <= 0:
                    raise ValueError(f"Сумма этапа {index} должна быть больше 0")
                start_date_raw = form.get(f"stage_start_date_{index}", "")
                start_date = parse_date(start_date_raw) if start_date_raw else None
                end_date = parse_date(form.get(f"stage_end_date_{index}", ""))
                if start_date is not None and start_date > end_date:
                    raise ValueError(f"Старт работ этапа {index} не может быть позже дедлайна")
                notes = form.get(f"stage_notes_{index}", "").strip()
                stage_specs.append((index, amount, start_date, end_date, notes))
                stage_total += amount
            if abs(stage_total - total_amount) > 0.01:
                raise ValueError("Сумма этапов должна совпадать с общей суммой контракта")
            contract_end_date = max(item[3] for item in stage_specs)
            contract_id = storage.add_contract(current_owner, title, description, signed_date, contract_end_date, advance_percent)
            for index, amount, start_date, end_date, notes in stage_specs:
                storage.add_stage(contract_id, index, notes, start_date, end_date, amount)
            return redirect(start_response, f"/contracts/{contract_id}?owner={current_owner}")
        except Exception as exc:
            body = render_dashboard(storage, current_owner) + f'<div class="flash">Не удалось создать контракт: {escape(str(exc))}</div>'
            html = layout("Контракты", body, owners, current_owner, "contracts", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path in ("/", "/contracts"):
        denied = guard("contracts", "view")
        if denied:
            return denied
        body = render_dashboard(storage, current_owner)
        html = layout("Контракты", body, owners, current_owner, "contracts", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path == "/auctions/new" and method == "POST":
        denied = guard("auctions", "view")
        if denied:
            return denied
        form = read_post_data(environ)
        try:
            auction_number = validate_auction_number(form["auction_number"])
            title = form["title"].strip()
            city = form["city"].strip()
            bid_deadline = parse_date(form["bid_deadline"])
            amount = parse_amount(form["amount"])
            source_url = form.get("source_url", "").strip()
            has_advance = form.get("has_advance") == "1"
            advance_percent = parse_optional_number(form.get("advance_percent", "")) if has_advance else None
            if not title:
                raise ValueError("Нужно указать название аукциона")
            if not city:
                raise ValueError("Нужно указать город")
            if has_advance:
                if advance_percent is None:
                    raise ValueError("Укажите процент авансирования")
                if advance_percent <= 0 or advance_percent > 100:
                    raise ValueError("Процент авансирования должен быть от 0,01 до 100")
            storage.add_auction(
                current_owner,
                auction_number,
                bid_deadline,
                amount,
                advance_percent,
                title,
                city,
                source_url,
                current_user.get("id") if current_user else None,
                current_user.get("full_name", "") if current_user else "",
            )
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, "Аукцион добавлен в реестр.", True)
        except Exception as exc:
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, f"Не удалось добавить аукцион: {exc}")
        html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path.startswith("/auctions/") and path.endswith("/status") and method == "POST":
        denied = guard("auctions", "edit")
        if denied:
            return denied
        form = read_post_data(environ)
        try:
            auction_id = int(path.split("/")[2])
            auction = next((item for item in storage.list_auctions(current_owner) if item.id == auction_id), None)
            if auction is None:
                raise ValueError("Аукцион не найден")
            estimate_status = form.get("estimate_status", auction.estimate_status)
            material_cost = auction.material_cost
            work_cost = auction.work_cost
            other_cost = auction.other_cost
            estimate_comment = auction.estimate_comment
            estimate_status_updated_at = auction.estimate_status_updated_at
            estimate_status_updated_by_name = auction.estimate_status_updated_by_name
            submit_decision_status = form.get("submit_decision_status", auction.submit_decision_status)
            submit_status_updated_at = auction.submit_status_updated_at
            submit_status_updated_by_name = auction.submit_status_updated_by_name
            result_status = form.get("result_status", auction.result_status)
            result_status_updated_at = auction.result_status_updated_at
            result_status_updated_by_name = auction.result_status_updated_by_name
            final_bid_amount = auction.final_bid_amount
            actor_name = current_user.get("full_name", "").strip() if current_user else ""
            if not procurement_status_allowed(current_user, auction, estimate_status, submit_decision_status, result_status):
                raise ValueError("Для роли «Отдел госзакупок» доступны только перевод «Подавать заявку» -> «Заявка подана» и изменение итога")
            if not supply_status_allowed(current_user, auction, estimate_status, submit_decision_status, result_status):
                raise ValueError("Для роли «Отдел снабжения» доступна только колонка «Считать»")
            if not management_status_allowed(current_user, auction, estimate_status, submit_decision_status, result_status):
                raise ValueError("Для роли «Руководство компании» доступны только решения в колонках «Считать» и «Заявка»")
            if estimate_status == "calculated":
                material_cost = None if form.get("skip_material_cost") == "1" else parse_optional_number(form.get("material_cost", ""))
                work_cost = None if form.get("skip_work_cost") == "1" else parse_optional_number(form.get("work_cost", ""))
                other_cost = None if form.get("skip_other_cost") == "1" else parse_optional_number(form.get("other_cost", ""))
                if material_cost is not None and material_cost <= 0:
                    raise ValueError("Стоимость материалов должна быть больше 0")
                if work_cost is not None and work_cost <= 0:
                    raise ValueError("Стоимость работ должна быть больше 0")
                if other_cost is not None and other_cost <= 0:
                    raise ValueError("Прочие расходы должны быть больше 0")
                if all(value is None for value in (material_cost, work_cost, other_cost)):
                    raise ValueError("Заполните хотя бы одно поле расчета или выберите статус «Не просчитан»")
                estimate_comment = form.get("estimate_comment", "").strip()
                if (
                    auction.estimate_status != "calculated"
                    or auction.material_cost != material_cost
                    or auction.work_cost != work_cost
                    or auction.other_cost != other_cost
                    or auction.estimate_comment != estimate_comment
                ):
                    estimate_status_updated_at = datetime.utcnow()
                    estimate_status_updated_by_name = actor_name
            else:
                material_cost = None
                work_cost = None
                other_cost = None
                estimate_comment = ""
                if estimate_status != auction.estimate_status:
                    estimate_status_updated_at = datetime.utcnow()
                    estimate_status_updated_by_name = actor_name
            if submit_decision_status != auction.submit_decision_status:
                submit_status_updated_at = datetime.utcnow()
                submit_status_updated_by_name = actor_name
            application_status = "submitted" if submit_decision_status == "submitted" else "not_submitted"
            if submit_decision_status != "submitted":
                result_status = "not_participated"
                result_status_updated_at = None
                result_status_updated_by_name = ""
                final_bid_amount = None
            elif result_status == "not_participated":
                result_status = "pending"
                result_status_updated_at = datetime.utcnow()
                result_status_updated_by_name = "Система"
                final_bid_amount = None
            elif result_status in {"won", "lost"}:
                final_bid_amount = parse_optional_number(form.get("final_bid_amount", ""))
                if final_bid_amount is None:
                    raise ValueError("Укажите сумму, за которую выиграли аукцион")
                if final_bid_amount <= 0 or final_bid_amount > auction.amount:
                    raise ValueError("Итоговая цена должна быть больше 0 и не превышать сумму аукциона")
                if result_status != auction.result_status or final_bid_amount != auction.final_bid_amount:
                    result_status_updated_at = datetime.utcnow()
                    result_status_updated_by_name = actor_name
            elif result_status == "recognized_winner":
                if auction.final_bid_amount is None:
                    raise ValueError("Сначала сохраните статус «Выигран» с ценой аукциона")
                final_bid_amount = auction.final_bid_amount
                if result_status != auction.result_status:
                    result_status_updated_at = datetime.utcnow()
                    result_status_updated_by_name = actor_name
            else:
                if result_status != auction.result_status:
                    result_status_updated_at = datetime.utcnow()
                    result_status_updated_by_name = actor_name
                final_bid_amount = None
            was_archived = is_auction_archived(auction)
            will_be_archived = (
                submit_decision_status == "rejected"
                or result_status in {"recognized_winner", "lost", "rejected"}
            )
            if will_be_archived and not was_archived:
                archived_at = datetime.now()
            elif will_be_archived:
                archived_at = auction.archived_at or datetime.now()
            else:
                archived_at = None
            updated = storage.update_auction_statuses(
                current_owner,
                auction_id,
                estimate_status,
                material_cost,
                work_cost,
                other_cost,
                estimate_comment,
                estimate_status_updated_at,
                estimate_status_updated_by_name,
                submit_decision_status,
                submit_status_updated_at,
                submit_status_updated_by_name,
                application_status,
                result_status,
                result_status_updated_at,
                result_status_updated_by_name,
                final_bid_amount,
                archived_at,
            )
            if not updated:
                raise ValueError("Аукцион не найден")
            updated_auction = next((item for item in storage.list_auctions(current_owner) if item.id == auction_id), None)
            if updated_auction is None:
                raise ValueError("Аукцион не найден")
            target_tab = "archive" if is_auction_archived(updated_auction) else "active"
            return redirect(start_response, f"/auctions?owner={current_owner}&tab={target_tab}")
        except Exception as exc:
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, f"Не удалось обновить статус аукциона: {exc}")
        html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path.startswith("/auctions/") and path.endswith("/discount") and method == "POST":
        denied = guard("auctions", "edit")
        if denied:
            return denied
        if is_procurement_user(current_user) or is_supply_user(current_user):
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, "Эта роль не меняет максимальное снижение.")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        form = read_post_data(environ)
        try:
            auction_id = int(path.split("/")[2])
            auction = next((item for item in storage.list_auctions(current_owner) if item.id == auction_id), None)
            if auction is None:
                raise ValueError("Аукцион не найден")
            if form.get("clear_discount") == "1":
                max_discount_percent = None
                min_bid_amount = None
            else:
                discount_percent = parse_optional_number(form.get("discount_percent", ""))
                min_amount = parse_optional_number(form.get("min_amount", ""))
                if discount_percent is None and min_amount is None:
                    raise ValueError("Нужно указать либо процент, либо минимальную сумму")
                if discount_percent is None:
                    if min_amount is None:
                        raise ValueError("Нужно указать минимальную сумму")
                    if min_amount < 0 or min_amount > auction.amount:
                        raise ValueError("Минимальная сумма должна быть в диапазоне от 0 до суммы аукциона")
                    discount_percent = round(((auction.amount - min_amount) / auction.amount) * 100, 2)
                if min_amount is None:
                    min_amount = auction_min_amount(auction.amount, discount_percent)
                if discount_percent < 0 or discount_percent >= 100:
                    raise ValueError("Процент снижения должен быть от 0 до 99,99")
                max_discount_percent = round(discount_percent, 2)
                min_bid_amount = round(min_amount, 2)
            if max_discount_percent is not None and max_discount_percent < 0:
                raise ValueError("Процент снижения не может быть отрицательным")
            actor_name = current_user.get("full_name", "").strip() if current_user else ""
            updated_at = None if max_discount_percent is None else datetime.utcnow()
            updated_by_name = "" if max_discount_percent is None else actor_name
            updated = storage.update_auction_max_discount(
                current_owner,
                auction_id,
                max_discount_percent,
                min_bid_amount,
                updated_at,
                updated_by_name,
            )
            if not updated:
                raise ValueError("Аукцион не найден")
            target_tab = "archive" if is_auction_archived(auction) else current_auction_tab
            return redirect(start_response, f"/auctions?owner={current_owner}&tab={target_tab}")
        except Exception as exc:
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, f"Не удалось обновить максимальное снижение: {exc}")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/auctions/") and path.endswith("/amount") and method == "POST":
        denied = guard("auctions", "edit")
        if denied:
            return denied
        if is_procurement_user(current_user) or is_supply_user(current_user) or is_management_user(current_user):
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, "Эта роль не меняет сумму аукциона.")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        form = read_post_data(environ)
        try:
            auction_id = int(path.split("/")[2])
            auction = next((item for item in storage.list_auctions(current_owner) if item.id == auction_id), None)
            if auction is None:
                raise ValueError("Аукцион не найден")
            amount = parse_amount(form["amount"])
            has_advance = form.get("has_advance") == "1"
            advance_percent = parse_optional_number(form.get("advance_percent", "")) if has_advance else None
            if auction.min_bid_amount is not None and amount < auction.min_bid_amount:
                raise ValueError("Сумма аукциона не может быть меньше уже заданной минимальной суммы")
            if has_advance:
                if advance_percent is None:
                    raise ValueError("Укажите процент авансирования")
                if advance_percent <= 0 or advance_percent > 100:
                    raise ValueError("Процент авансирования должен быть от 0,01 до 100")
            recalculated_discount = auction.max_discount_percent
            if auction.min_bid_amount is not None and amount > 0:
                recalculated_discount = round(((amount - auction.min_bid_amount) / amount) * 100, 2)
            updated = storage.update_auction_amount(current_owner, auction_id, amount, advance_percent, recalculated_discount)
            if not updated:
                raise ValueError("Аукцион не найден")
            target_tab = "archive" if is_auction_archived(auction) else current_auction_tab
            return redirect(start_response, f"/auctions?owner={current_owner}&tab={target_tab}")
        except Exception as exc:
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, f"Не удалось обновить сумму аукциона: {exc}")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/auctions/") and path.endswith("/details") and method == "POST":
        denied = guard("auctions", "edit")
        if denied:
            return denied
        if is_procurement_user(current_user) or is_supply_user(current_user) or is_management_user(current_user):
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, "Эта роль не меняет карточку аукциона.")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        form = read_post_data(environ)
        try:
            auction_id = int(path.split("/")[2])
            auction = next((item for item in storage.list_auctions(current_owner) if item.id == auction_id), None)
            if auction is None:
                raise ValueError("Аукцион не найден")
            auction_number = validate_auction_number(form.get("auction_number", ""))
            title = form.get("title", "").strip()
            city = form.get("city", "").strip()
            source_url = form.get("source_url", "").strip()
            created_date = parse_date(form.get("created_date", ""))
            has_advance = form.get("has_advance") == "1"
            advance_percent = parse_optional_number(form.get("advance_percent", "")) if has_advance else None
            if not title:
                raise ValueError("Нужно указать название аукциона")
            if not city:
                raise ValueError("Нужно указать город")
            if has_advance:
                if advance_percent is None:
                    raise ValueError("Укажите процент авансирования")
                if advance_percent <= 0 or advance_percent > 100:
                    raise ValueError("Процент авансирования должен быть от 0,01 до 100")
            updated = storage.update_auction_details(current_owner, auction_id, auction_number, title, city, source_url, created_date, advance_percent)
            if not updated:
                raise ValueError("Аукцион не найден")
            target_tab = "archive" if is_auction_archived(auction) else current_auction_tab
            return redirect(start_response, f"/auctions?owner={current_owner}&tab={target_tab}")
        except Exception as exc:
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, f"Не удалось обновить данные аукциона: {exc}")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/auctions/") and path.endswith("/deadline") and method == "POST":
        denied = guard("auctions", "edit")
        if denied:
            return denied
        if is_procurement_user(current_user) or is_supply_user(current_user) or is_management_user(current_user):
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, "Эта роль не меняет дедлайн аукциона.")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        form = read_post_data(environ)
        try:
            auction_id = int(path.split("/")[2])
            bid_deadline = parse_date(form["bid_deadline"])
            updated = storage.update_auction_deadline(current_owner, auction_id, bid_deadline)
            if not updated:
                raise ValueError("Аукцион не найден")
            updated_auction = next((item for item in storage.list_auctions(current_owner) if item.id == auction_id), None)
            if updated_auction is None:
                raise ValueError("Аукцион не найден")
            target_tab = "archive" if is_auction_archived(updated_auction) else current_auction_tab
            return redirect(start_response, f"/auctions?owner={current_owner}&tab={target_tab}")
        except Exception as exc:
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, f"Не удалось обновить дату подачи: {exc}")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/auctions/") and path.endswith("/delete") and method == "POST":
        denied = guard("auctions", "edit")
        if denied:
            return denied
        if is_procurement_user(current_user) or is_supply_user(current_user) or is_management_user(current_user):
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, "Эта роль не удаляет аукционы.")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        try:
            auction_id = int(path.split("/")[2])
            deleted = storage.soft_delete_auction(current_owner, auction_id, datetime.now())
            if not deleted:
                raise ValueError("Аукцион не найден")
            return redirect(start_response, f"/auctions?owner={current_owner}&tab={current_auction_tab}")
        except Exception as exc:
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, f"Не удалось удалить аукцион: {exc}")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/auctions/") and path.endswith("/restore") and method == "POST":
        denied = guard("auctions", "view")
        if denied:
            return denied
        try:
            auction_id = int(path.split("/")[2])
            restored = storage.restore_deleted_auction(current_owner, auction_id)
            if not restored:
                raise ValueError("Аукцион не найден")
            return redirect(start_response, f"/auctions?owner={current_owner}&tab=deleted")
        except Exception as exc:
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, f"Не удалось вернуть аукцион: {exc}")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/auctions/") and path.endswith("/purge") and method == "POST":
        denied = guard("auctions", "edit")
        if denied:
            return denied
        if not has_active_admin_mode(current_user):
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, "Только главный пользователь может очищать аукционы навсегда.")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        try:
            auction_id = int(path.split("/")[2])
            deleted = storage.hard_delete_auction(current_owner, auction_id)
            if not deleted:
                raise ValueError("Аукцион не найден")
            return redirect(start_response, f"/auctions?owner={current_owner}&tab=deleted")
        except Exception as exc:
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, f"Не удалось удалить аукцион навсегда: {exc}")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path == "/auctions/purge-deleted" and method == "POST":
        denied = guard("auctions", "edit")
        if denied:
            return denied
        if not has_active_admin_mode(current_user):
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, "Только главный пользователь может очищать корзину.")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        try:
            storage.hard_delete_all_deleted_auctions(current_owner)
            return redirect(start_response, f"/auctions?owner={current_owner}&tab=deleted")
        except Exception as exc:
            body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, f"Не удалось очистить корзину: {exc}")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/auctions/") and path.endswith("/row") and method == "GET":
        denied = guard("auctions", "view")
        if denied:
            return denied
        try:
            auction_id = int(path.split("/")[2])
            auction = next((item for item in storage.list_auctions(current_owner) if item.id == auction_id), None)
            if auction is None:
                raise ValueError("Аукцион не найден")
            html = render_auction_row(auction, current_owner, current_auction_tab, 1, current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        except Exception:
            start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
            return [b"Not found"]

    if path == "/auctions/table" and method == "GET":
        denied = guard("auctions", "view")
        if denied:
            return denied
        auctions = auctions_for_current_view(storage, current_owner, current_user, current_auction_tab, current_task_view)
        html = render_auction_rows(auctions, current_owner, current_auction_tab, current_user)
        start_response(
            "200 OK",
            [
                ("Content-Type", "text/html; charset=utf-8"),
                ("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0"),
                ("Pragma", "no-cache"),
            ],
        )
        return [html.encode("utf-8")]

    if path.startswith("/auctions/") and method == "GET":
        denied = guard("auctions", "view")
        if denied:
            return denied
        fallback_url = f"/auctions?owner={current_owner}&tab={current_auction_tab}"
        if current_task_view:
            fallback_url += "&task_view=1"
        fallback_url += "#auction-registry"
        return redirect(start_response, fallback_url)

    if path == "/auctions":
        denied = guard("auctions", "view")
        if denied:
            return denied
        body = render_auctions_section(storage, current_owner, current_user, current_auction_tab, task_view=current_task_view)
        html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path == "/access/users/new" and method == "POST":
        denied = guard("access", "edit")
        if denied:
            return denied
        form = read_post_data(environ)
        try:
            full_name = form["full_name"].strip()
            login = form["login"].strip().lower()
            role_name = form.get("role_name", "").strip() or "Viewer"
            permissions = parse_permissions(form)
            if not full_name:
                raise ValueError("Нужно указать имя пользователя")
            if not login:
                raise ValueError("Нужно указать логин")
            if not any(item["can_view"] for item in permissions.values()):
                raise ValueError("Нужно открыть хотя бы один раздел для просмотра")
            storage.create_web_user(current_owner, login, full_name, role_name, permissions)
            return redirect(start_response, f"/access?owner={current_owner}")
        except Exception as exc:
            body = render_access_section(storage, current_owner, request_base_url(environ), f"Не удалось добавить пользователя: {exc}")
        html = layout("Доступы", body, owners, current_owner, "access", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path.startswith("/access/users/") and path.endswith("/reset-password") and method == "POST":
        denied = guard("access", "edit")
        if denied:
            return denied
        try:
            user_id = int(path.split("/")[3])
            user = storage.get_web_user_by_id(user_id)
            if user is None or user["owner_chat_id"] != current_owner:
                raise ValueError("Пользователь не найден")
            storage.regenerate_password_setup_token(user_id, secrets.token_urlsafe(24))
            return redirect(start_response, f"/access?owner={current_owner}")
        except Exception as exc:
            body = render_access_section(storage, current_owner, request_base_url(environ), f"Не удалось сбросить пароль: {exc}")
        html = layout("Доступы", body, owners, current_owner, "access", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path.startswith("/access/users/") and path.endswith("/update") and method == "POST":
        denied = guard("access", "edit")
        if denied:
            return denied
        try:
            user_id = int(path.split("/")[3])
            form = read_post_data(environ)
            updated = storage.update_web_user(
                current_owner,
                user_id,
                form.get("full_name_visible", form.get("full_name", "")),
                form.get("role_name", ""),
                parse_permissions(form),
            )
            if not updated:
                raise ValueError("Пользователь не найден")
            return redirect(start_response, f"/access?owner={current_owner}")
        except Exception as exc:
            body = render_access_section(storage, current_owner, request_base_url(environ), f"Не удалось обновить пользователя: {exc}")
        html = layout("Доступы", body, owners, current_owner, "access", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path.startswith("/access/users/") and path.endswith("/toggle") and method == "POST":
        denied = guard("access", "edit")
        if denied:
            return denied
        try:
            user_id = int(path.split("/")[3])
            users = storage.list_web_users(current_owner)
            target = next((user for user in users if user["id"] == user_id), None)
            if target is None:
                raise ValueError("Пользователь не найден")
            updated = storage.set_web_user_active(current_owner, user_id, not target["is_active"])
            if not updated:
                raise ValueError("Нельзя отключить главного администратора")
            return redirect(start_response, f"/access?owner={current_owner}")
        except Exception as exc:
            body = render_access_section(storage, current_owner, request_base_url(environ), f"Не удалось изменить статус доступа: {exc}")
        html = layout("Доступы", body, owners, current_owner, "access", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path.startswith("/access/users/") and path.endswith("/delete") and method == "POST":
        denied = guard("access", "edit")
        if denied:
            return denied
        try:
            user_id = int(path.split("/")[3])
            deleted = storage.delete_web_user(current_owner, user_id)
            if not deleted:
                raise ValueError("Нельзя удалить главного администратора или пользователь не найден")
            return redirect(start_response, f"/access?owner={current_owner}")
        except Exception as exc:
            body = render_access_section(storage, current_owner, request_base_url(environ), f"Не удалось удалить пользователя: {exc}")
        html = layout("Доступы", body, owners, current_owner, "access", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path == "/access":
        denied = guard("access", "view")
        if denied:
            return denied
        body = render_access_section(storage, current_owner, request_base_url(environ))
        html = layout("Доступы", body, owners, current_owner, "access", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path == "/payables/new" and method == "POST":
        denied = guard("payables", "edit")
        if denied:
            return denied
        form = read_post_data(environ)
        try:
            counterparty = form.get("counterparty", "").strip()
            document_ref = form.get("document_ref", "").strip()
            document_date_raw = form.get("document_date", "").strip()
            document_date = parse_date(document_date_raw) if document_date_raw else None
            object_name = form.get("object_name", "").strip()
            comment = form.get("comment", "").strip()
            amount = parse_amount(form.get("amount", "0"))
            due_date_raw = form.get("due_date", "").strip()
            due_days_raw = form.get("due_days", "").strip()
            if due_date_raw:
                due_date = parse_date(due_date_raw)
            elif due_days_raw:
                due_days = int(due_days_raw)
                if due_days <= 0:
                    raise ValueError("Срок оплаты в днях должен быть больше 0")
                due_base = document_date or datetime.now(VLADIVOSTOK_TZ).date()
                due_date = due_base + timedelta(days=due_days)
            else:
                raise ValueError("Укажите срок оплаты датой или в днях")
            if not counterparty:
                raise ValueError("Укажите контрагента")
            if not document_ref:
                raise ValueError("Укажите документ")
            if amount <= 0:
                raise ValueError("Сумма должна быть больше 0")
            storage.add_payable(
                current_owner,
                counterparty,
                document_ref,
                document_date,
                object_name,
                comment,
                amount,
                due_date,
                current_user.get("id") if current_user else None,
                current_user.get("full_name", "") if current_user else "",
            )
            return redirect(start_response, f"/payables{payable_query_suffix(current_owner, current_payables_tab, current_payables_counterparty, current_payables_sort, current_payables_order)}")
        except Exception as exc:
            body = render_payables_section(storage, current_owner, current_user, current_payables_tab, f"Не удалось добавить запись: {exc}", counterparty_filter=current_payables_counterparty, sort_key=current_payables_sort, sort_order=current_payables_order)
            html = layout("Кредиторка", body, owners, current_owner, "payables", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/payables/") and path.endswith("/update") and method == "POST":
        denied = guard("payables", "edit")
        if denied:
            return denied
        payable_id = int(path.split("/")[2])
        form = read_post_data(environ)
        try:
            counterparty = form.get("counterparty", "").strip()
            document_ref = form.get("document_ref", "").strip()
            document_date_raw = form.get("document_date", "").strip()
            document_date = parse_date(document_date_raw) if document_date_raw else None
            object_name = form.get("object_name", "").strip()
            comment = form.get("comment", "").strip()
            amount = parse_amount(form.get("amount", "0"))
            due_date = parse_date(form.get("due_date", ""))
            if not counterparty:
                raise ValueError("Укажите контрагента")
            if not document_ref:
                raise ValueError("Укажите документ")
            if amount <= 0:
                raise ValueError("Сумма должна быть больше 0")
            if not storage.update_payable_details(current_owner, payable_id, counterparty, document_ref, document_date, object_name, comment, amount, due_date):
                raise ValueError("Запись не найдена")
            updated_entry = storage.get_payable(current_owner, payable_id)
            target_tab = "archive" if updated_entry is not None and is_payable_archived(updated_entry) else current_payables_tab
            return redirect(start_response, f"/payables{payable_query_suffix(current_owner, target_tab, current_payables_counterparty, current_payables_sort, current_payables_order)}")
        except Exception as exc:
            body = render_payables_section(storage, current_owner, current_user, current_payables_tab, f"Не удалось обновить запись: {exc}", counterparty_filter=current_payables_counterparty, sort_key=current_payables_sort, sort_order=current_payables_order)
            html = layout("Кредиторка", body, owners, current_owner, "payables", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/payables/") and path.endswith("/field") and method == "POST":
        denied = guard("payables", "edit")
        if denied:
            return denied
        payable_id = int(path.split("/")[2])
        form = read_post_data(environ)
        try:
            field_name = form.get("field_name", "").strip()
            entry = storage.get_payable(current_owner, payable_id)
            if entry is None:
                raise ValueError("Запись не найдена")
            counterparty = entry.counterparty
            document_ref = entry.document_ref
            document_date = entry.document_date
            object_name = entry.object_name
            comment = entry.comment
            amount = entry.amount
            due_date = entry.due_date

            if field_name == "counterparty":
                counterparty = form.get("value", "").strip()
                if not counterparty:
                    raise ValueError("Укажите контрагента")
            elif field_name == "document_ref":
                document_ref = form.get("value", "").strip()
                document_date_raw = form.get("document_date", "").strip()
                document_date = parse_date(document_date_raw) if document_date_raw else None
                if not document_ref:
                    raise ValueError("Укажите основание")
            elif field_name == "object_name":
                object_name = form.get("value", "").strip()
            elif field_name == "comment":
                comment = form.get("value", "").strip()
            elif field_name == "amount":
                amount = parse_amount(form.get("value", "0"))
                if amount <= 0:
                    raise ValueError("Сумма должна быть больше нуля")
            else:
                raise ValueError("Некорректное поле редактирования")

            if not storage.update_payable_details(
                current_owner,
                payable_id,
                counterparty,
                document_ref,
                document_date,
                object_name,
                comment,
                amount,
                due_date,
            ):
                raise ValueError("Не удалось обновить запись")
            return redirect(start_response, f"/payables{payable_query_suffix(current_owner, current_payables_tab, current_payables_counterparty, current_payables_sort, current_payables_order)}#payables-registry")
        except Exception as exc:
            body = render_payables_section(storage, current_owner, current_user, current_payables_tab, f"Не удалось обновить поле: {exc}", counterparty_filter=current_payables_counterparty, sort_key=current_payables_sort, sort_order=current_payables_order)
            html = layout("Кредиторка", body, owners, current_owner, "payables", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/payables/") and path.endswith("/payment") and method == "POST":
        denied = guard("payables", "edit")
        if denied:
            return denied
        payable_id = int(path.split("/")[2])
        form = read_post_data(environ)
        try:
            entry = storage.get_payable(current_owner, payable_id)
            if entry is None:
                raise ValueError("Запись не найдена")
            amount = parse_amount(form.get("amount", "0"))
            is_paid = form.get("is_paid") == "1"
            paid_amount = parse_amount(form.get("paid_amount", "0")) if is_paid else 0.0
            paid_date = parse_date(form["paid_date"]) if is_paid else None
            if amount <= 0:
                raise ValueError("Сумма обязательства должна быть больше 0")
            if paid_amount < 0:
                raise ValueError("Сумма оплаты не может быть отрицательной")
            updated = storage.update_payable_details(
                current_owner,
                payable_id,
                entry.counterparty,
                entry.document_ref,
                entry.document_date,
                entry.object_name,
                entry.comment,
                amount,
                entry.due_date,
            )
            if not updated:
                raise ValueError("Запись не найдена")
            if not storage.update_payable_payment(current_owner, payable_id, paid_amount if is_paid else 0.0, paid_date):
                raise ValueError("Не удалось обновить оплату")
            updated_entry = storage.get_payable(current_owner, payable_id)
            target_tab = "archive" if updated_entry is not None and is_payable_archived(updated_entry) else current_payables_tab
            return redirect(start_response, f"/payables{payable_query_suffix(current_owner, target_tab, current_payables_counterparty, current_payables_sort, current_payables_order)}")
        except Exception as exc:
            body = render_payables_section(storage, current_owner, current_user, current_payables_tab, f"Не удалось обновить оплату: {exc}", counterparty_filter=current_payables_counterparty, sort_key=current_payables_sort, sort_order=current_payables_order)
            html = layout("Кредиторка", body, owners, current_owner, "payables", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/payables/") and path.endswith("/payment/reset") and method == "POST":
        denied = guard("payables", "edit")
        if denied:
            return denied
        payable_id = int(path.split("/")[2])
        try:
            entry = storage.get_payable(current_owner, payable_id)
            if entry is None:
                raise ValueError("Запись не найдена")
            if not storage.update_payable_payment(current_owner, payable_id, 0.0, None):
                raise ValueError("Не удалось сбросить оплату")
            return redirect(start_response, f"/payables{payable_query_suffix(current_owner, current_payables_tab, current_payables_counterparty, current_payables_sort, current_payables_order)}")
        except Exception as exc:
            body = render_payables_section(storage, current_owner, current_user, current_payables_tab, f"Не удалось сбросить оплату: {exc}", counterparty_filter=current_payables_counterparty, sort_key=current_payables_sort, sort_order=current_payables_order)
            html = layout("Кредиторка", body, owners, current_owner, "payables", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/payables/") and path.endswith("/delete") and method == "POST":
        denied = guard("payables", "edit")
        if denied:
            return denied
        payable_id = int(path.split("/")[2])
        deleted = storage.soft_delete_payable(current_owner, payable_id, datetime.now())
        if not deleted:
            body = render_payables_section(storage, current_owner, current_user, current_payables_tab, "Не удалось удалить запись", counterparty_filter=current_payables_counterparty, sort_key=current_payables_sort, sort_order=current_payables_order)
            html = layout("Кредиторка", body, owners, current_owner, "payables", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        return redirect(start_response, f"/payables{payable_query_suffix(current_owner, current_payables_tab, current_payables_counterparty, current_payables_sort, current_payables_order)}")

    if path.startswith("/payables/") and path.endswith("/restore") and method == "POST":
        denied = guard("payables", "view")
        if denied:
            return denied
        payable_id = int(path.split("/")[2])
        restored = storage.restore_deleted_payable(current_owner, payable_id)
        if not restored:
            body = render_payables_section(storage, current_owner, current_user, current_payables_tab, "Не удалось вернуть запись", counterparty_filter=current_payables_counterparty, sort_key=current_payables_sort, sort_order=current_payables_order)
            html = layout("Кредиторка", body, owners, current_owner, "payables", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        return redirect(start_response, f"/payables{payable_query_suffix(current_owner, 'deleted', current_payables_counterparty, current_payables_sort, current_payables_order)}")

    if path.startswith("/payables/") and path.endswith("/purge") and method == "POST":
        if not has_active_admin_mode(current_user):
            body = render_forbidden_body("Кредиторка")
            html = layout("Доступ запрещен", body, owners, current_owner, "payables", current_user)
            start_response("403 Forbidden", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        payable_id = int(path.split("/")[2])
        deleted = storage.hard_delete_payable(current_owner, payable_id)
        if not deleted:
            body = render_payables_section(storage, current_owner, current_user, current_payables_tab, "Не удалось удалить запись навсегда", counterparty_filter=current_payables_counterparty, sort_key=current_payables_sort, sort_order=current_payables_order)
            html = layout("Кредиторка", body, owners, current_owner, "payables", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        return redirect(start_response, f"/payables{payable_query_suffix(current_owner, 'deleted', current_payables_counterparty, current_payables_sort, current_payables_order)}")

    if path == "/payables/purge-deleted" and method == "POST":
        if not has_active_admin_mode(current_user):
            body = render_forbidden_body("Кредиторка")
            html = layout("Доступ запрещен", body, owners, current_owner, "payables", current_user)
            start_response("403 Forbidden", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        storage.hard_delete_all_deleted_payables(current_owner)
        return redirect(start_response, f"/payables{payable_query_suffix(current_owner, 'deleted', current_payables_counterparty, current_payables_sort, current_payables_order)}")

    if method == "GET" and (path == "/payables/new" or (path.startswith("/payables/") and (path.endswith("/update") or path.endswith("/field") or path.endswith("/payment") or path.endswith("/payment/reset") or path.endswith("/delete") or path.endswith("/restore") or path.endswith("/purge")))):
        return redirect(start_response, f"/payables{payable_query_suffix(current_owner, current_payables_tab, current_payables_counterparty, current_payables_sort, current_payables_order)}")

    if path.startswith("/contracts/") and path.endswith("/settings") and method == "POST":
        if not can_edit_contract_stage_controls(current_user):
            body = render_forbidden_body("Контракты")
            html = layout("Доступ запрещен", body, owners, current_owner, "contracts", current_user)
            start_response("403 Forbidden", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        try:
            contract_id = int(path.split("/")[2])
            form = read_post_data(environ)
            has_advance = form.get("has_advance") == "1"
            advance_percent = parse_optional_number(form.get("advance_percent", "")) if has_advance else None
            if has_advance:
                if advance_percent is None:
                    raise ValueError("Укажите процент аванса")
                if advance_percent <= 0 or advance_percent > 100:
                    raise ValueError("Процент аванса должен быть от 0,01 до 100")
            updated = storage.update_contract_advance_percent(current_owner, contract_id, advance_percent)
            if not updated:
                raise ValueError("Контракт не найден")
            return redirect(start_response, f"/contracts/{contract_id}?owner={current_owner}")
        except Exception as exc:
            body = render_contract_detail(storage, current_owner, contract_id, current_user, f"Не удалось обновить контракт: {exc}")
            html = layout("Карточка контракта", body, owners, current_owner, "contracts", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/contracts/") and path.endswith("/signed-date") and method == "POST":
        if not can_edit_contract_stage_controls(current_user):
            body = render_forbidden_body("Контракты")
            html = layout("Доступ запрещен", body, owners, current_owner, "contracts", current_user)
            start_response("403 Forbidden", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        try:
            contract_id = int(path.split("/")[2])
            form = read_post_data(environ)
            signed_date = None if form.get("is_unsigned") == "1" else parse_date(form["signed_date"])
            updated = storage.update_contract_signed_date(current_owner, contract_id, signed_date)
            if not updated:
                raise ValueError("Контракт не найден")
            return redirect(start_response, f"/contracts/{contract_id}?owner={current_owner}")
        except Exception as exc:
            body = render_contract_detail(storage, current_owner, contract_id, current_user, f"Не удалось обновить дату контракта: {exc}")
            html = layout("Карточка контракта", body, owners, current_owner, "contracts", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path == "/payroll/employees/new" and method == "POST":
        denied = guard("payroll", "edit")
        if denied:
            return denied
        selected_month = parse_month_key(parse_qs(environ.get("QUERY_STRING", "")).get("month", [""])[0])
        form = read_post_data(environ)
        try:
            full_name = form.get("full_name", "").strip()
            role_title = form.get("role_title", "").strip()
            if not full_name:
                raise ValueError("Укажите ФИО сотрудника")
            if not role_title:
                raise ValueError("Укажите должность сотрудника")
            storage.add_payroll_employee(current_owner, full_name, role_title)
            month_param = selected_month.strftime("%Y-%m") if selected_month is not None else ""
            return redirect(start_response, f"/payroll?owner={current_owner}&month={month_param}")
        except Exception as exc:
            body = render_payroll_section(storage, current_owner, current_user, selected_month, f"Не удалось добавить сотрудника: {exc}")
            html = layout("Зарплата", body, owners, current_owner, "payroll", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/payroll/entries/") and path.endswith("/amount") and method == "POST":
        denied = guard("payroll", "edit")
        if denied:
            return denied
        employee_id = int(path.split("/")[3])
        selected_month = parse_month_key(parse_qs(environ.get("QUERY_STRING", "")).get("month", [""])[0]) or date.today().replace(day=1)
        form = read_post_data(environ)
        try:
            field_name = form.get("field_name", "").strip()
            amount = parse_amount(form.get("amount", "0"))
            if not storage.upsert_payroll_amount(current_owner, employee_id, selected_month, field_name, amount):
                raise ValueError("Не удалось обновить сумму")
            return redirect(start_response, f"/payroll?owner={current_owner}&month={selected_month.strftime('%Y-%m')}")
        except Exception as exc:
            body = render_payroll_section(storage, current_owner, current_user, selected_month, f"Не удалось обновить сумму: {exc}")
            html = layout("Зарплата", body, owners, current_owner, "payroll", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/payroll/entries/") and path.endswith("/payment") and method == "POST":
        denied = guard("payroll", "edit")
        if denied:
            return denied
        employee_id = int(path.split("/")[3])
        selected_month = parse_month_key(parse_qs(environ.get("QUERY_STRING", "")).get("month", [""])[0]) or date.today().replace(day=1)
        form = read_post_data(environ)
        try:
            payment_kind = form.get("payment_kind", "").strip()
            planned_amount = parse_amount(form.get("planned_amount", "0"))
            is_paid = form.get("is_paid") == "1"
            paid_amount = parse_amount(form.get("paid_amount", "0")) if is_paid else None
            paid_date = parse_date(form["paid_date"]) if is_paid else None
            if is_paid and paid_amount is not None and paid_amount < 0:
                raise ValueError("Сумма выплаты не может быть отрицательной")
            if not storage.upsert_payroll_payment(current_owner, employee_id, selected_month, payment_kind, planned_amount, paid_amount, paid_date, is_paid):
                raise ValueError("Не удалось обновить выплату")
            return redirect(start_response, f"/payroll?owner={current_owner}&month={selected_month.strftime('%Y-%m')}")
        except Exception as exc:
            body = render_payroll_section(storage, current_owner, current_user, selected_month, f"Не удалось обновить выплату: {exc}")
            html = layout("Зарплата", body, owners, current_owner, "payroll", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/payroll/entries/") and path.endswith("/note") and method == "POST":
        denied = guard("payroll", "edit")
        if denied:
            return denied
        employee_id = int(path.split("/")[3])
        selected_month = parse_month_key(parse_qs(environ.get("QUERY_STRING", "")).get("month", [""])[0]) or date.today().replace(day=1)
        form = read_post_data(environ)
        try:
            note = form.get("note", "")
            if not storage.upsert_payroll_note(current_owner, employee_id, selected_month, note):
                raise ValueError("Не удалось обновить заметку")
            return redirect(start_response, f"/payroll?owner={current_owner}&month={selected_month.strftime('%Y-%m')}")
        except Exception as exc:
            body = render_payroll_section(storage, current_owner, current_user, selected_month, f"Не удалось обновить заметку: {exc}")
            html = layout("Зарплата", body, owners, current_owner, "payroll", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if method == "GET" and (path == "/payroll/employees/new" or path.startswith("/payroll/entries/")):
        month_param = parse_qs(environ.get("QUERY_STRING", "")).get("month", [""])[0]
        month_query = f"&month={month_param}" if month_param else ""
        return redirect(start_response, f"/payroll?owner={current_owner}{month_query}")

    if path.startswith("/contracts/stages/") and path.endswith("/status") and method == "POST":
        if not can_edit_contract_stage_controls(current_user):
            body = render_forbidden_body("Контракты")
            html = layout("Доступ запрещен", body, owners, current_owner, "contracts", current_user)
            start_response("403 Forbidden", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        try:
            stage_id = int(path.split("/")[3])
            contract_id = int(parse_qs(environ.get("QUERY_STRING", "")).get("contract_id", ["0"])[0])
            form = read_post_data(environ)
            status = form.get("status", "not_started")
            if status not in STATUS_META:
                raise ValueError("Некорректный статус этапа")
            stage = storage.get_stage(current_owner, stage_id)
            if stage is None:
                raise ValueError("Этап не найден")
            actor_name = current_user.get("full_name", "").strip() if current_user else ""
            updated = storage.update_stage_status(current_owner, stage_id, status, datetime.utcnow(), actor_name)
            if not updated:
                raise ValueError("Этап не найден")
            return redirect(start_response, f"/contracts/{contract_id}?owner={current_owner}")
        except Exception as exc:
            body = render_contract_detail(storage, current_owner, contract_id, current_user, f"Не удалось обновить статус этапа: {exc}")
            html = layout("Карточка контракта", body, owners, current_owner, "contracts", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/contracts/stages/") and path.endswith("/payment-status") and method == "POST":
        if not can_edit_contract_stage_controls(current_user):
            body = render_forbidden_body("Контракты")
            html = layout("Доступ запрещен", body, owners, current_owner, "contracts", current_user)
            start_response("403 Forbidden", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        try:
            stage_id = int(path.split("/")[3])
            contract_id = int(parse_qs(environ.get("QUERY_STRING", "")).get("contract_id", ["0"])[0])
            form = read_post_data(environ)
            payment_status = form.get("payment_status", "unpaid")
            if payment_status not in STAGE_PAYMENT_META:
                raise ValueError("Некорректный статус оплаты")
            stage = storage.get_stage(current_owner, stage_id)
            if stage is None:
                raise ValueError("Этап не найден")
            actor_name = current_user.get("full_name", "").strip() if current_user else ""
            updated = storage.update_stage_payment_status(current_owner, stage_id, payment_status, datetime.utcnow(), actor_name)
            if not updated:
                raise ValueError("Этап не найден")
            return redirect(start_response, f"/contracts/{contract_id}?owner={current_owner}")
        except Exception as exc:
            body = render_contract_detail(storage, current_owner, contract_id, current_user, f"Не удалось обновить оплату этапа: {exc}")
            html = layout("Карточка контракта", body, owners, current_owner, "contracts", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/contracts/stages/") and path.endswith("/invoice-status") and method == "POST":
        if not can_edit_contract_stage_controls(current_user):
            body = render_forbidden_body("Контракты")
            html = layout("Доступ запрещен", body, owners, current_owner, "contracts", current_user)
            start_response("403 Forbidden", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        try:
            stage_id = int(path.split("/")[3])
            contract_id = int(parse_qs(environ.get("QUERY_STRING", "")).get("contract_id", ["0"])[0])
            form = read_post_data(environ)
            invoice_kind = form.get("invoice_kind", "").strip()
            if invoice_kind not in {"advance", "final"}:
                raise ValueError("Некорректный тип счета")
            issued = form.get("issued") == "1"
            actor_name = current_user.get("full_name", "").strip() if current_user else ""
            updated = storage.update_stage_invoice_status(
                current_owner,
                stage_id,
                invoice_kind,
                issued,
                datetime.utcnow() if issued else None,
                actor_name,
            )
            if not updated:
                raise ValueError("Этап не найден")
            return redirect(start_response, f"/contracts/{contract_id}?owner={current_owner}")
        except Exception as exc:
            body = render_contract_detail(storage, current_owner, contract_id, current_user, f"Не удалось обновить счет этапа: {exc}")
            html = layout("Карточка контракта", body, owners, current_owner, "contracts", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/contracts/stages/") and path.endswith("/deadline") and method == "POST":
        if not can_edit_contract_stage_controls(current_user):
            body = render_forbidden_body("Контракты")
            html = layout("Доступ запрещен", body, owners, current_owner, "contracts", current_user)
            start_response("403 Forbidden", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        try:
            stage_id = int(path.split("/")[3])
            contract_id = int(parse_qs(environ.get("QUERY_STRING", "")).get("contract_id", ["0"])[0])
            form = read_post_data(environ)
            start_date_raw = form.get("start_date", "")
            start_date = parse_date(start_date_raw) if start_date_raw else None
            end_date = parse_date(form["end_date"])
            if start_date is not None and start_date > end_date:
                raise ValueError("Старт работ не может быть позже дедлайна этапа")
            updated = storage.update_stage_deadline(current_owner, stage_id, start_date, end_date)
            if not updated:
                raise ValueError("Этап не найден")
            return redirect(start_response, f"/contracts/{contract_id}?owner={current_owner}")
        except Exception as exc:
            body = render_contract_detail(storage, current_owner, contract_id, current_user, f"Не удалось обновить дедлайн этапа: {exc}")
            html = layout("Карточка контракта", body, owners, current_owner, "contracts", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/contracts/stages/") and path.endswith("/amount") and method == "POST":
        if not can_edit_contract_stage_controls(current_user):
            body = render_forbidden_body("Контракты")
            html = layout("Доступ запрещен", body, owners, current_owner, "contracts", current_user)
            start_response("403 Forbidden", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        try:
            stage_id = int(path.split("/")[3])
            contract_id = int(parse_qs(environ.get("QUERY_STRING", "")).get("contract_id", ["0"])[0])
            form = read_post_data(environ)
            amount = parse_amount(form["amount"])
            if amount <= 0:
                raise ValueError("Сумма этапа должна быть больше 0")
            updated = storage.update_stage_amount(current_owner, stage_id, amount)
            if not updated:
                raise ValueError("Этап не найден")
            return redirect(start_response, f"/contracts/{contract_id}?owner={current_owner}")
        except Exception as exc:
            body = render_contract_detail(storage, current_owner, contract_id, current_user, f"Не удалось обновить сумму этапа: {exc}")
            html = layout("Карточка контракта", body, owners, current_owner, "contracts", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/contracts/") and path.endswith("/stages/new") and method == "POST":
        denied = guard("contracts", "edit")
        if denied:
            return denied
        try:
            contract_id = int(path.split("/")[2])
            form = read_post_data(environ)
            contract = storage.get_contract(current_owner, contract_id)
            if contract is None:
                raise ValueError("Контракт не найден")
            position = int(form["position"])
            end_date = parse_date(form["end_date"])
            if end_date > contract.end_date:
                raise ValueError(f"Этап не может быть позже дедлайна контракта ({format_date(contract.end_date)})")
            amount = parse_amount(form["amount"])
            storage.add_stage(contract_id, position, form.get("notes", ""), end_date, amount)
            return redirect(start_response, f"/contracts/{contract_id}?owner={current_owner}")
        except Exception as exc:
            body = render_contract_detail(storage, current_owner, contract_id, current_user, f"Не удалось добавить этап: {exc}")
            html = layout("Карточка контракта", body, owners, current_owner, "contracts", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/contracts/") and path.endswith("/payments/new") and method == "POST":
        denied = guard("contracts", "edit")
        if denied:
            return denied
        try:
            contract_id = int(path.split("/")[2])
            form = read_post_data(environ)
            payment_date = parse_date(form["payment_date"])
            amount = parse_amount(form["amount"])
            created = storage.add_payment(current_owner, contract_id, payment_date, amount)
            if created is None:
                raise ValueError("Контракт не найден")
            return redirect(start_response, f"/contracts/{contract_id}?owner={current_owner}")
        except Exception as exc:
            body = render_contract_detail(storage, current_owner, contract_id, current_user, f"Не удалось добавить оплату: {exc}")
            html = layout("Карточка контракта", body, owners, current_owner, "contracts", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/contracts/"):
        denied = guard("contracts", "view")
        if denied:
            return denied
        try:
            contract_id = int(path.rsplit("/", 1)[1])
        except ValueError:
            contract_id = -1
        body = render_contract_detail(storage, current_owner, contract_id, current_user)
        html = layout("Карточка контракта", body, owners, current_owner, "contracts", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    placeholder_routes = {
        "/expenses": (
            "Расходы компании",
            "Отдельный блок для операционных и проектных затрат, чтобы видеть чистую картину денег.",
            [
                "Постоянные расходы компании",
                "Проектные расходы по объектам",
                "Разбивка по категориям",
                "План/факт по затратам",
            ],
            "expenses",
        ),
        "/finance-analysis": (
            "Финансовый анализ",
            "Будущий модуль для общей картины бизнеса: cashflow, маржа, долг, оборачиваемость и сценарии.",
            [
                "План-факт по выручке и оплатам",
                "Долг клиентов и кассовые разрывы",
                "Маржинальность по контрактам",
                "Сводная аналитика по периодам",
            ],
            "finance",
        ),
    }
    if path in placeholder_routes:
        title, subtitle, bullets, section_id = placeholder_routes[path]
        denied = guard(section_id, "view")
        if denied:
            return denied
        body = render_placeholder_section(title, subtitle, bullets)
        html = layout(title, body, owners, current_owner, section_id, current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path == "/payables":
        denied = guard("payables", "view")
        if denied:
            return denied
        body = render_payables_section(storage, current_owner, current_user, current_payables_tab, counterparty_filter=current_payables_counterparty, sort_key=current_payables_sort, sort_order=current_payables_order)
        html = layout("Кредиторка", body, owners, current_owner, "payables", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path == "/payroll":
        denied = guard("payroll", "view")
        if denied:
            return denied
        selected_month = parse_month_key(parse_qs(environ.get("QUERY_STRING", "")).get("month", [""])[0])
        body = render_payroll_section(storage, current_owner, current_user, selected_month)
        html = layout("Зарплата", body, owners, current_owner, "payroll", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
    return [b"Not found"]


def main() -> None:
    host = os.getenv("WEB_HOST", "127.0.0.1")
    port = int(os.getenv("WEB_PORT") or os.getenv("PORT", "8000"))
    with make_server(host, port, app) as server:
        print(f"CRM draft is running on http://{host}:{port}")
        server.serve_forever()


if __name__ == "__main__":
    main()
