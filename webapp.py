from __future__ import annotations

import os
import hashlib
import secrets
from datetime import datetime
from datetime import date
from http.cookies import SimpleCookie
from html import escape
from typing import Iterable
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server

from storage import Storage, WEB_SECTION_IDS


STATUS_META = {
    "not_started": ("✖", "Не приступили"),
    "in_progress": ("🛠️", "В работе"),
    "waiting_payment": ("🟡", "Ждем оплату"),
    "paid": ("✅", "Оплачен"),
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
    "pending": ("Ждем итог", "chip warn"),
    "won": ("Выигран", "chip ok"),
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
    ("pending", "Ждем итог"),
    ("won", "Выигран"),
    ("lost", "Проигран"),
    ("rejected", "Заявка отклонена"),
]

MAX_DISCOUNT_OPTIONS = [0.0, 2.5, 5.0, 7.5, 10.0, 12.5, 15.0, 17.5, 20.0, 22.5, 25.0]
SESSION_COOKIE = "felis_session"


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


def auction_min_amount(total_amount: float, discount_percent: float) -> float:
    return round(total_amount * (1 - discount_percent / 100), 2)


def is_auction_archived(item) -> bool:
    if item.submit_decision_status == "rejected":
        return True
    return item.result_status in {"won", "lost", "rejected"}


def status_badge(status: str) -> str:
    emoji, label = STATUS_META.get(status, STATUS_META["not_started"])
    return f'<span class="status-chip">{emoji} {escape(label)}</span>'


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
        overdue_stages = sum(1 for stage in stages if stage.end_date < date.today())
        payload.append(
            {
                "contract": contract,
                "stages": stages,
                "payments": payments,
                "total_amount": total_amount,
                "paid_amount": paid_amount,
                "debt_amount": debt_amount,
                "paid_ratio": paid_ratio,
                "overdue_stages": overdue_stages,
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


def password_state_label(value: str) -> str:
    mapping = {
        "local_only": "Локальный вход без пароля",
        "pending_setup": "Пароль задаст сам",
    }
    return mapping.get(value, value)


def auction_chip(value: str, mapping: dict[str, tuple[str, str]]) -> str:
    label, css = mapping.get(value, ("Неизвестно", "chip"))
    return f'<span class="{css}">{escape(label)}</span>'


def auction_current_chip(field_name: str, current_values: dict[str, str]) -> str:
    if field_name == "estimate_status":
        return auction_chip(current_values[field_name], AUCTION_ESTIMATE_META)
    if field_name == "submit_decision_status":
        return auction_chip(current_values[field_name], AUCTION_SUBMIT_DECISION_META)
    if field_name == "result_status":
        return auction_chip(current_values[field_name], AUCTION_RESULT_META)
    return '<span class="chip">Неизвестно</span>'


def result_summary(item) -> str:
    if item.result_status not in {"won", "lost"} or item.final_bid_amount is None or item.amount <= 0:
        return ""
    discount_percent = max(0.0, round(((item.amount - item.final_bid_amount) / item.amount) * 100, 2))
    return (
        f'<span class="result-meta">Цена: {escape(format_amount(item.final_bid_amount))}</span>'
        f'<span class="result-meta">Снижение: {escape(format_discount_percent(discount_percent))}</span>'
    )


def estimate_summary(item) -> str:
    if item.estimate_status != "calculated" or item.material_cost is None:
        return ""
    return (
        '<span class="result-meta result-meta-stack">'
        '<span>Материалы:</span>'
        f'<span>{escape(format_amount(item.material_cost))}</span>'
        '</span>'
    )


def added_date_meta(item) -> str:
    added_date = item.created_at.date()
    days_since_added = (date.today() - added_date).days
    css_class = "auction-added-date is-new" if days_since_added <= 1 else "auction-added-date"
    return f'<span class="{css_class}">Добавлен: {escape(format_date(added_date))}</span>'


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
    if active_field != "estimate_status" and item.material_cost is not None:
        hidden_inputs.append(
            f'<input type="hidden" name="material_cost" value="{escape(format_amount_input(item.material_cost))}">'
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
      <summary>{auction_current_chip(active_field, current_values)}</summary>
      <form class="status-popover" method="post" action="/auctions/{auction_id}/status?owner={owner_chat_id}&tab={escape(active_tab)}">
        {''.join(hidden_inputs)}
        {''.join(option_buttons)}
      </form>
    </details>
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
    needs_material_cost = current_estimate == "calculated"
    return f"""
    <details class="status-menu result-menu">
      <summary>
        <span class="discount-value">
          {auction_current_chip("estimate_status", current_values)}
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
        <div class="result-helper">Для статуса "Просчитан" укажите стоимость материалов.</div>
        <div class="field estimate-cost-field{" is-hidden" if not needs_material_cost else ""}">
          <label>Стоимость материалов, ₽</label>
          <input type="text" name="material_cost" value="{escape(material_value)}" placeholder="Например, 3 250 000,00" data-money-input="1" {"required" if needs_material_cost else ""}>
          <div class="result-error{" is-visible" if needs_material_cost and not material_value else ""}">Укажите стоимость материалов.</div>
        </div>
        <div class="action-row">
          <button class="submit-btn" type="submit" {"disabled" if needs_material_cost and not material_value else ""}>Сохранить расчет</button>
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
    hidden_inputs = []
    for field_name in ("estimate_status", "submit_decision_status"):
        hidden_inputs.append(
            f'<input type="hidden" name="{field_name}" value="{escape(current_values[field_name])}">'
        )
    if item.material_cost is not None:
        hidden_inputs.append(
            f'<input type="hidden" name="material_cost" value="{escape(format_amount_input(item.material_cost))}">'
        )
    quick_buttons = []
    edit_buttons = []
    current_result = current_values["result_status"]
    for value, label in options:
        chip_class = AUCTION_RESULT_META.get(value, ("", "chip"))[1]
        active_class = " is-active" if current_result == value else ""
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
        helper = '<div class="result-helper">Для статусов "Выигран" и "Проигран" укажите финальную цену аукциона.</div>'
    summary_html = result_summary(item)
    return f"""
    <details class="status-menu result-menu">
      <summary>
        <span class="discount-value">
          {auction_current_chip("result_status", current_values)}
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
    auction_id: int,
    amount: float,
    current_discount: float | None,
    current_min_amount: float | None,
    is_locked: bool,
    is_required: bool,
    active_tab: str,
) -> str:
    current_label = '<span class="discount-placeholder">—</span>'
    percent_value = ""
    min_amount_value = ""
    if current_discount is not None and current_min_amount is not None:
        current_label = (
            f'<span class="discount-percent">{escape(format_discount_percent(current_discount))}</span>'
            f'<span class="discount-amount">{escape(format_amount(current_min_amount))}</span>'
        )
        percent_value = str(current_discount).replace(".", ",")
        min_amount_value = format_amount_input(current_min_amount)
    elif is_required:
        current_label = '<span class="discount-alert">Введите<br>максимальное<br>снижение!</span>'
    if is_locked:
        return '<span class="discount-value"><span class="discount-placeholder">—</span></span>'
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


def render_amount_form(owner_chat_id: int, auction_id: int, current_amount: float, active_tab: str) -> str:
    return f"""
    <details class="status-menu">
      <summary><span class="amount-value">{format_amount(current_amount)}</span></summary>
      <form class="status-popover amount-form" method="post" action="/auctions/{auction_id}/amount?owner={owner_chat_id}&tab={escape(active_tab)}">
        <div class="field">
          <label>Сумма аукциона, ₽</label>
          <input type="text" name="amount" value="{format_amount_input(current_amount)}" data-money-input="1">
        </div>
        <div class="action-row">
          <button class="submit-btn" type="submit">Сохранить сумму</button>
        </div>
      </form>
    </details>
    """


def render_auction_details_form(owner_chat_id: int, item, active_tab: str) -> str:
    source_link = (
        f'<a href="{escape(item.source_url)}" target="_blank" rel="noreferrer">{escape(item.auction_number)}</a>'
        if item.source_url
        else escape(item.auction_number)
    )
    return f"""
    <details class="status-menu lot-menu">
      <summary>
        <div class="auction-number">№ {source_link}</div>
        <div class="timeline-title">{escape(item.title)}</div>
      </summary>
      <form class="status-popover lot-form" method="post" action="/auctions/{item.id}/details?owner={owner_chat_id}&tab={escape(active_tab)}">
        <div class="field">
          <label>Номер аукциона</label>
          <input type="text" name="auction_number" value="{escape(item.auction_number)}" required>
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


def render_auction_row(item, owner_chat_id: int, active_tab: str) -> str:
    current_values = {
        "estimate_status": item.estimate_status,
        "submit_decision_status": item.submit_decision_status,
        "result_status": item.result_status,
    }
    days_left = (item.bid_deadline - date.today()).days
    show_deadline_note = days_left > 0
    is_deadline_danger = (
        item.submit_decision_status != "submitted"
        and item.submit_decision_status == "approved"
        and 0 < days_left <= 2
    )
    if not show_deadline_note:
        deadline_note = ""
    else:
        deadline_note = f"Осталось {days_left} дн."
    return f"""
    <tr id="auction-{item.id}" data-auction-row="{item.id}">
      <td>
        {render_auction_details_form(owner_chat_id, item, active_tab)}
        <div class="auction-lot-meta">
          <span class="contract-meta">{escape(item.city)}</span>
          {added_date_meta(item)}
        </div>
      </td>
      <td class="nowrap">{render_deadline_form(owner_chat_id, item.id, item.bid_deadline, deadline_note, is_deadline_danger, active_tab)}</td>
      <td class="nowrap">{render_amount_form(owner_chat_id, item.id, item.amount, active_tab)}</td>
      <td>{render_estimate_form(owner_chat_id, item, current_values, active_tab)}</td>
      <td>{render_auction_status_form(owner_chat_id, item.id, "submit_decision_status", item, current_values, AUCTION_SUBMIT_OPTIONS, AUCTION_SUBMIT_DECISION_META, active_tab)}</td>
      <td>{render_discount_form(owner_chat_id, item.id, item.amount, item.max_discount_percent, item.min_bid_amount, item.submit_decision_status in {"pending", "rejected"}, item.submit_decision_status == "submitted" and item.max_discount_percent is None, active_tab)}</td>
      <td>{render_result_form(owner_chat_id, item, current_values, active_tab)}</td>
    </tr>
    """


def render_auction_rows(auctions, owner_chat_id: int, active_tab: str) -> str:
    return "".join(render_auction_row(item, owner_chat_id, active_tab) for item in auctions)


SECTIONS = [
    ("contracts", "Контракты", "/contracts"),
    ("auctions", "Аукционы", "/auctions"),
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
    user_panel = (
        f"""
        <div class="current-user-card">
          <div class="current-user-name">{escape(current_user["full_name"])}</div>
          <div class="current-user-email">{escape(current_user["login"])}</div>
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
    .current-user-name {{
      font-size: 15px;
      font-weight: 700;
    }}
    .current-user-email {{
      margin-top: 4px;
      font-size: 12px;
      color: rgba(255,255,255,0.82);
    }}
    .logout-link {{
      display: inline-block;
      margin-top: 10px;
      font-size: 12px;
      border-bottom: 1px dashed rgba(255,255,255,0.45);
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
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 14px;
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
      overflow: hidden;
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
      font-size: 28px;
      font-weight: 700;
      margin-bottom: 6px;
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
    .table th, .table td {{
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: middle;
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
    .permission-box strong {{
      font-size: 14px;
    }}
    .check-row {{
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      font-size: 14px;
      color: var(--muted);
    }}
    .check-row label {{
      display: inline-flex;
      gap: 8px;
      align-items: center;
      cursor: pointer;
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
      color: var(--danger);
      border-color: rgba(184,50,50,0.22);
      background: #fff2f2;
    }}
    .auction-number {{
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 6px;
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
      text-align: left;
      cursor: pointer;
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
    .auction-table td:nth-child(6) .status-popover,
    .auction-table td:nth-child(7) .status-popover {{
      left: auto;
      right: 0;
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
      <div class="sidebar-note">
        Контракты уже рабочие. Доступы тоже начинаем собирать всерьез, чтобы дальше не городить роли поверх хаоса.
      </div>
      <div class="sidebar-note">
        Локальный прототип. Дальше можно вынести это в общий backend и подключить вместе с Telegram.
      </div>
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
      <div class="cta-row">
        <a class="ghost-btn" href="/contracts">Контракты</a>
        {f'<a class="ghost-btn" href="/contracts?owner={current_owner}">Обновить данные</a>' if current_owner is not None else ''}
        <span class="ghost-btn">Следующий шаг: связать разделы в единую систему</span>
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

document.addEventListener("click", (event) => {{
  const estimateQuick = event.target.closest('.estimate-form .estimate-quick');
  if (estimateQuick) {{
    const form = estimateQuick.closest('.estimate-form');
    const hiddenEstimate = form ? form.querySelector('input[name="estimate_status"]') : null;
    const costInput = form ? form.querySelector('input[name="material_cost"]') : null;
    if (hiddenEstimate) {{
      hiddenEstimate.value = estimateQuick.dataset.estimateValue || "";
    }}
    if (costInput) {{
      costInput.value = "";
    }}
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
  const costField = form.querySelector('.estimate-cost-field');
  const costInput = form.querySelector('input[name="material_cost"]');
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
  if (costField) {{
    costField.classList.toggle('is-hidden', !needsCost);
  }}
  if (costInput) {{
    costInput.required = Boolean(needsCost);
    if (!needsCost) {{
      costInput.value = "";
    }} else {{
      costInput.value = "";
      window.setTimeout(() => costInput.focus(), 0);
    }}
  }}
  if (errorBox) {{
    errorBox.classList.toggle('is-visible', Boolean(needsCost));
  }}
  if (submitButton) {{
    submitButton.disabled = Boolean(needsCost);
  }}
}});

document.addEventListener("submit", (event) => {{
  if (event.target.closest(".discount-form, .amount-form, .deadline-form, .lot-form, .result-form, .estimate-form")) {{
    window.sessionStorage.setItem("auctionScrollY", String(window.scrollY));
  }}
}});

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
  const materialInput = event.target.closest('.estimate-form input[name="material_cost"]');
  if (materialInput) {{
    const form = materialInput.closest('.estimate-form');
    const submitButton = form ? form.querySelector('button[type="submit"]') : null;
    const selected = form ? form.querySelector('.estimate-form input[name="estimate_status"]') : null;
    const errorBox = form ? form.querySelector('.result-error') : null;
    const needsCost = selected && selected.value === "calculated";
    const cleaned = materialInput.value.replace(/\\s+/g, "").replace(",", ".").trim();
    const hasValue = cleaned !== "" && Number(cleaned) > 0;
    if (submitButton) {{
      submitButton.disabled = Boolean(needsCost && !hasValue);
    }}
    if (errorBox) {{
      errorBox.classList.toggle('is-visible', Boolean(needsCost && !hasValue));
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

window.addEventListener("load", () => {{
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
    overdue_total = sum(item["overdue_stages"] for item in payload)
    upcoming = storage.upcoming_items(owner_chat_id, within_days=30)
    paid_ratio = (total_paid / total_amount) if total_amount > 0 else 0.0

    stats_html = f"""
    <section class="stats">
      <article class="card stat-card">
        <div class="stat-label">Контрактов</div>
        <div class="stat-value">{len(payload)}</div>
        <div class="stat-note">В активном контуре</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Общий бюджет</div>
        <div class="stat-value">{format_amount(total_amount)}</div>
        <div class="stat-note">Сумма всех этапов</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Оплачено</div>
        <div class="stat-value">{format_amount(total_paid)}</div>
        <div class="stat-note">{format_percent(paid_ratio * 100)} от общего объема</div>
      </article>
      <article class="card stat-card">
        <div class="stat-label">Долг заказчиков</div>
        <div class="stat-value">{format_amount(total_debt)}</div>
        <div class="stat-note">Просроченных этапов: {overdue_total}</div>
      </article>
    </section>
    """

    contracts_html_parts = []
    for item in payload:
        contract = item["contract"]
        progress_width = round(item["paid_ratio"] * 100, 1)
        contracts_html_parts.append(
            f"""
            <a class="contract-item" href="/contracts/{contract.id}?owner={owner_chat_id}">
              <div class="contract-top">
                <div>
                  <div class="contract-name">{escape(contract.title)}</div>
                  <div class="contract-meta">
                    Дедлайн: {format_date(contract.end_date)}<br>
                    {escape(contract.description) if contract.description else 'Описание пока не заполнено'}
                  </div>
                </div>
                <div class="contract-money">
                  <div class="money-big">{format_amount(item["total_amount"])}</div>
                  <div class="contract-meta">Оплачено {format_amount(item["paid_amount"])}</div>
                </div>
              </div>
              <div class="progress-wrap">
                <div class="progress-track"><div class="progress-bar" style="width:{progress_width}%"></div></div>
                <div class="progress-meta">
                  <span>{format_percent(progress_width)} оплачено</span>
                  <span>Долг {format_amount(item["debt_amount"])}</span>
                </div>
              </div>
              <div class="info-row">
                <span class="chip">Этапов: {len(item["stages"])}</span>
                <span class="chip">Оплат: {len(item["payments"])}</span>
                <span class="chip{' danger' if item['overdue_stages'] else ''}">Просрочек: {item['overdue_stages']}</span>
              </div>
            </a>
            """
        )
    contracts_html = "".join(contracts_html_parts) or '<div class="empty">Контракты пока не добавлены.</div>'

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
    <section class="grid">
      <section class="card panel">
        <div class="panel-head">
          <div>
            <h2 class="panel-title">Контракты</h2>
            <div class="panel-sub">Карточки уже выглядят как основа CRM. Следом можно добавить формы создания и редактирования.</div>
          </div>
          <div class="chip">Текущий владелец: {owner_chat_id}</div>
        </div>
        <div class="contract-list">{contracts_html}</div>
      </section>
      <aside class="section-stack">
        <section class="card panel">
          <div class="panel-head">
            <div>
              <h2 class="panel-title">Новый контракт</h2>
              <div class="panel-sub">Первый рабочий ввод уже из браузера</div>
            </div>
          </div>
          <form class="form-grid" method="post" action="/contracts/new?owner={owner_chat_id}">
            <div class="field">
              <label>Название</label>
              <input type="text" name="title" required>
            </div>
            <div class="field">
              <label>Описание</label>
              <textarea name="description" placeholder="Кратко о контракте"></textarea>
            </div>
            <div class="field">
              <label>Общий дедлайн</label>
              <input type="text" name="end_date" placeholder="31-07-2026" required>
            </div>
            <button class="submit-btn" type="submit">Создать контракт</button>
          </form>
        </section>
        <section class="card panel">
          <div class="panel-head">
            <div>
              <h2 class="panel-title">Ближайшие сроки</h2>
              <div class="panel-sub">То, что сейчас уже напоминает бот, здесь может стать рабочим календарем.</div>
            </div>
          </div>
          <div class="timeline">{upcoming_html}</div>
        </section>
        <section class="card panel">
          <div class="panel-head">
            <div>
              <h2 class="panel-title">Что дальше</h2>
              <div class="panel-sub">Самый логичный следующий слой для CRM</div>
            </div>
          </div>
          <div class="contract-meta">
            1. Формы ввода контрактов, этапов и оплат<br>
            2. Авторизация и роли сотрудников<br>
            3. Фильтры по долгам, срокам и статусам<br>
            4. Событийная лента и комментарии<br>
            5. Интеграция Telegram-бота с тем же backend
          </div>
        </section>
      </aside>
    </section>
    """


def render_contract_detail(storage: Storage, owner_chat_id: int, contract_id: int, flash_message: str = "") -> str:
    payload = next((item for item in contract_payload(storage, owner_chat_id) if item["contract"].id == contract_id), None)
    if payload is None:
        return '<div class="card panel"><div class="empty">Контракт не найден.</div></div>'

    contract = payload["contract"]
    stages_html = "".join(
        f"""
        <tr>
          <td>{escape(stage.name)}</td>
          <td>{status_badge(stage.status)}</td>
          <td>{format_date(stage.end_date)}</td>
          <td>{format_amount(stage.amount)}</td>
          <td>{escape(stage.notes) if stage.notes else '—'}</td>
        </tr>
        """
        for stage in payload["stages"]
    ) or '<tr><td colspan="5">Этапов пока нет.</td></tr>'

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
    <div class="detail-hero">
      <section class="detail-box">
        <div class="eyebrow">Карточка контракта</div>
        <h1 style="font-size:40px; margin-bottom:12px;">{escape(contract.title)}</h1>
        <div class="hero-copy">
          {escape(contract.description) if contract.description else 'Описание пока не заполнено. В следующем шаге можно сделать редактирование прямо из web-интерфейса.'}
        </div>
        <div class="info-row" style="margin-top:18px;">
          <span class="chip">ID: {contract.id}</span>
          <span class="chip">Дедлайн: {format_date(contract.end_date)}</span>
          <span class="chip">Этапов: {len(payload["stages"])}</span>
          <span class="chip">Оплат: {len(payload["payments"])}</span>
        </div>
      </section>
      <aside class="detail-side">
        <div class="mini-card">
          <div class="stat-label">Общая сумма</div>
          <div class="mini-value">{format_amount(payload["total_amount"])}</div>
        </div>
        <div class="mini-card">
          <div class="stat-label">Оплачено</div>
          <div class="mini-value">{format_amount(payload["paid_amount"])}</div>
          <div class="stat-note">{format_percent(payload["paid_ratio"] * 100)} от общего бюджета</div>
        </div>
        <div class="mini-card">
          <div class="stat-label">Долг</div>
          <div class="mini-value">{format_amount(payload["debt_amount"])}</div>
        </div>
      </aside>
    </div>
    {flash_html}
    <section class="grid" style="margin-top:22px;">
      <section class="card panel">
        <div class="panel-head">
          <div>
            <h2 class="panel-title">Этапы контракта</h2>
            <div class="panel-sub">Здесь дальше удобно сделать inline-редактирование статусов, дедлайнов и сумм.</div>
          </div>
          <a class="chip" href="/?owner={owner_chat_id}">← Назад к дашборду</a>
        </div>
        <table class="table">
          <thead>
            <tr>
              <th>Этап</th>
              <th>Статус</th>
              <th>Дедлайн</th>
              <th>Сумма</th>
              <th>Примечание</th>
            </tr>
          </thead>
          <tbody>{stages_html}</tbody>
        </table>
      </section>
      <aside class="section-stack">
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
        <section class="card panel">
          <div class="panel-head">
            <div>
              <h2 class="panel-title">Добавить этап</h2>
              <div class="panel-sub">Быстрый ввод следующего этапа</div>
            </div>
          </div>
          <form class="form-grid" method="post" action="/contracts/{contract.id}/stages/new?owner={owner_chat_id}">
            <div class="field">
              <label>Номер этапа</label>
              <input type="number" min="1" name="position" value="{len(payload["stages"]) + 1}" required>
            </div>
            <div class="field">
              <label>Дедлайн этапа</label>
              <input type="text" name="end_date" placeholder="31-07-2026" required>
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
        </section>
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
        <section class="card panel">
          <div class="panel-head">
            <div>
              <h2 class="panel-title">Что можно докрутить</h2>
              <div class="panel-sub">Уже следующим шагом</div>
            </div>
          </div>
          <div class="contract-meta">
            • Создание оплаты и этапа прямо из браузера<br>
            • Редактирование статуса этапа в один клик<br>
            • История изменений по контракту<br>
            • Отдельная страница должников и просрочек
          </div>
        </section>
      </aside>
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
        <div class="stat-value">BigBoss</div>
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
        setup_block = f"""
        <div class="field">
          <label>Ссылка для установки пароля</label>
          <div class="copy-field">
            <input id="setup-link-{user["id"]}" type="text" value="{escape(setup_link)}" readonly>
            <button class="copy-btn" type="button" onclick="copyText('setup-link-{user["id"]}')" title="Скопировать ссылку">⧉</button>
          </div>
        </div>
        """
        reset_button = f"""
        <button class="secondary-btn" type="submit" formaction="/access/users/{user["id"]}/reset-password?owner={owner_chat_id}" formmethod="post">
          Сбросить пароль и обновить ссылку
        </button>
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
                {setup_block}
                <div class="permissions-grid">{''.join(permission_boxes)}</div>
                {action_buttons}
                <div class="action-row">{reset_button}</div>
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
          • Вы остаетесь главным администратором BigBoss<br>
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
    active_tab: str = "active",
    flash_message: str = "",
    success: bool = False,
) -> str:
    storage.ensure_demo_auctions(owner_chat_id)
    all_auctions = storage.list_auctions(owner_chat_id)
    active_auctions = [item for item in all_auctions if not is_auction_archived(item)]
    archived_auctions = [item for item in all_auctions if is_auction_archived(item)]
    active_auctions.sort(key=lambda item: (item.bid_deadline, item.id))
    archived_auctions.sort(
        key=lambda item: (
            item.archived_at or item.created_at,
            item.id,
        ),
        reverse=True,
    )
    auctions = archived_auctions if active_tab == "archive" else active_auctions
    total_amount = sum(item.amount for item in active_auctions)
    estimate_count = sum(1 for item in active_auctions if item.estimate_status == "approved")
    submit_decision_count = sum(1 for item in active_auctions if item.submit_decision_status == "approved")
    submitted_count = sum(1 for item in all_auctions if item.submit_decision_status == "submitted")
    won_count = sum(1 for item in all_auctions if item.result_status == "won")

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

    rows_html = render_auction_rows(auctions, owner_chat_id, active_tab)
    flash_html = f'<div class="flash{" ok" if success else ""}">{escape(flash_message)}</div>' if flash_message else ""
    tab_links = f"""
    <div class="tab-row">
      <a class="tab-btn{" active" if active_tab == "active" else ""}" href="/auctions?owner={owner_chat_id}&tab=active#auction-registry">
        В работе
        <span class="tab-count">{len(active_auctions)}</span>
      </a>
      <a class="tab-btn{" active" if active_tab == "archive" else ""}" href="/auctions?owner={owner_chat_id}&tab=archive#auction-registry">
        Архив
        <span class="tab-count">{len(archived_auctions)}</span>
      </a>
    </div>
    """
    empty_message = (
        "Сейчас в работе нет аукционов. Все завершенные или отклоненные лоты уже ушли в архив."
        if active_tab == "active"
        else "Архив пока пуст. Как только по аукциону будет итог или решение не подавать, он окажется здесь."
    )
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
            <input type="text" name="auction_number" placeholder="0145300000126000011" required>
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
          • После статуса «Заявка подана» доступны варианты: «Ждем итог», «Выигран», «Проигран», «Заявка отклонена»
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
    if current_user.get("is_super_admin"):
        return True
    permissions = current_user.get("permissions", {}).get(section_id, {})
    return bool(permissions.get("can_edit" if mode == "edit" else "can_view"))


def render_auth_body(storage: Storage, flash_message: str = "", setup_message: str = "") -> str:
    hint = storage.auth_hint_user()
    hint_html = ""
    if hint is not None:
        hint_html = (
            f"<div class=\"auth-note\">Админ по умолчанию сейчас: <strong>{escape(hint['full_name'])}</strong><br>"
            f"Логин: {escape(hint['login'])}</div>"
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
            <input type="text" name="login" placeholder="bigboss" required>
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
    if current_auction_tab not in {"active", "archive"}:
        current_auction_tab = "active"
    cookies = parse_cookies(environ)
    current_user = storage.get_web_user_by_session(cookies.get(SESSION_COOKIE, ""))
    current_owner = current_user["owner_chat_id"] if current_user else None
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
        body = render_auth_body(storage, "", "Пароль создан. Теперь можно войти.")
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
        return clear_session_cookie(start_response, "/contracts")

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
            end_date = parse_date(form["end_date"])
            if not title:
                raise ValueError("Название контракта обязательно")
            contract_id = storage.add_contract(current_owner, title, description, end_date)
            return redirect(start_response, f"/contracts/{contract_id}?owner={current_owner}")
        except Exception as exc:
            body = render_dashboard(storage, current_owner) + f'<div class="flash">Не удалось создать контракт: {escape(str(exc))}</div>'
            html = layout("CRM Draft", body, owners, current_owner, "contracts", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path in ("/", "/contracts"):
        denied = guard("contracts", "view")
        if denied:
            return denied
        body = render_dashboard(storage, current_owner)
        html = layout("CRM Draft", body, owners, current_owner, "contracts", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path == "/auctions/new" and method == "POST":
        denied = guard("auctions", "edit")
        if denied:
            return denied
        form = read_post_data(environ)
        try:
            auction_number = form["auction_number"].strip()
            title = form["title"].strip()
            city = form["city"].strip()
            bid_deadline = parse_date(form["bid_deadline"])
            amount = parse_amount(form["amount"])
            source_url = form.get("source_url", "").strip()
            if not auction_number:
                raise ValueError("Нужно указать номер аукциона")
            if not title:
                raise ValueError("Нужно указать название аукциона")
            if not city:
                raise ValueError("Нужно указать город")
            storage.add_auction(current_owner, auction_number, bid_deadline, amount, title, city, source_url)
            body = render_auctions_section(storage, current_owner, current_auction_tab, "Аукцион добавлен в реестр.", True)
        except Exception as exc:
            body = render_auctions_section(storage, current_owner, current_auction_tab, f"Не удалось добавить аукцион: {exc}")
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
            submit_decision_status = form.get("submit_decision_status", auction.submit_decision_status)
            result_status = form.get("result_status", auction.result_status)
            final_bid_amount = auction.final_bid_amount
            if estimate_status == "calculated":
                material_cost = parse_optional_number(form.get("material_cost", ""))
                if material_cost is None:
                    raise ValueError("Укажите стоимость материалов")
                if material_cost <= 0:
                    raise ValueError("Стоимость материалов должна быть больше 0")
            else:
                material_cost = None
            application_status = "submitted" if submit_decision_status == "submitted" else "not_submitted"
            if submit_decision_status != "submitted":
                result_status = "not_participated"
                final_bid_amount = None
            elif result_status == "not_participated":
                result_status = "pending"
                final_bid_amount = None
            elif result_status in {"won", "lost"}:
                final_bid_amount = parse_optional_number(form.get("final_bid_amount", ""))
                if final_bid_amount is None:
                    raise ValueError("Укажите сумму, за которую выиграли аукцион")
                if final_bid_amount <= 0 or final_bid_amount > auction.amount:
                    raise ValueError("Итоговая цена должна быть больше 0 и не превышать сумму аукциона")
            else:
                final_bid_amount = None
            was_archived = is_auction_archived(auction)
            will_be_archived = (
                submit_decision_status == "rejected"
                or result_status in {"won", "lost", "rejected"}
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
                submit_decision_status,
                application_status,
                result_status,
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
            body = render_auctions_section(storage, current_owner, current_auction_tab, f"Не удалось обновить статус аукциона: {exc}")
        html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
        return [html.encode("utf-8")]

    if path.startswith("/auctions/") and path.endswith("/discount") and method == "POST":
        denied = guard("auctions", "edit")
        if denied:
            return denied
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
            updated = storage.update_auction_max_discount(current_owner, auction_id, max_discount_percent, min_bid_amount)
            if not updated:
                raise ValueError("Аукцион не найден")
            target_tab = "archive" if is_auction_archived(auction) else current_auction_tab
            return redirect(start_response, f"/auctions?owner={current_owner}&tab={target_tab}")
        except Exception as exc:
            body = render_auctions_section(storage, current_owner, current_auction_tab, f"Не удалось обновить максимальное снижение: {exc}")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/auctions/") and path.endswith("/amount") and method == "POST":
        denied = guard("auctions", "edit")
        if denied:
            return denied
        form = read_post_data(environ)
        try:
            auction_id = int(path.split("/")[2])
            auction = next((item for item in storage.list_auctions(current_owner) if item.id == auction_id), None)
            if auction is None:
                raise ValueError("Аукцион не найден")
            amount = parse_amount(form["amount"])
            if auction.min_bid_amount is not None and amount < auction.min_bid_amount:
                raise ValueError("Сумма аукциона не может быть меньше уже заданной минимальной суммы")
            recalculated_discount = auction.max_discount_percent
            if auction.min_bid_amount is not None and amount > 0:
                recalculated_discount = round(((amount - auction.min_bid_amount) / amount) * 100, 2)
            updated = storage.update_auction_amount(current_owner, auction_id, amount, recalculated_discount)
            if not updated:
                raise ValueError("Аукцион не найден")
            target_tab = "archive" if is_auction_archived(auction) else current_auction_tab
            return redirect(start_response, f"/auctions?owner={current_owner}&tab={target_tab}")
        except Exception as exc:
            body = render_auctions_section(storage, current_owner, current_auction_tab, f"Не удалось обновить сумму аукциона: {exc}")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/auctions/") and path.endswith("/details") and method == "POST":
        denied = guard("auctions", "edit")
        if denied:
            return denied
        form = read_post_data(environ)
        try:
            auction_id = int(path.split("/")[2])
            auction = next((item for item in storage.list_auctions(current_owner) if item.id == auction_id), None)
            if auction is None:
                raise ValueError("Аукцион не найден")
            auction_number = form.get("auction_number", "").strip()
            title = form.get("title", "").strip()
            city = form.get("city", "").strip()
            source_url = form.get("source_url", "").strip()
            created_date = parse_date(form.get("created_date", ""))
            if not auction_number:
                raise ValueError("Нужно указать номер аукциона")
            if not title:
                raise ValueError("Нужно указать название аукциона")
            if not city:
                raise ValueError("Нужно указать город")
            updated = storage.update_auction_details(current_owner, auction_id, auction_number, title, city, source_url, created_date)
            if not updated:
                raise ValueError("Аукцион не найден")
            target_tab = "archive" if is_auction_archived(auction) else current_auction_tab
            return redirect(start_response, f"/auctions?owner={current_owner}&tab={target_tab}")
        except Exception as exc:
            body = render_auctions_section(storage, current_owner, current_auction_tab, f"Не удалось обновить данные аукциона: {exc}")
            html = layout("Аукционы", body, owners, current_owner, "auctions", current_user)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]

    if path.startswith("/auctions/") and path.endswith("/deadline") and method == "POST":
        denied = guard("auctions", "edit")
        if denied:
            return denied
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
            body = render_auctions_section(storage, current_owner, current_auction_tab, f"Не удалось обновить дату подачи: {exc}")
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
            html = render_auction_row(auction, current_owner, current_auction_tab)
            start_response("200 OK", [("Content-Type", "text/html; charset=utf-8")])
            return [html.encode("utf-8")]
        except Exception:
            start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
            return [b"Not found"]

    if path == "/auctions/table" and method == "GET":
        denied = guard("auctions", "view")
        if denied:
            return denied
        auctions = storage.list_auctions(current_owner)
        html = render_auction_rows(auctions, current_owner, current_auction_tab)
        start_response(
            "200 OK",
            [
                ("Content-Type", "text/html; charset=utf-8"),
                ("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0"),
                ("Pragma", "no-cache"),
            ],
        )
        return [html.encode("utf-8")]

    if path == "/auctions":
        denied = guard("auctions", "view")
        if denied:
            return denied
        body = render_auctions_section(storage, current_owner, current_auction_tab)
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
            body = render_access_section(
                storage,
                current_owner,
                request_base_url(environ),
                "Пользователь добавлен. Скопируйте ниже ссылку установки пароля и отправьте сотруднику.",
                True,
            )
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
            body = render_access_section(
                storage,
                current_owner,
                request_base_url(environ),
                f"Новая ссылка установки пароля готова для пользователя «{user['full_name']}».",
                True,
            )
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
            body = render_access_section(storage, current_owner, request_base_url(environ), "Права пользователя обновлены.", True)
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
            body = render_access_section(storage, current_owner, request_base_url(environ), "Статус доступа обновлен.", True)
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
            body = render_access_section(storage, current_owner, request_base_url(environ), "Пользователь удален.", True)
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
            body = render_contract_detail(storage, current_owner, contract_id, f"Не удалось добавить этап: {exc}")
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
            body = render_contract_detail(storage, current_owner, contract_id, f"Не удалось добавить оплату: {exc}")
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
        body = render_contract_detail(storage, current_owner, contract_id)
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
        "/payroll": (
            "Зарплата",
            "Позже сюда можно вынести сотрудников, начисления, авансы и привязку к объектам.",
            [
                "Список сотрудников и ролей",
                "Начисления и выплаты",
                "Привязка к контрактам и объектам",
                "История выплат и задолженностей",
            ],
            "payroll",
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
