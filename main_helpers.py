from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse, urlunparse

from moex_iss import ASSET_TYPE_FIAT, ASSET_TYPE_METAL


def normalize_miniapp_url(url: str | None) -> str | None:
    if not url:
        return None
    value = url.strip()
    if not value:
        return None
    parsed = urlparse(value)
    if not parsed.scheme or not parsed.netloc:
        return value.rstrip("/")
    path = parsed.path or ""
    if path in ("", "/"):
        path = "/miniapp"
    elif path == "/miniapp/":
        path = "/miniapp"
    return urlunparse(parsed._replace(path=path))


def ru_weekday_short(d: date) -> str:
    names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    return names[d.weekday()]


def top_movers_date_options(base_date: date) -> list[tuple[str, date]]:
    return [
        ("Текущая", base_date),
        ("Вчера", base_date - timedelta(days=1)),
        ("Позавчера", base_date - timedelta(days=2)),
    ]


def article_button_text(button_name: str, text_code: str) -> str:
    raw = str(button_name or "").strip()
    if raw:
        return raw[:64]
    raw = str(text_code or "").strip()
    if not raw:
        return "Статья"
    label = raw.replace("_", " ").replace("-", " ").strip().title()
    return label[:64]


def alert_query_prompt(asset_type: str) -> str:
    if asset_type == ASSET_TYPE_METAL:
        return "Введи тикер или название металла (например: GLDRUB_TOM):"
    if asset_type == ASSET_TYPE_FIAT:
        return "Введи валюту или тикер пары (например: доллар, USD000UTSTOM):"
    return "Введи тикер, ISIN или название компании:"


def parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        raw = value
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None
