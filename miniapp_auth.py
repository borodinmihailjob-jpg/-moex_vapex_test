from __future__ import annotations

import hashlib
import hmac
import json
import time
from urllib.parse import parse_qsl


class MiniAppAuthError(Exception):
    pass


def _parse_init_data(init_data: str) -> dict[str, str]:
    pairs = dict(parse_qsl(init_data, keep_blank_values=True, strict_parsing=False))
    return {str(k): str(v) for k, v in pairs.items()}


def _calc_webapp_hash(bot_token: str, fields: dict[str, str]) -> str:
    data_check_arr = [f"{k}={v}" for k, v in sorted(fields.items()) if k != "hash"]
    data_check_string = "\n".join(data_check_arr)
    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    return hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()


def parse_and_validate_init_data(
    bot_token: str,
    init_data: str,
    *,
    max_age_seconds: int | None = 24 * 60 * 60,
    now_ts: int | None = None,
) -> dict:
    fields = _parse_init_data(init_data)
    got_hash = fields.get("hash")
    if not got_hash:
        raise MiniAppAuthError("initData hash is missing")
    expected = _calc_webapp_hash(bot_token, fields)
    if not hmac.compare_digest(got_hash, expected):
        raise MiniAppAuthError("initData hash mismatch")
    auth_date_raw = (fields.get("auth_date") or "").strip()
    if not auth_date_raw:
        raise MiniAppAuthError("initData auth_date is missing")
    try:
        auth_ts = int(auth_date_raw)
    except ValueError as exc:
        raise MiniAppAuthError("initData auth_date is invalid") from exc
    if auth_ts <= 0:
        raise MiniAppAuthError("initData auth_date is invalid")
    now_value = int(time.time()) if now_ts is None else int(now_ts)
    if auth_ts - now_value > 60:
        raise MiniAppAuthError("initData auth_date is in the future")
    if max_age_seconds is not None and now_value - auth_ts > max_age_seconds:
        raise MiniAppAuthError("initData is too old")
    user_raw = fields.get("user")
    if not user_raw:
        raise MiniAppAuthError("initData user is missing")
    try:
        return json.loads(user_raw)
    except json.JSONDecodeError as exc:
        raise MiniAppAuthError("initData user is invalid JSON") from exc
