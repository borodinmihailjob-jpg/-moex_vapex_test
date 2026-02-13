from __future__ import annotations

import hashlib
import hmac
import json
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


def parse_and_validate_init_data(bot_token: str, init_data: str) -> dict:
    fields = _parse_init_data(init_data)
    got_hash = fields.get("hash")
    if not got_hash:
        raise MiniAppAuthError("initData hash is missing")
    expected = _calc_webapp_hash(bot_token, fields)
    if not hmac.compare_digest(got_hash, expected):
        raise MiniAppAuthError("initData hash mismatch")
    user_raw = fields.get("user")
    if not user_raw:
        raise MiniAppAuthError("initData user is missing")
    try:
        return json.loads(user_raw)
    except json.JSONDecodeError as exc:
        raise MiniAppAuthError("initData user is invalid JSON") from exc
