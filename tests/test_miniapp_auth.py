import hashlib
import hmac
import json
import unittest
from urllib.parse import urlencode

from miniapp_auth import MiniAppAuthError, parse_and_validate_init_data


def make_init_data(bot_token: str, user_payload: dict) -> str:
    fields = {
        "auth_date": "1700000000",
        "query_id": "AAEAAAE",
        "user": json.dumps(user_payload, separators=(",", ":")),
    }
    data_check = "\n".join(f"{k}={v}" for k, v in sorted(fields.items()))
    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    fields["hash"] = hmac.new(secret_key, data_check.encode("utf-8"), hashlib.sha256).hexdigest()
    return urlencode(fields)


class MiniAppAuthTests(unittest.TestCase):
    def test_validate_init_data_ok(self):
        token = "123456:ABCDEF"
        init_data = make_init_data(token, {"id": 777, "username": "tester"})
        user = parse_and_validate_init_data(token, init_data)
        self.assertEqual(user["id"], 777)

    def test_validate_init_data_bad_hash(self):
        token = "123456:ABCDEF"
        init_data = make_init_data(token, {"id": 777}) + "x"
        with self.assertRaises(MiniAppAuthError):
            parse_and_validate_init_data(token, init_data)


if __name__ == "__main__":
    unittest.main()
