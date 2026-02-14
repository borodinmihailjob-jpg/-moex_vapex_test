import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

import miniapp


class _FakeLoansRepo:
    def __init__(self):
        self.next_loan_id = 1
        self.next_event_id = 1
        self.next_actual_payment_id = 1
        self.loans = {}
        self.events = {}
        self.actual_payments = {}
        self.cache = {}
        self.req_index = {}
        self.actual_payment_req_index = {}
        self.share_links = {}
        self.reminders = {}

    async def list_loan_accounts(self, db_dsn, user_id, include_archived=False):
        out = []
        for loan in self.loans.values():
            if loan["user_id"] != user_id:
                continue
            if not include_archived and loan["status"] != "ACTIVE":
                continue
            out.append({k: v for k, v in loan.items() if k != "user_id"})
        out.sort(key=lambda x: x["id"], reverse=True)
        return out

    async def create_loan_account(self, db_dsn, user_id, **kwargs):
        loan_id = self.next_loan_id
        self.next_loan_id += 1
        loan = {
            "id": loan_id,
            "user_id": user_id,
            "name": kwargs.get("name"),
            "principal": format(kwargs["principal"], "f"),
            "current_principal": format(kwargs["current_principal"], "f"),
            "annual_rate": format(kwargs["annual_rate"], "f"),
            "accrual_mode": kwargs.get("accrual_mode", "MONTHLY"),
            "insurance_monthly": format(kwargs.get("insurance_monthly", Decimal("0")), "f"),
            "one_time_costs": format(kwargs.get("one_time_costs", Decimal("0")), "f"),
            "payment_type": kwargs["payment_type"],
            "term_months": kwargs["term_months"],
            "issue_date": kwargs["issue_date"].isoformat() if kwargs.get("issue_date") else None,
            "first_payment_date": kwargs["first_payment_date"].isoformat(),
            "currency": kwargs.get("currency", "RUB"),
            "status": "ACTIVE",
            "created_at": None,
            "updated_at": None,
        }
        self.loans[loan_id] = loan
        return loan_id

    async def get_loan_account(self, db_dsn, user_id, loan_id):
        loan = self.loans.get(loan_id)
        if not loan or loan["user_id"] != user_id:
            return None
        return {k: v for k, v in loan.items() if k != "user_id"}

    async def archive_loan_account(self, db_dsn, user_id, loan_id):
        loan = self.loans.get(loan_id)
        if not loan or loan["user_id"] != user_id:
            return False
        loan["status"] = "ARCHIVED"
        self.cache.pop(loan_id, None)
        return True

    async def list_loan_events(self, db_dsn, user_id, loan_id):
        loan = self.loans.get(loan_id)
        if not loan or loan["user_id"] != user_id:
            return []
        return list(self.events.get(loan_id, []))

    async def create_loan_event(self, db_dsn, user_id, loan_id, *, event_type, event_date, payload, client_request_id=None):
        loan = self.loans.get(loan_id)
        if not loan or loan["user_id"] != user_id:
            return 0, False
        if client_request_id:
            key = (loan_id, client_request_id)
            if key in self.req_index:
                return self.req_index[key], False
        event_id = self.next_event_id
        self.next_event_id += 1
        row = {
            "id": event_id,
            "loan_id": loan_id,
            "user_id": user_id,
            "event_type": event_type,
            "event_date": event_date.isoformat(),
            "payload": payload,
            "client_request_id": client_request_id,
            "created_at": None,
        }
        self.events.setdefault(loan_id, []).append(row)
        self.cache.pop(loan_id, None)
        if client_request_id:
            self.req_index[(loan_id, client_request_id)] = event_id
        return event_id, True

    async def get_loan_schedule_cache(self, db_dsn, user_id, loan_id):
        loan = self.loans.get(loan_id)
        if not loan or loan["user_id"] != user_id:
            return None
        return self.cache.get(loan_id)

    async def upsert_loan_schedule_cache(self, db_dsn, *, loan_id, version, version_hash, summary_json, payload_json):
        self.cache[loan_id] = {
            "loan_id": loan_id,
            "version": version,
            "version_hash": version_hash,
            "summary_json": summary_json,
            "payload_json": payload_json,
            "computed_at": None,
        }

    async def list_loan_actual_payments(self, db_dsn, user_id, loan_id):
        loan = self.loans.get(loan_id)
        if not loan or loan["user_id"] != user_id:
            return []
        return list(self.actual_payments.get(loan_id, []))

    async def create_loan_actual_payment(
        self,
        db_dsn,
        *,
        user_id,
        loan_id,
        payment_date,
        amount,
        principal_paid,
        interest_paid,
        note=None,
        client_request_id=None,
    ):
        loan = self.loans.get(loan_id)
        if not loan or loan["user_id"] != user_id:
            return 0, False
        if client_request_id:
            key = (loan_id, client_request_id)
            if key in self.actual_payment_req_index:
                return self.actual_payment_req_index[key], False
        payment_id = self.next_actual_payment_id
        self.next_actual_payment_id += 1
        row = {
            "id": payment_id,
            "loan_id": loan_id,
            "user_id": user_id,
            "payment_date": payment_date.isoformat(),
            "amount": format(amount, "f"),
            "principal_paid": format(principal_paid, "f"),
            "interest_paid": format(interest_paid, "f"),
            "note": note,
            "client_request_id": client_request_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self.actual_payments.setdefault(loan_id, []).append(row)
        self.cache.pop(loan_id, None)
        if client_request_id:
            self.actual_payment_req_index[(loan_id, client_request_id)] = payment_id
        return payment_id, True

    async def create_loan_share_link(self, db_dsn, *, user_id, loan_id, token, payload, expires_at):
        loan = self.loans.get(loan_id)
        if not loan or loan["user_id"] != user_id:
            return False
        self.share_links[token] = {
            "loan_id": loan_id,
            "user_id": user_id,
            "token": token,
            "payload": payload or {},
            "expires_at": expires_at.isoformat() if expires_at else None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        return True

    async def get_loan_share_link(self, db_dsn, token):
        return self.share_links.get(token)

    async def get_loan_reminder_settings(self, db_dsn, user_id):
        return self.reminders.get(
            user_id,
            {"enabled": False, "days_before": 3, "last_sent_on": None},
        )

    async def set_loan_reminder_settings(self, db_dsn, user_id, *, enabled=None, days_before=None):
        current = await self.get_loan_reminder_settings(db_dsn, user_id)
        next_enabled = current["enabled"] if enabled is None else bool(enabled)
        next_days_before = current["days_before"] if days_before is None else int(days_before)
        next_days_before = max(1, min(30, next_days_before))
        updated = {
            "enabled": next_enabled,
            "days_before": next_days_before,
            "last_sent_on": current.get("last_sent_on"),
        }
        self.reminders[user_id] = updated
        return updated


class LoansApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.repo = _FakeLoansRepo()
        self.app = web.Application()
        miniapp.attach_miniapp_routes(self.app, db_dsn="fake", bot_token="fake")
        self.server = TestServer(self.app)
        self.client = TestClient(self.server)

        self.patches = [
            patch.object(miniapp, "_auth_user_id", autospec=True, return_value=777),
            patch.object(miniapp, "create_loan_account", autospec=True, side_effect=self.repo.create_loan_account),
            patch.object(miniapp, "list_loan_accounts", autospec=True, side_effect=self.repo.list_loan_accounts),
            patch.object(miniapp, "get_loan_account", autospec=True, side_effect=self.repo.get_loan_account),
            patch.object(miniapp, "archive_loan_account", autospec=True, side_effect=self.repo.archive_loan_account),
            patch.object(miniapp, "list_loan_events", autospec=True, side_effect=self.repo.list_loan_events),
            patch.object(miniapp, "create_loan_event", autospec=True, side_effect=self.repo.create_loan_event),
            patch.object(miniapp, "get_loan_schedule_cache", autospec=True, side_effect=self.repo.get_loan_schedule_cache),
            patch.object(miniapp, "upsert_loan_schedule_cache", autospec=True, side_effect=self.repo.upsert_loan_schedule_cache),
            patch.object(miniapp, "list_loan_actual_payments", autospec=True, side_effect=self.repo.list_loan_actual_payments),
            patch.object(miniapp, "create_loan_actual_payment", autospec=True, side_effect=self.repo.create_loan_actual_payment),
            patch.object(miniapp, "create_loan_share_link", autospec=True, side_effect=self.repo.create_loan_share_link),
            patch.object(miniapp, "get_loan_share_link", autospec=True, side_effect=self.repo.get_loan_share_link),
            patch.object(miniapp, "get_loan_reminder_settings", autospec=True, side_effect=self.repo.get_loan_reminder_settings),
            patch.object(miniapp, "set_loan_reminder_settings", autospec=True, side_effect=self.repo.set_loan_reminder_settings),
        ]
        for p in self.patches:
            p.start()

        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()
        for p in reversed(self.patches):
            p.stop()

    async def _create_loan(self):
        payload = {
            "name": "Ипотека • Тест",
            "principal": "3500000.00",
            "current_principal": "3200000.00",
            "annual_rate": "12.90",
            "payment_type": "ANNUITY",
            "term_months": 240,
            "first_payment_date": "2026-03-03",
            "issue_date": "2026-02-10",
            "currency": "RUB",
            "rate_periods": [
                {"start_date": "2026-03-03", "end_date": "2030-12-03", "annual_rate": "12.90"},
                {"start_date": "2030-12-04", "end_date": "2046-02-03", "annual_rate": "10.90"},
            ],
        }
        resp = await self.client.post("/api/miniapp/loans", json=payload)
        self.assertEqual(resp.status, 201)
        data = await resp.json()
        self.assertTrue(data["loan_id"] > 0)
        return int(data["loan_id"])

    async def test_create_get_schedule(self):
        loan_id = await self._create_loan()

        resp = await self.client.get("/api/miniapp/loans")
        self.assertEqual(resp.status, 200)
        rows = (await resp.json())["data"]["items"]
        self.assertEqual(len(rows), 1)

        resp = await self.client.get(f"/api/miniapp/loans/{loan_id}")
        self.assertEqual(resp.status, 200)
        details = (await resp.json())["data"]
        self.assertEqual(details["loan"]["id"], loan_id)
        self.assertGreater(int(details["summary"]["payments_count"]), 0)

        resp = await self.client.get(f"/api/miniapp/loans/{loan_id}/schedule?page=1&page_size=60")
        self.assertEqual(resp.status, 200)
        schedule = (await resp.json())["data"]
        self.assertEqual(schedule["page"], 1)
        self.assertGreater(len(schedule["items"]), 0)

    async def test_corrupted_schedule_cache_is_recomputed(self):
        loan_id = await self._create_loan()

        first = await self.client.get(f"/api/miniapp/loans/{loan_id}")
        self.assertEqual(first.status, 200)
        first_data = (await first.json())["data"]
        self.assertGreater(int(first_data["summary"]["payments_count"]), 0)

        cached = self.repo.cache.get(loan_id)
        self.assertIsNotNone(cached)
        assert cached is not None
        self.repo.cache[loan_id] = {
            "loan_id": loan_id,
            "version": cached["version"],
            "version_hash": cached["version_hash"],
            "summary_json": {},
            "payload_json": [],
            "computed_at": None,
        }

        second = await self.client.get(f"/api/miniapp/loans/{loan_id}")
        self.assertEqual(second.status, 200)
        second_data = (await second.json())["data"]
        self.assertGreater(int(second_data["summary"]["payments_count"]), 0)

    async def test_extra_payment_idempotency(self):
        loan_id = await self._create_loan()

        payload = {
            "date": "2026-03-03",
            "amount": "100000.00",
            "mode": "ONE_TIME",
            "strategy": "REDUCE_TERM",
        }
        headers = {"Idempotency-Key": "abc-123"}
        r1 = await self.client.post(f"/api/miniapp/loans/{loan_id}/events/extra-payment", json=payload, headers=headers)
        r2 = await self.client.post(f"/api/miniapp/loans/{loan_id}/events/extra-payment", json=payload, headers=headers)
        self.assertEqual(r1.status, 200)
        self.assertEqual(r2.status, 200)
        d1 = (await r1.json())["data"]
        d2 = (await r2.json())["data"]
        self.assertEqual(d1["event_id"], d2["event_id"])
        self.assertTrue(d1["created"])
        self.assertFalse(d2["created"])

    async def test_preview_and_tips(self):
        loan_id = await self._create_loan()

        preview_payload = {
            "events": [
                {
                    "type": "EXTRA_PAYMENT",
                    "date": "2026-03-03",
                    "amount": "5000.00",
                    "mode": "MONTHLY",
                    "strategy": "REDUCE_TERM",
                }
            ]
        }
        resp = await self.client.post(f"/api/miniapp/loans/{loan_id}/scenarios/preview", json=preview_payload)
        self.assertEqual(resp.status, 200)
        data = (await resp.json())["data"]
        self.assertIn("base_summary", data)
        self.assertIn("scenario_summary", data)
        self.assertIn("months_diff", data)

        resp = await self.client.get(f"/api/miniapp/loans/{loan_id}/tips")
        self.assertEqual(resp.status, 200)
        tips_data = (await resp.json())["data"]
        self.assertGreaterEqual(len(tips_data["tips"]), 2)

    async def test_validation_error(self):
        bad_payload = {
            "principal": "0",
            "current_principal": "0",
            "annual_rate": "12.90",
            "payment_type": "ANNUITY",
            "term_months": 240,
            "first_payment_date": "2026-03-03",
        }
        resp = await self.client.post("/api/miniapp/loans", json=bad_payload)
        self.assertEqual(resp.status, 400)
        data = await resp.json()
        self.assertEqual(data["error_code"], "VALIDATION_ERROR")

    async def test_actual_payments_and_reminders(self):
        loan_id = await self._create_loan()

        headers = {"Idempotency-Key": "actual-1"}
        payload = {
            "payment_date": "2026-03-03",
            "amount": "45800.00",
            "principal_paid": "10000.00",
            "interest_paid": "35800.00",
            "note": "Платеж внесен",
        }
        r1 = await self.client.post(f"/api/miniapp/loans/{loan_id}/actual-payments", json=payload, headers=headers)
        r2 = await self.client.post(f"/api/miniapp/loans/{loan_id}/actual-payments", json=payload, headers=headers)
        self.assertEqual(r1.status, 200)
        self.assertEqual(r2.status, 200)
        d1 = (await r1.json())["data"]
        d2 = (await r2.json())["data"]
        self.assertEqual(d1["payment_id"], d2["payment_id"])
        self.assertTrue(d1["created"])
        self.assertFalse(d2["created"])

        resp = await self.client.get(f"/api/miniapp/loans/{loan_id}/actual-payments")
        self.assertEqual(resp.status, 200)
        items = (await resp.json())["data"]["items"]
        self.assertEqual(len(items), 1)

        resp = await self.client.get("/api/miniapp/loan-reminders/settings")
        self.assertEqual(resp.status, 200)
        default_settings = (await resp.json())["data"]
        self.assertEqual(default_settings["days_before"], 3)
        self.assertFalse(default_settings["enabled"])

        resp = await self.client.post("/api/miniapp/loan-reminders/settings", json={"enabled": True, "days_before": 5})
        self.assertEqual(resp.status, 200)
        new_settings = (await resp.json())["data"]
        self.assertEqual(new_settings["days_before"], 5)
        self.assertTrue(new_settings["enabled"])

    async def test_refinance_optimizer_export_share(self):
        loan_id = await self._create_loan()

        resp = await self.client.post(
            f"/api/miniapp/loans/{loan_id}/refinance/preview",
            json={"new_annual_rate": "10.90", "refinance_cost": "50000.00"},
        )
        self.assertEqual(resp.status, 200)
        refinance_data = (await resp.json())["data"]
        self.assertIn("base_summary", refinance_data)
        self.assertIn("refinance_summary", refinance_data)
        self.assertIn("total_saving", refinance_data)

        resp = await self.client.post(
            f"/api/miniapp/loans/{loan_id}/optimize",
            json={"goal_type": "PAYMENT_TARGET", "target_payment": "40000"},
        )
        self.assertEqual(resp.status, 200)
        optimize_1 = (await resp.json())["data"]
        self.assertEqual(optimize_1["goal_type"], "PAYMENT_TARGET")

        resp = await self.client.post(
            f"/api/miniapp/loans/{loan_id}/optimize",
            json={"goal_type": "CLOSE_BY_DATE", "target_date": "2035-01-01"},
        )
        self.assertEqual(resp.status, 200)
        optimize_2 = (await resp.json())["data"]
        self.assertEqual(optimize_2["goal_type"], "CLOSE_BY_DATE")

        csv_resp = await self.client.get(f"/api/miniapp/loans/{loan_id}/export?format=csv")
        self.assertEqual(csv_resp.status, 200)
        self.assertIn("text/csv", csv_resp.headers.get("Content-Type", ""))

        pdf_resp = await self.client.get(f"/api/miniapp/loans/{loan_id}/export?format=pdf")
        self.assertEqual(pdf_resp.status, 200)
        self.assertIn("application/pdf", pdf_resp.headers.get("Content-Type", ""))

        share_create = await self.client.post(
            f"/api/miniapp/loans/{loan_id}/share",
            json={"expires_in_days": 7},
        )
        self.assertEqual(share_create.status, 200)
        share_data = (await share_create.json())["data"]
        self.assertTrue(share_data["token"])
        self.assertTrue(share_data["share_url"])

        share_get = await self.client.get(share_data["share_url"])
        self.assertEqual(share_get.status, 200)
        share_payload = (await share_get.json())["data"]
        self.assertEqual(int(share_payload["loan"]["id"]), loan_id)


if __name__ == "__main__":
    unittest.main()
