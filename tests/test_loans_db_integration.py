import os
import time
import unittest
from datetime import date
from decimal import Decimal

import asyncpg
from dotenv import load_dotenv

from db import (
    create_loan_account,
    create_loan_event,
    get_loan_account,
    get_loan_schedule_cache,
    init_db,
    list_loan_accounts,
    list_loan_events,
    upsert_loan_schedule_cache,
)


class LoansDbIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        load_dotenv()
        self.db_dsn = (
            os.getenv("TEST_DATABASE_URL")
            or os.getenv("DATABASE_URL")
            or os.getenv("DB_DSN")
            or ""
        ).strip()
        if not self.db_dsn:
            self.skipTest("DATABASE_URL/TEST_DATABASE_URL/DB_DSN is not set")

        self.user_id = int(f"97{int(time.time() * 1000) % 10**11:011d}")
        self.other_user_id = self.user_id + 1
        await init_db(self.db_dsn)

    async def asyncTearDown(self):
        if not self.db_dsn:
            return
        conn = await asyncpg.connect(dsn=self.db_dsn)
        try:
            await conn.execute("DELETE FROM loan_accounts WHERE user_id = $1", self.user_id)
            await conn.execute("DELETE FROM loan_accounts WHERE user_id = $1", self.other_user_id)
            await conn.execute("DELETE FROM users WHERE telegram_user_id = $1", self.user_id)
            await conn.execute("DELETE FROM users WHERE telegram_user_id = $1", self.other_user_id)
        finally:
            await conn.close()

    async def test_create_list_get_and_event_idempotency_with_cache_invalidation(self):
        loan_id = await create_loan_account(
            self.db_dsn,
            self.user_id,
            name="Integration Loan",
            principal=Decimal("1000000.00"),
            annual_rate=Decimal("12.50"),
            payment_type="ANNUITY",
            term_months=120,
            first_payment_date=date(2026, 3, 3),
            issue_date=date(2026, 2, 10),
            currency="RUB",
        )
        self.assertGreater(loan_id, 0)

        rows = await list_loan_accounts(self.db_dsn, self.user_id)
        self.assertTrue(any(int(x["id"]) == loan_id for x in rows))

        loan = await get_loan_account(self.db_dsn, self.user_id, loan_id)
        self.assertIsNotNone(loan)
        assert loan is not None
        self.assertEqual(int(loan["id"]), loan_id)
        self.assertEqual(loan["payment_type"], "ANNUITY")

        # First insert creates event.
        event_id_1, created_1 = await create_loan_event(
            self.db_dsn,
            self.user_id,
            loan_id,
            event_type="EXTRA_PAYMENT",
            event_date=date(2026, 4, 3),
            payload={"amount": "50000.00", "mode": "ONE_TIME", "strategy": "REDUCE_TERM"},
            client_request_id="req-integration-1",
        )
        self.assertTrue(created_1)
        self.assertGreater(event_id_1, 0)

        # Second insert with same idempotency key must be deduped.
        event_id_2, created_2 = await create_loan_event(
            self.db_dsn,
            self.user_id,
            loan_id,
            event_type="EXTRA_PAYMENT",
            event_date=date(2026, 4, 3),
            payload={"amount": "50000.00", "mode": "ONE_TIME", "strategy": "REDUCE_TERM"},
            client_request_id="req-integration-1",
        )
        self.assertFalse(created_2)
        self.assertEqual(event_id_1, event_id_2)

        events = await list_loan_events(self.db_dsn, self.user_id, loan_id)
        self.assertEqual(len(events), 1)

        # Put cache manually, then adding a new event should invalidate it.
        await upsert_loan_schedule_cache(
            self.db_dsn,
            loan_id=loan_id,
            version=1,
            version_hash="hash-1",
            summary_json={"payments_count": 120},
            payload_json=[{"date": "2026-03-03", "payment": "10000.00"}],
        )
        cache_before = await get_loan_schedule_cache(self.db_dsn, self.user_id, loan_id)
        self.assertIsNotNone(cache_before)

        _, created_3 = await create_loan_event(
            self.db_dsn,
            self.user_id,
            loan_id,
            event_type="RATE_CHANGE",
            event_date=date(2026, 9, 3),
            payload={"annual_rate": "10.90"},
            client_request_id="req-integration-2",
        )
        self.assertTrue(created_3)

        cache_after = await get_loan_schedule_cache(self.db_dsn, self.user_id, loan_id)
        self.assertIsNone(cache_after)

    async def test_owner_isolation(self):
        loan_id = await create_loan_account(
            self.db_dsn,
            self.user_id,
            name="Isolation Loan",
            principal=Decimal("500000.00"),
            annual_rate=Decimal("10.00"),
            payment_type="ANNUITY",
            term_months=60,
            first_payment_date=date(2026, 3, 3),
            issue_date=date(2026, 2, 10),
            currency="RUB",
        )

        foreign_view = await get_loan_account(self.db_dsn, self.other_user_id, loan_id)
        self.assertIsNone(foreign_view)

        event_id, created = await create_loan_event(
            self.db_dsn,
            self.other_user_id,
            loan_id,
            event_type="EXTRA_PAYMENT",
            event_date=date(2026, 4, 3),
            payload={"amount": "1000.00", "mode": "ONE_TIME", "strategy": "REDUCE_TERM"},
            client_request_id="foreign-req",
        )
        self.assertEqual(event_id, 0)
        self.assertFalse(created)


if __name__ == "__main__":
    unittest.main()
