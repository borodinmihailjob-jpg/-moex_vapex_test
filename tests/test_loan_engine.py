import unittest
from datetime import date
from decimal import Decimal

from loan_engine import (
    ExtraPaymentEvent,
    HolidayEvent,
    LoanInput,
    RateChangeEvent,
    calculate,
)


class LoanEngineTests(unittest.TestCase):
    def make_loan(self) -> LoanInput:
        return LoanInput(
            principal=Decimal("3500000.00"),
            current_principal=Decimal("3500000.00"),
            annual_rate=Decimal("12.90"),
            payment_type="ANNUITY",
            term_months=240,
            first_payment_date=date(2026, 3, 3),
            issue_date=date(2026, 2, 10),
            currency="RUB",
            calc_date=date(2026, 2, 14),
        )

    def test_annuity_without_events(self):
        loan = self.make_loan()
        summary, schedule, _, _ = calculate(loan, [])
        self.assertEqual(len(schedule), 240)
        self.assertEqual(Decimal(schedule[-1]["balance"]), Decimal("0.00"))
        principal_sum = sum(Decimal(x["principal"]) for x in schedule)
        self.assertAlmostEqual(float(principal_sum), float(loan.current_principal), places=1)
        self.assertEqual(int(summary["payments_count"]), 240)

    def test_zero_rate(self):
        loan = self.make_loan()
        loan.annual_rate = Decimal("0")
        summary, schedule, _, _ = calculate(loan, [])
        self.assertEqual(schedule[0]["interest"], "0.00")
        expected = (loan.principal / Decimal(loan.term_months)).quantize(Decimal("0.01"))
        self.assertEqual(Decimal(schedule[0]["payment"]), expected)
        self.assertEqual(Decimal(summary["total_interest"]), Decimal("0.00"))

    def test_extra_reduce_term_vs_reduce_payment(self):
        loan = self.make_loan()
        e_term = [ExtraPaymentEvent(date=date(2026, 4, 3), amount=Decimal("100000.00"), mode="ONE_TIME", strategy="REDUCE_TERM")]
        e_pay = [ExtraPaymentEvent(date=date(2026, 4, 3), amount=Decimal("100000.00"), mode="ONE_TIME", strategy="REDUCE_PAYMENT")]
        s_term, sch_term, _, _ = calculate(loan, e_term)
        s_pay, sch_pay, _, _ = calculate(loan, e_pay)
        self.assertLess(int(s_term["payments_count"]), int(s_pay["payments_count"]))
        self.assertLess(Decimal(s_term["total_interest"]), Decimal(s_pay["total_interest"]))
        self.assertLess(Decimal(s_pay["monthly_payment"]), Decimal(self.make_loan().principal))
        self.assertTrue(len(sch_term) > 0 and len(sch_pay) > 0)

    def test_extra_more_than_balance(self):
        loan = self.make_loan()
        events = [ExtraPaymentEvent(date=date(2026, 3, 3), amount=Decimal("99999999.00"), mode="ONE_TIME", strategy="REDUCE_TERM")]
        _, schedule, _, _ = calculate(loan, events)
        self.assertEqual(Decimal(schedule[-1]["balance"]), Decimal("0.00"))
        self.assertLessEqual(len(schedule), 2)

    def test_rate_change(self):
        loan = self.make_loan()
        base_summary, _, _, _ = calculate(loan, [])
        changed_summary, changed_schedule, _, _ = calculate(
            loan,
            [RateChangeEvent(date=date(2026, 9, 3), annual_rate=Decimal("10.90"))],
        )
        self.assertNotEqual(base_summary["total_interest"], changed_summary["total_interest"])
        self.assertTrue(any(Decimal(x["annual_rate"]) == Decimal("10.90") for x in changed_schedule))

    def test_holiday_interest_only(self):
        loan = self.make_loan()
        summary, schedule, _, _ = calculate(
            loan,
            [HolidayEvent(start_date=date(2026, 6, 3), end_date=date(2026, 8, 3), holiday_type="INTEREST_ONLY")],
        )
        affected = [x for x in schedule if x["date"] in {"2026-06-03", "2026-07-03", "2026-08-03"}]
        self.assertTrue(affected)
        for item in affected:
            self.assertEqual(Decimal(item["principal"]), Decimal("0.00"))
        self.assertGreater(Decimal(summary["total_interest"]), Decimal("0.00"))

    def test_calculation_from_current_principal_and_current_date(self):
        loan = LoanInput(
            principal=Decimal("5000000.00"),
            current_principal=Decimal("3200000.00"),
            annual_rate=Decimal("11.50"),
            payment_type="ANNUITY",
            term_months=240,
            first_payment_date=date(2020, 3, 3),
            issue_date=date(2020, 2, 10),
            currency="RUB",
            calc_date=date(2026, 2, 14),
        )
        summary, schedule, _, _ = calculate(loan, [])
        self.assertGreater(len(schedule), 0)
        self.assertGreater(Decimal(summary["monthly_payment"]), Decimal("0.00"))
        self.assertEqual(Decimal(summary["remaining_balance"]), Decimal("3200000.00"))
        self.assertEqual(Decimal(summary["paid_principal_to_date"]), Decimal("1800000.00"))


if __name__ == "__main__":
    unittest.main()
