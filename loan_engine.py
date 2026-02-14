from __future__ import annotations

import calendar
import hashlib
import json
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Literal

MONEY_Q = Decimal("0.01")
RATE_MONTHS = Decimal("12")
RATE_100 = Decimal("100")

PaymentType = Literal["ANNUITY", "DIFFERENTIATED"]
AccrualMode = Literal["MONTHLY", "ACT_365"]
ExtraMode = Literal["ONE_TIME", "MONTHLY", "WEEKLY", "BIWEEKLY", "YEARLY"]
ExtraStrategy = Literal["REDUCE_TERM", "REDUCE_PAYMENT"]
HolidayType = Literal["INTEREST_ONLY", "PAUSE_CAPITALIZE"]


@dataclass(slots=True)
class LoanInput:
    principal: Decimal
    current_principal: Decimal
    annual_rate: Decimal
    payment_type: PaymentType
    term_months: int
    first_payment_date: date
    issue_date: date | None = None
    currency: str = "RUB"
    calc_date: date | None = None
    accrual_mode: AccrualMode = "MONTHLY"
    insurance_monthly: Decimal = Decimal("0")
    one_time_costs: Decimal = Decimal("0")


@dataclass(slots=True)
class ExtraPaymentEvent:
    date: date
    amount: Decimal
    mode: ExtraMode
    strategy: ExtraStrategy
    end_date: date | None = None


@dataclass(slots=True)
class RateChangeEvent:
    date: date
    annual_rate: Decimal


@dataclass(slots=True)
class HolidayEvent:
    start_date: date
    end_date: date
    holiday_type: HolidayType


LoanEvent = ExtraPaymentEvent | RateChangeEvent | HolidayEvent


def q_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_Q, rounding=ROUND_HALF_EVEN)


def add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    day = min(d.day, calendar.monthrange(y, m)[1])
    return date(y, m, day)


def month_diff(start: date, end: date) -> int:
    if end < start:
        return 0
    months = (end.year - start.year) * 12 + (end.month - start.month)
    if end.day < start.day:
        months -= 1
    return max(0, months)


def next_payment_date(first_payment_date: date, calc_date: date) -> date:
    if first_payment_date >= calc_date:
        return first_payment_date
    elapsed = month_diff(first_payment_date, calc_date)
    candidate = add_months(first_payment_date, elapsed)
    if candidate < calc_date:
        candidate = add_months(candidate, 1)
    return candidate


def annuity_payment(principal: Decimal, annual_rate: Decimal, months: int) -> Decimal:
    if months <= 0 or principal <= 0:
        return Decimal("0")
    if annual_rate <= 0:
        return q_money(principal / Decimal(months))
    i = annual_rate / RATE_MONTHS / RATE_100
    one_plus = Decimal("1") + i
    factor = one_plus ** months
    payment = principal * i * factor / (factor - Decimal("1"))
    return q_money(payment)


def _event_dict(ev: LoanEvent) -> dict:
    if isinstance(ev, ExtraPaymentEvent):
        return {
            "type": "EXTRA_PAYMENT",
            "date": ev.date.isoformat(),
            "amount": str(ev.amount),
            "mode": ev.mode,
            "strategy": ev.strategy,
            "end_date": ev.end_date.isoformat() if ev.end_date else None,
        }
    if isinstance(ev, RateChangeEvent):
        return {
            "type": "RATE_CHANGE",
            "date": ev.date.isoformat(),
            "annual_rate": str(ev.annual_rate),
        }
    return {
        "type": "HOLIDAY",
        "start_date": ev.start_date.isoformat(),
        "end_date": ev.end_date.isoformat(),
        "holiday_type": ev.holiday_type,
    }


def build_version_hash(loan: LoanInput, events: list[LoanEvent]) -> tuple[int, str]:
    payload = {
        "loan": {
            "principal": str(loan.principal),
            "current_principal": str(loan.current_principal),
            "annual_rate": str(loan.annual_rate),
            "payment_type": loan.payment_type,
            "term_months": loan.term_months,
            "first_payment_date": loan.first_payment_date.isoformat(),
            "issue_date": loan.issue_date.isoformat() if loan.issue_date else None,
            "currency": loan.currency,
            "calc_date": loan.calc_date.isoformat() if loan.calc_date else None,
            "accrual_mode": loan.accrual_mode,
            "insurance_monthly": str(loan.insurance_monthly),
            "one_time_costs": str(loan.one_time_costs),
        },
        "events": sorted((_event_dict(ev) for ev in events), key=lambda x: json.dumps(x, sort_keys=True)),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    version = int(digest[:8], 16)
    return version, digest


def _count_recurring_occurrences(event: ExtraPaymentEvent, prev_date: date, payment_date: date) -> int:
    if event.end_date and prev_date > event.end_date:
        return 0
    if payment_date < event.date:
        return 0

    start_window = max(prev_date, event.date)
    end_window = payment_date
    if event.end_date is not None:
        end_window = min(end_window, event.end_date)
    if start_window > end_window:
        return 0

    if event.mode == "MONTHLY":
        return 1 if payment_date.day == event.date.day else 0
    if event.mode == "YEARLY":
        return 1 if (payment_date.month, payment_date.day) == (event.date.month, event.date.day) else 0

    step = 7 if event.mode == "WEEKLY" else 14
    cur = event.date
    if cur < start_window:
        delta_days = (start_window - cur).days
        jumps = delta_days // step
        cur = cur + timedelta(days=jumps * step)
        while cur < start_window:
            cur = cur + timedelta(days=step)

    cnt = 0
    while cur <= end_window:
        cnt += 1
        cur = cur + timedelta(days=step)
    return cnt


def _calc_interest(balance: Decimal, annual_rate: Decimal, accrual_mode: AccrualMode, prev_payment_date: date, payment_date: date) -> Decimal:
    if annual_rate <= 0 or balance <= 0:
        return Decimal("0.00")
    if accrual_mode == "ACT_365":
        days = max(1, (payment_date - prev_payment_date).days)
        return q_money(balance * (annual_rate / RATE_100) * Decimal(days) / Decimal(365))
    monthly_rate = annual_rate / RATE_MONTHS / RATE_100
    return q_money(balance * monthly_rate)


def calculate(loan: LoanInput, events: list[LoanEvent]) -> tuple[dict, list[dict], int, str]:
    if loan.term_months < 1:
        raise ValueError("term_months must be >= 1")
    if loan.term_months > 600:
        raise ValueError("term_months must be <= 600")
    if loan.principal <= 0:
        raise ValueError("principal must be > 0")
    if loan.current_principal <= 0:
        raise ValueError("current_principal must be > 0")
    if loan.current_principal > loan.principal:
        raise ValueError("current_principal must be <= principal")
    if loan.annual_rate < 0 or loan.annual_rate > 100:
        raise ValueError("annual_rate must be in [0, 100]")
    if loan.accrual_mode not in {"MONTHLY", "ACT_365"}:
        raise ValueError("accrual_mode invalid")

    version, version_hash = build_version_hash(loan, events)

    calc_date = loan.calc_date or date.today()
    start_date = next_payment_date(loan.first_payment_date, calc_date)
    elapsed_months = month_diff(loan.first_payment_date, start_date)
    months_left_total = max(1, loan.term_months - elapsed_months)

    extra_events = [e for e in events if isinstance(e, ExtraPaymentEvent)]
    rate_events = sorted([e for e in events if isinstance(e, RateChangeEvent)], key=lambda x: x.date)
    holiday_events = [e for e in events if isinstance(e, HolidayEvent)]

    balance = q_money(loan.current_principal)
    current_rate = loan.annual_rate
    rate_idx = 0

    while rate_idx < len(rate_events) and rate_events[rate_idx].date <= start_date:
        current_rate = rate_events[rate_idx].annual_rate
        rate_idx += 1

    annuity_target_payment = annuity_payment(balance, current_rate, months_left_total) if loan.payment_type == "ANNUITY" else Decimal("0")
    principal_part_fixed = q_money(balance / Decimal(months_left_total)) if loan.payment_type == "DIFFERENTIATED" else Decimal("0")

    schedule: list[dict] = []
    paid_total_future = Decimal("0")
    paid_interest_future = Decimal("0")
    paid_principal_future = Decimal("0")

    prev_payment_date = add_months(start_date, -1)

    for month_idx in range(1200):
        if balance <= 0:
            break

        payment_date = add_months(start_date, month_idx)
        months_left = max(1, months_left_total - month_idx)

        rate_changed = False
        while rate_idx < len(rate_events) and rate_events[rate_idx].date <= payment_date:
            current_rate = rate_events[rate_idx].annual_rate
            rate_idx += 1
            rate_changed = True

        if rate_changed:
            if loan.payment_type == "ANNUITY":
                annuity_target_payment = annuity_payment(balance, current_rate, months_left)
            else:
                principal_part_fixed = q_money(balance / Decimal(months_left))

        in_holiday: HolidayEvent | None = None
        for he in holiday_events:
            if he.start_date <= payment_date <= he.end_date:
                in_holiday = he
                break

        interest = _calc_interest(balance, current_rate, loan.accrual_mode, prev_payment_date, payment_date)
        payment = Decimal("0")
        principal_part = Decimal("0")

        if in_holiday is not None:
            if in_holiday.holiday_type == "INTEREST_ONLY":
                payment = interest
                principal_part = Decimal("0")
            else:
                payment = Decimal("0")
                principal_part = Decimal("0")
                balance = q_money(balance + interest)
                schedule.append(
                    {
                        "date": payment_date.isoformat(),
                        "payment": str(q_money(payment)),
                        "interest": str(interest),
                        "principal": str(q_money(principal_part)),
                        "balance": str(q_money(balance)),
                        "annual_rate": str(q_money(current_rate)),
                        "event": "HOLIDAY_PAUSE_CAPITALIZE",
                    }
                )
                prev_payment_date = payment_date
                continue
        else:
            if loan.payment_type == "ANNUITY":
                if annuity_target_payment <= 0:
                    annuity_target_payment = annuity_payment(balance, current_rate, months_left)
                principal_part = q_money(annuity_target_payment - interest)
                if principal_part < 0:
                    principal_part = Decimal("0")
                if principal_part > balance:
                    principal_part = balance
                payment = q_money(principal_part + interest)
            else:
                if principal_part_fixed <= 0:
                    principal_part_fixed = q_money(balance / Decimal(months_left))
                principal_part = principal_part_fixed if principal_part_fixed < balance else balance
                payment = q_money(principal_part + interest)

        balance = q_money(balance - principal_part)

        month_extra = Decimal("0")
        month_event_notes: list[str] = []

        for ex in extra_events:
            cnt = 0
            if ex.mode == "ONE_TIME":
                cnt = 1 if ex.date == payment_date else 0
            else:
                cnt = _count_recurring_occurrences(ex, prev_payment_date + timedelta(days=1), payment_date)
            if cnt <= 0 or balance <= 0:
                continue

            extra_amt = q_money(ex.amount * Decimal(cnt))
            if extra_amt > balance:
                extra_amt = balance
            if extra_amt <= 0:
                continue

            balance = q_money(balance - extra_amt)
            month_extra += extra_amt
            month_event_notes.append(f"EXTRA_{ex.mode}_{ex.strategy}")

            if loan.payment_type == "ANNUITY":
                if ex.strategy == "REDUCE_PAYMENT":
                    annuity_target_payment = annuity_payment(balance, current_rate, max(1, months_left - 1))
            else:
                if ex.strategy == "REDUCE_PAYMENT" and balance > 0:
                    principal_part_fixed = q_money(balance / Decimal(max(1, months_left - 1)))

        paid_total_future += payment + month_extra
        paid_interest_future += interest
        paid_principal_future += principal_part + month_extra

        schedule.append(
            {
                "date": payment_date.isoformat(),
                "payment": str(q_money(payment + month_extra)),
                "interest": str(q_money(interest)),
                "principal": str(q_money(principal_part + month_extra)),
                "balance": str(q_money(balance)),
                "annual_rate": str(q_money(current_rate)),
                "event": ",".join(month_event_notes) if month_event_notes else None,
            }
        )
        prev_payment_date = payment_date

    if schedule and Decimal(schedule[-1]["balance"]) != Decimal("0.00"):
        tail = Decimal(schedule[-1]["balance"])
        if tail > 0 and tail < Decimal("0.05"):
            schedule[-1]["balance"] = "0.00"
            schedule[-1]["principal"] = str(q_money(Decimal(schedule[-1]["principal"]) + tail))
            schedule[-1]["payment"] = str(q_money(Decimal(schedule[-1]["payment"]) + tail))
            paid_principal_future += tail
            paid_total_future += tail

    next_payment = schedule[0] if schedule else None
    paid_principal_to_date = q_money(loan.principal - loan.current_principal)
    insurance_total = q_money(loan.insurance_monthly * Decimal(len(schedule)))
    total_future_cost = q_money(paid_total_future + insurance_total + loan.one_time_costs)

    summary = {
        "principal": str(q_money(loan.principal)),
        "current_principal": str(q_money(loan.current_principal)),
        "remaining_balance": str(q_money(loan.current_principal)),
        "balance_after_schedule": str(q_money(balance)),
        "monthly_payment": next_payment["payment"] if next_payment else "0.00",
        "total_paid": str(q_money(paid_total_future)),
        "total_interest": str(q_money(paid_interest_future)),
        "total_principal_paid": str(q_money(paid_principal_future)),
        "paid_principal_to_date": str(q_money(paid_principal_to_date)),
        "payments_count": int(len(schedule)),
        "payoff_date": schedule[-1]["date"] if schedule else None,
        "next_payment": next_payment,
        "schedule_start_date": start_date.isoformat(),
        "accrual_mode": loan.accrual_mode,
        "insurance_total": str(insurance_total),
        "one_time_costs": str(q_money(loan.one_time_costs)),
        "total_future_cost": str(total_future_cost),
    }
    return summary, schedule, version, version_hash
