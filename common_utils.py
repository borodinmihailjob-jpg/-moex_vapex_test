from __future__ import annotations


def safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def pick_stock_candidate_by_isin(cands: list[dict], isin: str) -> dict | None:
    isin_upper = isin.strip().upper()
    if not isin_upper:
        return cands[0] if cands else None
    for cand in cands:
        if str(cand.get("isin") or "").strip().upper() == isin_upper:
            return cand
    return cands[0] if cands else None
