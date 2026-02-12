#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from portfolio_cards import build_portfolio_map_card_png


def _demo_payload() -> dict:
    positions = [
        {"ticker": "GLDRUB_TOM", "name": "Золото", "value_rub": 467_317.40, "pnl_pct": 3.42, "pnl_rub": 15987.2},
        {"ticker": "PHOR", "name": "ФосАгро", "value_rub": 152_112.00, "pnl_pct": -1.97, "pnl_rub": -3053.5},
        {"ticker": "MDMG", "name": "МД Медикал Груп", "value_rub": 146_072.30, "pnl_pct": 2.11, "pnl_rub": 3032.5},
        {"ticker": "PLZL", "name": "Полюс", "value_rub": 141_457.00, "pnl_pct": 5.61, "pnl_rub": 7935.8},
        {"ticker": "SBER", "name": "Сбербанк", "value_rub": 36_116.50, "pnl_pct": -1.67, "pnl_rub": -603.1},
        {"ticker": "ROSN", "name": "Роснефть", "value_rub": 19_421.20, "pnl_pct": -5.03, "pnl_rub": -979.1},
        {"ticker": "BEGI", "name": "Банк Санкт-Петербург", "value_rub": 12_314.90, "pnl_pct": -0.26, "pnl_rub": -32.0},
    ]
    total = sum(float(p["value_rub"]) for p in positions)
    return {
        "positions": positions,
        "meta": {
            "as_of": "12.02.2026 18:00 МСК",
            "total_value": total,
            "instruments_count": len(positions),
            "mode": "share",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate demo portfolio treemap cards")
    parser.add_argument("--input-json", help="Path to payload JSON with keys positions/meta")
    parser.add_argument("--out-dir", default="./artifacts", help="Output directory")
    args = parser.parse_args()

    if args.input_json:
        payload = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
    else:
        payload = _demo_payload()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    share = build_portfolio_map_card_png(payload, mode="share")
    square = build_portfolio_map_card_png(payload, mode="square")

    share_path = out_dir / "portfolio_map_share_1200x630.png"
    square_path = out_dir / "portfolio_map_square_1080x1080.png"
    share_path.write_bytes(share)
    square_path.write_bytes(square)
    print(f"Saved: {share_path}")
    print(f"Saved: {square_path}")


if __name__ == "__main__":
    main()
