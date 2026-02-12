import io
import unittest

from PIL import Image, ImageDraw

from portfolio_cards import (
    _build_treemap_layout,
    _fit_line,
    _load_font,
    build_portfolio_map_card_png,
)


class PortfolioMapCardTests(unittest.TestCase):
    def test_layout_rects_are_inside_bounds(self):
        items = [
            {"ticker": "AAA", "value_rub": 50_000},
            {"ticker": "BBB", "value_rub": 30_000},
            {"ticker": "CCC", "value_rub": 20_000},
            {"ticker": "DDD", "value_rub": 10_000},
            {"ticker": "EEE", "value_rub": 5_000},
        ]
        x, y, w, h = 20, 40, 800, 420
        layout = _build_treemap_layout(items, x, y, w, h, gap=6)
        self.assertTrue(layout)
        for _, rect in layout:
            x1, y1, x2, y2 = rect
            self.assertGreaterEqual(x1, x)
            self.assertGreaterEqual(y1, y)
            self.assertLessEqual(x2, x + w)
            self.assertLessEqual(y2, y + h)
            self.assertGreater(x2 - x1, 0)
            self.assertGreater(y2 - y1, 0)

    def test_fit_line_applies_ellipsis(self):
        img = Image.new("RGB", (400, 80), (255, 255, 255))
        draw = ImageDraw.Draw(img)
        font = _load_font(20, bold=True)
        text = "VERY_LONG_TICKER_NAME_WITH_SUFFIX"
        fitted = _fit_line(draw, text, font, max_width=120)
        self.assertTrue(fitted.endswith("..."))
        self.assertLessEqual(draw.textlength(fitted, font=font), 120)

    def test_render_share_and_square_sizes(self):
        payload = {
            "positions": [
                {"ticker": "GLDRUB_TOM", "name": "Золото", "value_rub": 467317.4, "pnl_pct": 3.42, "pnl_rub": 15987.2},
                {"ticker": "PHOR", "name": "ФосАгро", "value_rub": 152112.0, "pnl_pct": -1.97, "pnl_rub": -3053.5},
                {"ticker": "SBER", "name": "Сбербанк", "value_rub": 36116.5, "pnl_pct": -1.67, "pnl_rub": -603.1},
            ],
            "meta": {
                "as_of": "12.02.2026 18:00 МСК",
                "total_value": 655545.9,
                "instruments_count": 3,
                "mode": "share",
            },
        }
        share_png = build_portfolio_map_card_png(payload, mode="share")
        square_png = build_portfolio_map_card_png(payload, mode="square")

        share = Image.open(io.BytesIO(share_png))
        square = Image.open(io.BytesIO(square_png))
        self.assertEqual(share.size, (1200, 630))
        self.assertEqual(square.size, (1080, 1080))


if __name__ == "__main__":
    unittest.main()
