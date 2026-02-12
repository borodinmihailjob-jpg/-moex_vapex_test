import io
import unittest

from PIL import Image

from portfolio_cards import build_portfolio_share_card_png


class PortfolioShareCardTests(unittest.TestCase):
    def _base_input(self):
        composition_rows = [
            {"instrument_id": 1, "secid": "SBER", "name_ru": "Сбербанк", "share_pct": 36.2, "ret_30d": 4.2, "asset_type": "stock"},
            {"instrument_id": 2, "secid": "GLDRUB_TOM", "name_ru": "Золото", "share_pct": 22.1, "ret_30d": 2.7, "asset_type": "metal"},
            {"instrument_id": 3, "secid": "PHOR", "name_ru": "ФосАгро очень длинное имя компании для проверки обрезки", "share_pct": 18.4, "ret_30d": -3.5, "asset_type": "stock"},
            {"instrument_id": 4, "secid": "USDRUB_TOM", "name_ru": "Доллар", "share_pct": 11.2, "ret_30d": 1.1, "asset_type": "fiat"},
        ]
        top_gainers = [
            {"secid": "SBER", "shortname": "Сбербанк", "pnl_pct": 4.5},
            {"secid": "PLZL", "shortname": "Полюс", "pnl_pct": 2.9},
            {"secid": "GLDRUB_TOM", "shortname": "Золото", "pnl_pct": 2.1},
        ]
        top_losers = [
            {"secid": "PHOR", "shortname": "ФосАгро", "pnl_pct": -3.2},
            {"secid": "ROSN", "shortname": "Роснефть", "pnl_pct": -2.4},
            {"secid": "BEGI", "shortname": "БСПБ", "pnl_pct": -1.9},
        ]
        return composition_rows, top_gainers, top_losers

    def test_share_card_size(self):
        rows, gainers, losers = self._base_input()
        data = build_portfolio_share_card_png(
            composition_rows=rows,
            portfolio_return_30d=5.29,
            moex_return_30d=2.26,
            top_gainers=gainers,
            top_losers=losers,
        )
        img = Image.open(io.BytesIO(data))
        self.assertEqual(img.size, (1080, 1350))

    def test_public_policy_rejects_currency_values(self):
        rows, gainers, losers = self._base_input()
        rows[0]["name_ru"] = "Секрет 10000 RUB"
        with self.assertRaises(ValueError):
            build_portfolio_share_card_png(
                composition_rows=rows,
                portfolio_return_30d=5.29,
                moex_return_30d=2.26,
                top_gainers=gainers,
                top_losers=losers,
            )


if __name__ == "__main__":
    unittest.main()
