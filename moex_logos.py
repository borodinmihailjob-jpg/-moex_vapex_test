import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_LOGO_URL = "https://dummyimage.com/32x32/e6e6e6/8a8a8a.png&text=%20"
EODHD_MOEX_TEMPLATE_URL = "https://eodhd.com/img/logos/MCX/{ticker}.png"
_LOGO_FILE = Path(__file__).resolve().parent / "moex_logos.json"
_logo_map_cache: dict[str, str] | None = None
_missing_tickers_cache: set[str] | None = None


def _load_logo_data() -> tuple[dict[str, str], set[str]]:
    global _logo_map_cache, _missing_tickers_cache
    if _logo_map_cache is not None and _missing_tickers_cache is not None:
        return _logo_map_cache, _missing_tickers_cache

    out: dict[str, str] = {}
    missing: set[str] = set()
    try:
        raw = json.loads(_LOGO_FILE.read_text(encoding="utf-8"))
        logos = raw.get("logos", {}) if isinstance(raw, dict) else {}
        if isinstance(logos, dict):
            for ticker, url in logos.items():
                t = str(ticker or "").strip().upper()
                u = str(url or "").strip()
                if t and u:
                    out[t] = u
        missing_list = raw.get("missing_tickers", []) if isinstance(raw, dict) else []
        if isinstance(missing_list, list):
            for ticker in missing_list:
                t = str(ticker or "").strip().upper()
                if t:
                    missing.add(t)
    except FileNotFoundError:
        logger.warning("Logo file not found: %s", _LOGO_FILE)
    except Exception:
        logger.exception("Failed to parse logo file: %s", _LOGO_FILE)

    _logo_map_cache = out
    _missing_tickers_cache = missing
    return out, missing


def get_moex_logo_url(ticker: str | None) -> str:
    t = str(ticker or "").strip().upper()
    if not t:
        return DEFAULT_LOGO_URL

    logo_map, missing_tickers = _load_logo_data()
    if t in logo_map:
        return logo_map[t]
    if t in missing_tickers:
        return DEFAULT_LOGO_URL
    return EODHD_MOEX_TEMPLATE_URL.format(ticker=t)
