import io
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import squarify
from PIL import Image, ImageDraw, ImageFilter, ImageFont

MSK_TZ = ZoneInfo("Europe/Moscow")
MAP_CARD_SIZES = {
    "share": (1200, 630),
    "square": (1080, 1080),
}


def money(x: float) -> str:
    return f"{x:,.2f}".replace(",", " ")


def _load_font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    candidates = [
        "/System/Library/Fonts/Supplemental/SF Pro Display Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/SF Pro Display Regular.ttf",
        "/Library/Fonts/Inter-Bold.ttf" if bold else "/Library/Fonts/Inter-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/Library/Fonts/Arial Bold.ttf" if bold else "/Library/Fonts/Arial.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def _blend(c1: tuple[int, int, int], c2: tuple[int, int, int], t: float) -> tuple[int, int, int]:
    t = max(0.0, min(1.0, t))
    return (
        int(c1[0] + (c2[0] - c1[0]) * t),
        int(c1[1] + (c2[1] - c1[1]) * t),
        int(c1[2] + (c2[2] - c1[2]) * t),
    )


def _tile_color_by_pnl_pct(pnl_pct: float | None) -> tuple[int, int, int]:
    neutral = (87, 104, 132)
    green = (39, 181, 133)
    red = (207, 64, 105)
    if pnl_pct is None:
        return neutral
    norm = max(-8.0, min(8.0, float(pnl_pct))) / 8.0
    strength = abs(norm) ** 0.65
    if norm >= 0:
        return _blend(neutral, green, strength)
    return _blend(neutral, red, strength)


def _text_color_for_bg(bg: tuple[int, int, int]) -> tuple[int, int, int]:
    lum = 0.299 * bg[0] + 0.587 * bg[1] + 0.114 * bg[2]
    return (20, 20, 20) if lum > 150 else (245, 245, 245)


def _fit_line(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> str:
    if draw.textlength(text, font=font) <= max_width:
        return text
    out = text
    while len(out) > 1 and draw.textlength(out + "...", font=font) > max_width:
        out = out[:-1]
    return out + "..." if out else ""


def _text_height(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    left, top, right, bottom = draw.textbbox((0, 0), text, font=font)
    return max(1, int(bottom - top))


def _build_treemap_layout(
    items: list[dict],
    x: int,
    y: int,
    w: int,
    h: int,
    gap: int = 6,
) -> list[tuple[dict, tuple[int, int, int, int]]]:
    if not items or w <= 0 or h <= 0:
        return []
    ordered = sorted(items, key=lambda t: float(t.get("value_rub") or 0.0), reverse=True)
    sizes = [max(1e-9, float(t.get("value_rub") or 0.0)) for t in ordered]
    norm = squarify.normalize_sizes(sizes, w, h)
    if hasattr(squarify, "padded_squarify"):
        raw = squarify.padded_squarify(norm, x, y, w, h)
    else:
        raw = squarify.squarify(norm, x, y, w, h)

    out: list[tuple[dict, tuple[int, int, int, int]]] = []
    for item, rect in zip(ordered, raw):
        rx = int(rect["x"]) + gap // 2
        ry = int(rect["y"]) + gap // 2
        rw = int(rect["dx"]) - gap
        rh = int(rect["dy"]) - gap
        if rw < 2 or rh < 2:
            continue
        out.append((item, (rx, ry, rx + rw, ry + rh)))
    return out


def _draw_drop_shadow(
    canvas: Image.Image,
    rect: tuple[int, int, int, int],
    radius: int,
    offset: tuple[int, int] = (0, 8),
    blur: int = 18,
    color: tuple[int, int, int, int] = (38, 61, 97, 62),
) -> None:
    shadow = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(shadow)
    x1, y1, x2, y2 = rect
    ox, oy = offset
    d.rounded_rectangle((x1 + ox, y1 + oy, x2 + ox, y2 + oy), radius=radius, fill=color)
    shadow = shadow.filter(ImageFilter.GaussianBlur(blur))
    canvas.alpha_composite(shadow)


def _draw_soft_background(draw: ImageDraw.ImageDraw, w: int, h: int) -> None:
    top = (226, 236, 250)
    bottom = (204, 219, 240)
    for y in range(h):
        t = y / max(1, h - 1)
        c = _blend(top, bottom, t)
        draw.line((0, y, w, y), fill=c)
    draw.ellipse((-int(w * 0.2), -int(h * 0.3), int(w * 0.35), int(h * 0.4)), fill=(236, 242, 252))
    draw.ellipse((int(w * 0.55), -int(h * 0.22), int(w * 1.15), int(h * 0.45)), fill=(220, 235, 248))


def _to_payload_from_tiles(tiles: list[dict], mode: str = "share") -> dict:
    positions = []
    for t in tiles:
        value = float(t.get("value") or t.get("value_rub") or 0.0)
        pnl_pct = t.get("pnl_pct")
        pnl_rub = None
        if pnl_pct is not None:
            try:
                pnl_rub = value * float(pnl_pct) / 100.0
            except (TypeError, ValueError):
                pnl_rub = None
        positions.append(
            {
                "ticker": str(t.get("secid") or t.get("ticker") or "UNKNOWN"),
                "name": str(t.get("shortname") or t.get("name") or "").strip(),
                "value_rub": value,
                "pnl_pct": (float(pnl_pct) if pnl_pct is not None else None),
                "pnl_rub": pnl_rub,
            }
        )
    total = sum(float(p["value_rub"]) for p in positions)
    return {
        "positions": positions,
        "meta": {
            "as_of": datetime.now(MSK_TZ).strftime("%d.%m.%Y %H:%M МСК"),
            "total_value": total,
            "instruments_count": len(positions),
            "mode": mode,
        },
    }


def build_portfolio_map_card_png(payload: dict, mode: str = "share") -> bytes:
    size = MAP_CARD_SIZES.get(mode, MAP_CARD_SIZES["share"])
    width, height = size
    canvas = Image.new("RGBA", size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(canvas)
    _draw_soft_background(draw, width, height)

    card_pad = int(min(width, height) * 0.04)
    card_rect = (card_pad, card_pad, width - card_pad, height - card_pad)
    card_radius = int(min(width, height) * 0.04)
    _draw_drop_shadow(canvas, card_rect, radius=card_radius, blur=22)
    draw.rounded_rectangle(card_rect, radius=card_radius, fill=(245, 247, 252), outline=(223, 231, 244), width=2)

    title_font = _load_font(58 if mode == "share" else 52, bold=True)
    subtitle_font = _load_font(24 if mode == "share" else 22, bold=False)
    badge_title_font = _load_font(26 if mode == "share" else 24, bold=True)
    badge_value_font = _load_font(58 if mode == "share" else 52, bold=True)
    ticker_font = _load_font(22 if mode == "share" else 20, bold=True)
    name_font = _load_font(17 if mode == "share" else 16, bold=False)
    value_font = _load_font(22 if mode == "share" else 20, bold=True)
    pnl_font = _load_font(16 if mode == "share" else 15, bold=True)
    footer_font = _load_font(18 if mode == "share" else 17, bold=False)

    meta = payload.get("meta", {})
    positions = payload.get("positions", [])
    total_value = float(meta.get("total_value") or sum(float(p.get("value_rub") or 0.0) for p in positions))
    instruments_count = int(meta.get("instruments_count") or len(positions))
    as_of = str(meta.get("as_of") or datetime.now(MSK_TZ).strftime("%d.%m.%Y %H:%M МСК"))
    pnl_total = sum(float(p.get("pnl_rub") or 0.0) for p in positions)
    pnl_pct = (pnl_total / total_value * 100.0) if abs(total_value) > 1e-12 else 0.0

    x1, y1, x2, y2 = card_rect
    header_h = int((y2 - y1) * (0.2 if mode == "share" else 0.22))
    inner_pad = int(min(width, height) * 0.02)

    draw.text((x1 + inner_pad, y1 + inner_pad - 2), "Карта портфеля", fill=(34, 45, 78), font=title_font)
    sub = f"Инструментов: {instruments_count} • Общая стоимость: {money(total_value)} ₽ • {as_of}"
    draw.text((x1 + inner_pad, y1 + inner_pad + int(title_font.size * 1.15)), sub, fill=(96, 112, 143), font=subtitle_font)

    badge_w = int((x2 - x1) * (0.22 if mode == "share" else 0.3))
    badge_h = int(header_h * 0.8)
    badge_x2 = x2 - inner_pad
    badge_x1 = badge_x2 - badge_w
    badge_y1 = y1 + inner_pad
    badge_y2 = badge_y1 + badge_h
    badge_bg = (226, 243, 233) if pnl_pct >= 0 else (249, 231, 236)
    badge_fg = (28, 133, 91) if pnl_pct >= 0 else (181, 52, 92)
    draw.rounded_rectangle((badge_x1, badge_y1, badge_x2, badge_y2), radius=18, fill=badge_bg)
    draw.text((badge_x1 + 20, badge_y1 + 12), "P&L сегодня", fill=badge_fg, font=badge_title_font)
    draw.text((badge_x1 + 20, badge_y1 + int(badge_h * 0.42)), f"{pnl_pct:+.2f}%", fill=badge_fg, font=badge_value_font)

    sep_y = y1 + header_h
    draw.line((x1 + inner_pad, sep_y, x2 - inner_pad, sep_y), fill=(223, 231, 244), width=2)

    treemap_x = x1 + inner_pad
    treemap_y = sep_y + inner_pad // 2
    footer_h = int((y2 - y1) * 0.08)
    treemap_w = (x2 - x1) - inner_pad * 2
    treemap_h = (y2 - y1) - header_h - footer_h - inner_pad

    treemap_items = []
    for p in positions:
        treemap_items.append(
            {
                "ticker": str(p.get("ticker") or "UNKNOWN"),
                "name": str(p.get("name") or "").strip(),
                "value_rub": float(p.get("value_rub") or 0.0),
                "pnl_pct": p.get("pnl_pct"),
            }
        )
    placements = _build_treemap_layout(treemap_items, treemap_x, treemap_y, treemap_w, treemap_h, gap=6)

    for item, rect in placements:
        rx1, ry1, rx2, ry2 = rect
        tw = rx2 - rx1
        th = ry2 - ry1
        fill = _tile_color_by_pnl_pct(item.get("pnl_pct"))
        fg = _text_color_for_bg(fill)
        radius = max(8, min(24, int(min(tw, th) * 0.12)))
        draw.rounded_rectangle((rx1, ry1, rx2, ry2), radius=radius, fill=fill)
        draw.rounded_rectangle((rx1, ry1, rx2, ry2), radius=radius, outline=(255, 255, 255, 120), width=2)

        if tw < 80 or th < 55:
            continue
        px = rx1 + 10
        py = ry1 + 8
        ticker = _fit_line(draw, item["ticker"], ticker_font, tw - 20)
        draw.text((px, py), ticker, fill=fg, font=ticker_font)
        py += _text_height(draw, ticker, ticker_font) + 2

        if th >= 92:
            name = _fit_line(draw, item.get("name") or "", name_font, tw - 20)
            if name:
                draw.text((px, py), name, fill=fg, font=name_font)

        val = _fit_line(draw, f"{money(float(item['value_rub']))} ₽", value_font, tw - 20)
        draw.text((px, ry2 - _text_height(draw, val, value_font) - 10), val, fill=fg, font=value_font)

        pnl = item.get("pnl_pct")
        if pnl is not None and tw >= 165 and th >= 70:
            ptxt = f"P&L {float(pnl):+,.2f}%".replace(",", " ")
            ptxt = _fit_line(draw, ptxt, pnl_font, tw - 20)
            pw = int(draw.textlength(ptxt, font=pnl_font))
            draw.text((rx2 - pw - 10, ry1 + 8), ptxt, fill=fg, font=pnl_font)

    draw.text(
        ((x1 + x2) // 2 - int(draw.textlength("@moex_vapex_bot", font=footer_font) // 2), y2 - footer_h + 8),
        "@moex_vapex_bot",
        fill=(101, 116, 145),
        font=footer_font,
    )

    out = io.BytesIO()
    canvas.convert("RGB").save(out, format="PNG")
    return out.getvalue()


def build_portfolio_map_png(tiles: list[dict]) -> bytes:
    payload = _to_payload_from_tiles(tiles, mode="share")
    return build_portfolio_map_card_png(payload, mode="share")


def build_portfolio_map_square_png(tiles: list[dict]) -> bytes:
    payload = _to_payload_from_tiles(tiles, mode="square")
    return build_portfolio_map_card_png(payload, mode="square")


def build_portfolio_share_card_png(
    *,
    composition_rows: list[dict],
    portfolio_return_30d: float | None,
    moex_return_30d: float | None,
    top_gainers: list[dict],
    top_losers: list[dict],
) -> bytes:
    width, height = 1080, 1350
    image = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)

    def alpha(c: tuple[int, int, int], a: int) -> tuple[int, int, int, int]:
        return c[0], c[1], c[2], a

    def strength_color(base: tuple[int, int, int], strength: str) -> tuple[int, int, int]:
        if strength == "strong":
            return base
        if strength == "moderate":
            return _blend((245, 247, 252), base, 0.72)
        return _blend((245, 247, 252), base, 0.55)

    def trend_state(v: float | None) -> tuple[str, str]:
        if v is None:
            return "flat", "slight"
        av = abs(float(v))
        if av >= 7.0:
            strength = "strong"
        elif av >= 3.0:
            strength = "moderate"
        else:
            strength = "slight"
        if v > 0:
            return "up", strength
        if v < 0:
            return "down", strength
        return "flat", strength

    def tint_by_trend(v: float | None) -> tuple[int, int, int]:
        state, strength = trend_state(v)
        if state == "up":
            return strength_color((28, 163, 110), strength)
        if state == "down":
            return strength_color((205, 62, 105), strength)
        return (141, 150, 165)

    def weight_level(share_pct: float) -> str:
        if share_pct >= 15.0:
            return "high"
        if share_pct >= 6.0:
            return "mid"
        return "low"

    def weight_ratio(level: str) -> float:
        return {"high": 0.75, "mid": 0.5, "low": 0.25}.get(level, 0.25)

    def draw_glass_card(rect: tuple[int, int, int, int], radius: int = 26) -> None:
        _draw_drop_shadow(image, rect, radius=radius, blur=14, offset=(0, 6), color=(56, 72, 109, 45))
        draw.rounded_rectangle(rect, radius=radius, fill=alpha((250, 252, 255), 214), outline=alpha((255, 255, 255), 230), width=2)
        x1, y1, x2, y2 = rect
        h = max(1, y2 - y1)
        for i in range(0, h, 2):
            t = i / h
            c = _blend((255, 255, 255), (240, 246, 253), t)
            draw.line((x1 + 2, y1 + i, x2 - 2, y1 + i), fill=alpha(c, 50))

    def pct_text(v: float | None) -> str:
        if v is None:
            return "0.00%"
        return f"{float(v):+,.2f}%".replace(",", " ")

    def draw_label_value_line(
        x: int,
        y: int,
        label: str,
        value: str,
        label_font: ImageFont.ImageFont,
        value_font: ImageFont.ImageFont,
        label_color: tuple[int, int, int],
        value_color: tuple[int, int, int],
        max_width: int,
    ) -> tuple[str, str]:
        label_w = int(draw.textlength(label, font=label_font))
        max_val_w = max(20, max_width - label_w)
        val_fit = _fit_line(draw, value, value_font, max_val_w)
        val_w = int(draw.textlength(val_fit, font=value_font))
        max_label_w = max(20, max_width - val_w)
        label_fit = _fit_line(draw, label, label_font, max_label_w)
        draw.text((x, y), label_fit, fill=label_color, font=label_font)
        lx = x + int(draw.textlength(label_fit, font=label_font))
        draw.text((lx, y), val_fit, fill=value_color, font=value_font)
        return label_fit, val_fit

    def validate_public_text_policy(texts: list[str]) -> None:
        forbidden_currency = re.compile(r"(₽|RUB|руб|USD|EUR|CNY|\$|€|¥)", re.IGNORECASE)
        sum_like = re.compile(r"\d[\d\s]{3,}(?:[.,]\d+)?")
        for txt in texts:
            if re.search(r"\d", txt) and forbidden_currency.search(txt):
                raise ValueError("PUBLIC policy: currency value detected")
            if "%" not in txt and sum_like.search(txt):
                # allow date like 12.02.2026
                if not re.search(r"\d{2}\.\d{2}\.\d{4}", txt):
                    raise ValueError("PUBLIC policy: absolute numeric value detected")

    # Background
    _draw_soft_background(draw, width, height)
    draw.ellipse((-140, -120, 500, 420), fill=(237, 244, 252))
    draw.ellipse((620, -160, 1180, 380), fill=(226, 238, 252))

    title_font = _load_font(68, bold=True)
    subtitle_font = _load_font(26, bold=False)
    card_title_font = _load_font(34, bold=True)
    row_font = _load_font(22, bold=False)
    row_bold = _load_font(22, bold=True)
    pct_font = _load_font(21, bold=True)
    small_font = _load_font(20, bold=False)
    compare_big = _load_font(32, bold=True)

    pad = 48
    content_w = width - pad * 2
    y = 62
    public_texts: list[str] = []

    header_title = "Мой портфель"
    header_sub = datetime.now(MSK_TZ).strftime("%d.%m.%Y")
    draw.text((pad, y), header_title, fill=(31, 45, 80), font=title_font)
    draw.text((pad, y + 78), header_sub, fill=(106, 123, 151), font=subtitle_font)
    public_texts.extend([header_title, header_sub])
    y += 132

    # 2) Состав портфеля
    comp_h = 620
    comp_rect = (pad, y, pad + content_w, y + comp_h)
    draw_glass_card(comp_rect)
    cx1, cy1, cx2, cy2 = comp_rect
    draw.text((cx1 + 24, cy1 + 20), "Состав портфеля", fill=(39, 55, 90), font=card_title_font)
    header_sep_y = cy1 + 66
    draw.line((cx1 + 22, header_sep_y, cx2 - 22, header_sep_y), fill=(220, 230, 244), width=1)
    list_y = header_sep_y + 12
    row_h = 25
    name_col_w = 430
    bar_col_w = 230
    month_col_x = cx2 - 120
    for idx, item in enumerate(composition_rows[:20]):
        base_y = list_y + idx * row_h
        if base_y + row_h > cy2 - 12:
            break
        name = str(item.get("name_ru") or "").strip() or str(item.get("secid") or "UNKNOWN")
        share_pct = float(item.get("share_pct") or 0.0)
        ret_30 = item.get("ret_30d")
        pname = _fit_line(draw, name, row_font, name_col_w)
        draw.text((cx1 + 24, base_y), pname, fill=(57, 74, 106), font=row_font)
        level = weight_level(share_pct)
        ratio = weight_ratio(level)
        bar_x = cx1 + 24 + name_col_w + 20
        bar_y = base_y + 7
        draw.rounded_rectangle((bar_x, bar_y, bar_x + bar_col_w, bar_y + 11), radius=7, fill=(225, 234, 246))
        fill_w = int(bar_col_w * ratio)
        draw.rounded_rectangle((bar_x, bar_y, bar_x + fill_w, bar_y + 11), radius=7, fill=(100, 140, 198))
        share_text = f"{share_pct:.1f}%"
        draw.text((bar_x + bar_col_w + 8, base_y - 1), share_text, fill=(116, 131, 158), font=small_font)
        mtxt = pct_text(ret_30)
        mcolor = tint_by_trend(ret_30)
        mt = _fit_line(draw, mtxt, pct_font, cx2 - month_col_x - 16)
        tw = int(draw.textlength(mt, font=pct_font))
        draw.text((cx2 - 18 - tw, base_y - 1), mt, fill=mcolor, font=pct_font)
        if idx < min(19, len(composition_rows) - 1):
            draw.line((cx1 + 24, base_y + row_h - 2, cx2 - 24, base_y + row_h - 2), fill=(231, 238, 248), width=1)
        public_texts.extend([name, pname, share_text, mt])
    y += comp_h + 20

    # 3 + 4) Аллокация и Динамика
    left_w = int(content_w * 0.43)
    right_w = content_w - left_w - 20
    block_h = 300
    alloc_rect = (pad, y, pad + left_w, y + block_h)
    dyn_rect = (pad + left_w + 20, y, pad + left_w + 20 + right_w, y + block_h)
    draw_glass_card(alloc_rect)
    draw_glass_card(dyn_rect)

    ax1, ay1, ax2, ay2 = alloc_rect
    draw.text((ax1 + 20, ay1 + 18), "Аллокация", fill=(39, 55, 90), font=card_title_font)
    cat_map = {"stock": "Акции", "metal": "Металлы", "fiat": "Другое"}
    alloc = {"Акции": 0.0, "Металлы": 0.0, "Другое": 0.0}
    for item in composition_rows:
        share = float(item.get("share_pct") or 0.0)
        key = cat_map.get(str(item.get("asset_type") or "").lower(), "Другое")
        alloc[key] += share
    colors = {"Акции": (80, 143, 226), "Металлы": (46, 178, 133), "Другое": (167, 123, 232)}
    donut_center = (ax1 + 110, ay1 + 175)
    donut_r = 72
    start = -90
    total = sum(alloc.values()) or 100.0
    for label in ("Акции", "Металлы", "Другое"):
        val = alloc[label]
        sweep = 360.0 * (val / total)
        draw.pieslice(
            (
                donut_center[0] - donut_r,
                donut_center[1] - donut_r,
                donut_center[0] + donut_r,
                donut_center[1] + donut_r,
            ),
            start=start,
            end=start + sweep,
            fill=colors[label],
        )
        start += sweep
    draw.ellipse(
        (
            donut_center[0] - 42,
            donut_center[1] - 42,
            donut_center[0] + 42,
            donut_center[1] + 42,
        ),
        fill=alpha((250, 252, 255), 240),
    )
    ly = ay1 + 106
    for label in ("Акции", "Металлы", "Другое"):
        pct = f"{alloc[label]:.1f}%"
        draw.rounded_rectangle((ax1 + 205, ly + 5, ax1 + 219, ly + 19), radius=4, fill=colors[label])
        lname = _fit_line(draw, label, row_font, 110)
        draw.text((ax1 + 227, ly), lname, fill=(61, 77, 106), font=row_font)
        pct_fit = _fit_line(draw, pct, row_bold, 84)
        draw.text((ax2 - 18 - int(draw.textlength(pct_fit, font=row_bold)), ly), pct_fit, fill=(61, 77, 106), font=row_bold)
        public_texts.extend([lname, pct_fit])
        ly += 42

    dx1, dy1, dx2, dy2 = dyn_rect
    draw.text((dx1 + 20, dy1 + 18), "Динамика", fill=(39, 55, 90), font=card_title_font)
    draw.text((dx1 + 20, dy1 + 64), "Топ 3 лучших активов", fill=(96, 112, 141), font=small_font)
    ly = dy1 + 95
    for item in top_gainers[:3]:
        name = str(item.get("shortname") or item.get("secid") or "UNKNOWN").strip()
        pct = float(item.get("pnl_pct") or 0.0)
        pname = _fit_line(draw, name, row_font, right_w - 210)
        chip = f"▲ {pct_text(pct)}"
        chip = _fit_line(draw, chip, pct_font, 140)
        c = tint_by_trend(pct)
        draw.text((dx1 + 20, ly), pname, fill=(54, 71, 103), font=row_font)
        tw = int(draw.textlength(chip, font=pct_font))
        draw.text((dx2 - 18 - tw, ly), chip, fill=c, font=pct_font)
        public_texts.extend([name, pname, chip])
        ly += 34
    draw.line((dx1 + 20, ly + 4, dx2 - 20, ly + 4), fill=(226, 236, 248), width=1)
    draw.text((dx1 + 20, ly + 14), "Топ 3 худших активов", fill=(96, 112, 141), font=small_font)
    ly += 45
    for item in top_losers[:3]:
        name = str(item.get("shortname") or item.get("secid") or "UNKNOWN").strip()
        pct = float(item.get("pnl_pct") or 0.0)
        pname = _fit_line(draw, name, row_font, right_w - 210)
        chip = f"▼ {pct_text(-abs(pct))}"
        chip = _fit_line(draw, chip, pct_font, 140)
        c = tint_by_trend(-abs(pct))
        draw.text((dx1 + 20, ly), pname, fill=(54, 71, 103), font=row_font)
        tw = int(draw.textlength(chip, font=pct_font))
        draw.text((dx2 - 18 - tw, ly), chip, fill=c, font=pct_font)
        public_texts.extend([name, pname, chip])
        ly += 34

    y += block_h + 20

    # 5) Сравнение с индексом MOEX
    cmp_h = 192
    cmp_rect = (pad, y, pad + content_w, y + cmp_h)
    draw_glass_card(cmp_rect)
    kx1, ky1, kx2, ky2 = cmp_rect
    draw.text((kx1 + 22, ky1 + 18), "Сравнение с индексом MOEX (30 дней)", fill=(39, 55, 90), font=card_title_font)
    port_txt = pct_text(portfolio_return_30d)
    moex_txt = pct_text(moex_return_30d)
    alpha_v = None
    if portfolio_return_30d is not None and moex_return_30d is not None:
        alpha_v = float(portfolio_return_30d) - float(moex_return_30d)
    alpha_val = "0.00%" if alpha_v is None else pct_text(alpha_v)
    alpha_label = "Разница "
    line_w = (kx2 - kx1) - 44
    l1, v1 = draw_label_value_line(
        kx1 + 22,
        ky1 + 72,
        "Портфель: ",
        port_txt,
        compare_big,
        compare_big,
        (54, 71, 103),
        tint_by_trend(portfolio_return_30d),
        line_w,
    )
    l2, v2 = draw_label_value_line(
        kx1 + 22,
        ky1 + 111,
        "MOEX (IMOEX): ",
        moex_txt,
        compare_big,
        compare_big,
        (54, 71, 103),
        tint_by_trend(moex_return_30d),
        line_w,
    )
    l3, v3 = draw_label_value_line(
        kx1 + 22,
        ky1 + 150,
        alpha_label,
        alpha_val,
        compare_big,
        compare_big,
        (54, 71, 103),
        tint_by_trend(alpha_v),
        line_w,
    )
    public_texts.extend([l1 + v1, l2 + v2, l3 + v3])

    validate_public_text_policy(public_texts)

    out = io.BytesIO()
    image.convert("RGB").save(out, format="PNG")
    return out.getvalue()
