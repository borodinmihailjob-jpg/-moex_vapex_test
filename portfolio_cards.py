import io
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
    width = 2200
    height = 3000
    image = Image.new("RGB", (width, height), (241, 245, 252))
    draw = ImageDraw.Draw(image)

    title_font = _load_font(74, bold=True)
    h_font = _load_font(42, bold=True)
    text_font = _load_font(30, bold=False)
    metric_font = _load_font(56, bold=True)
    ticker_font = _load_font(22, bold=False)

    draw.ellipse((-220, -260, 540, 420), fill=(233, 244, 255))
    draw.ellipse((width - 520, -180, width + 140, 420), fill=(223, 251, 242))

    pad = 56
    content_w = width - 2 * pad
    y = pad

    def card(x: int, top: int, w: int, h: int, title: str | None = None) -> int:
        draw.rounded_rectangle((x, top, x + w, top + h), radius=30, fill=(255, 255, 255), outline=(224, 233, 246), width=2)
        if title:
            draw.text((x + 24, top + 18), title, fill=(35, 49, 84), font=h_font)
            return top + 84
        return top + 18

    header_h = 190
    hy = card(pad, y, content_w, header_h)
    draw.text((pad + 26, hy - 6), "Мой портфель на MOEX", fill=(27, 38, 66), font=title_font)
    draw.text((pad + 28, hy + 78), datetime.now(MSK_TZ).strftime("Снимок на %d.%m.%Y %H:%M МСК"), fill=(108, 128, 162), font=text_font)
    badge_x = pad + content_w - 230
    draw.rounded_rectangle((badge_x, hy + 14, badge_x + 180, hy + 74), radius=18, fill=(230, 251, 243))
    draw.text((badge_x + 28, hy + 28), "SHARE", fill=(28, 151, 104), font=text_font)
    y += header_h + 24

    kpi_h = 220
    kpi_gap = 18
    kpi_w = (content_w - 2 * kpi_gap) // 3
    p30_text = "н/д" if portfolio_return_30d is None else f"{portfolio_return_30d:+.2f}%"
    p30_color = (29, 163, 108) if (portfolio_return_30d or 0.0) >= 0 else (218, 78, 95)
    m30_text = "н/д" if moex_return_30d is None else f"{moex_return_30d:+.2f}%"
    m30_color = (29, 163, 108) if (moex_return_30d or 0.0) >= 0 else (218, 78, 95)
    alpha = None
    if portfolio_return_30d is not None and moex_return_30d is not None:
        alpha = portfolio_return_30d - moex_return_30d

    for i in range(3):
        x = pad + i * (kpi_w + kpi_gap)
        cy = card(x, y, kpi_w, kpi_h)
        if i == 0:
            draw.text((x + 26, cy + 4), "Рост портфеля (30д)", fill=(97, 117, 149), font=text_font)
            draw.text((x + 26, cy + 70), p30_text, fill=p30_color, font=metric_font)
        elif i == 1:
            draw.text((x + 26, cy + 4), "Индекс MOEX (30д)", fill=(97, 117, 149), font=text_font)
            draw.text((x + 26, cy + 70), m30_text, fill=m30_color, font=metric_font)
        else:
            draw.text((x + 26, cy + 4), "Сравнение", fill=(97, 117, 149), font=text_font)
            if alpha is None:
                atxt, acolor = "н/д", (139, 155, 182)
            elif alpha >= 0:
                atxt, acolor = f"Опережение +{alpha:.2f}%", (29, 163, 108)
            else:
                atxt, acolor = f"Отставание {alpha:.2f}%", (218, 78, 95)
            metric = _load_font(42, bold=True)
            draw.text((x + 26, cy + 72), _fit_line(draw, atxt, metric, kpi_w - 52), fill=acolor, font=metric)
    y += kpi_h + 24

    table_w = int(content_w * 0.68)
    right_w = content_w - table_w - 18
    table_h = 1180
    chart_h = table_h

    ty = card(pad, y, table_w, table_h, "Состав портфеля")
    col1_x = pad + 24
    col2_x = pad + table_w - 420
    col3_x = pad + table_w - 132
    draw.text((col1_x, ty), "Название актива", fill=(109, 128, 162), font=ticker_font)
    draw.text((col2_x, ty), "Доля", fill=(109, 128, 162), font=ticker_font)
    draw.text((col3_x, ty), "Месяц", fill=(109, 128, 162), font=ticker_font)
    draw.line((pad + 22, ty + 34, pad + table_w - 22, ty + 34), fill=(231, 238, 249), width=2)
    ty += 48
    for item in composition_rows[:20]:
        share_pct = float(item.get("share_pct") or 0.0)
        secid = str(item.get("secid") or "UNKNOWN")
        name = str(item.get("name_ru") or "").strip() or secid
        ret_30 = item.get("ret_30d")
        draw.text((col1_x, ty), _fit_line(draw, f"{name} ({secid})", text_font, col2_x - col1_x - 20), fill=(42, 56, 88), font=text_font)
        bar_x = col2_x
        bar_w = 170
        bar_h = 18
        bar_y = ty + 9
        fill_w = int(max(0.0, min(1.0, share_pct / 100.0)) * bar_w)
        draw.rounded_rectangle((bar_x, bar_y, bar_x + bar_w, bar_y + bar_h), radius=10, fill=(233, 240, 250))
        if fill_w > 0:
            draw.rounded_rectangle((bar_x, bar_y, bar_x + fill_w, bar_y + bar_h), radius=10, fill=(82, 148, 233))
        draw.text((bar_x + bar_w + 10, ty + 1), f"{share_pct:.1f}%", fill=(97, 117, 149), font=ticker_font)
        if ret_30 is None:
            rtxt, rcol = "н/д", (140, 156, 181)
        else:
            rv = float(ret_30)
            rtxt = f"{rv:+.2f}%"
            rcol = (29, 163, 108) if rv >= 0 else (218, 78, 95)
        draw.text((col3_x, ty), rtxt, fill=rcol, font=text_font)
        ty += 52
        if ty > y + table_h - 52:
            break

    rx = pad + table_w + 18
    cy = card(rx, y, right_w, chart_h, "Динамика")
    draw.text((rx + 24, cy + 4), "Top-3 актива", fill=(97, 117, 149), font=text_font)
    ly = cy + 50
    for item in top_gainers[:3]:
        name = str(item.get("shortname") or "").strip() or str(item.get("secid") or "UNKNOWN")
        draw.text((rx + 24, ly), _fit_line(draw, f"• {name}", text_font, right_w - 150), fill=(42, 56, 88), font=text_font)
        draw.text((rx + right_w - 116, ly), f"{item['pnl_pct']:+.2f}%", fill=(29, 163, 108), font=text_font)
        ly += 44
    if not top_gainers:
        draw.text((rx + 24, ly), "• н/д", fill=(140, 156, 181), font=text_font)
        ly += 44

    draw.text((rx + 24, ly + 12), "Top-3 убыточных", fill=(97, 117, 149), font=text_font)
    ly += 56
    for item in top_losers[:3]:
        name = str(item.get("shortname") or "").strip() or str(item.get("secid") or "UNKNOWN")
        draw.text((rx + 24, ly), _fit_line(draw, f"• {name}", text_font, right_w - 150), fill=(42, 56, 88), font=text_font)
        draw.text((rx + right_w - 116, ly), f"{item['pnl_pct']:+.2f}%", fill=(218, 78, 95), font=text_font)
        ly += 44
    if not top_losers:
        draw.text((rx + 24, ly), "• н/д", fill=(140, 156, 181), font=text_font)
        ly += 44

    spark_top = y + chart_h - 360
    draw.text((rx + 24, spark_top), "Тренд изменения (условная шкала)", fill=(97, 117, 149), font=ticker_font)
    plot_x1 = rx + 24
    plot_y1 = spark_top + 44
    plot_x2 = rx + right_w - 24
    plot_y2 = y + chart_h - 34
    draw.rounded_rectangle((plot_x1, plot_y1, plot_x2, plot_y2), radius=18, fill=(247, 250, 255), outline=(226, 234, 246), width=2)
    vals = [float(r["ret_30d"]) for r in composition_rows if r.get("ret_30d") is not None][:12]
    if len(vals) < 3:
        vals = [0.4, 1.1, 0.8, 1.6, 1.3, 1.9, 1.5, 2.0]
    mn = min(vals)
    mx = max(vals)
    span = max(1e-9, mx - mn)
    points: list[tuple[int, int]] = []
    for i, v in enumerate(vals):
        px = int(plot_x1 + 20 + i * (plot_x2 - plot_x1 - 40) / max(1, len(vals) - 1))
        py = int(plot_y2 - 20 - ((v - mn) / span) * (plot_y2 - plot_y1 - 40))
        points.append((px, py))
    for i in range(1, len(points)):
        draw.line((points[i - 1], points[i]), fill=(82, 148, 233), width=5)
    for px, py in points:
        draw.ellipse((px - 5, py - 5, px + 5, py + 5), fill=(82, 148, 233))

    y += table_h + 24

    bottom_h = 300
    by = card(pad, y, content_w, bottom_h, "Сравнение с индексом MOEX (30 дней)")
    moex_text = "н/д" if moex_return_30d is None else f"{moex_return_30d:+.2f}%"
    port_text = "н/д" if portfolio_return_30d is None else f"{portfolio_return_30d:+.2f}%"
    draw.text((pad + 24, by + 6), f"Портфель: {port_text}", fill=(42, 56, 88), font=text_font)
    draw.text((pad + 24, by + 54), f"MOEX (IMOEX): {moex_text}", fill=(42, 56, 88), font=text_font)
    if alpha is None:
        alpha_text, alpha_color = "Сравнение: н/д", (140, 156, 181)
    elif alpha >= 0:
        alpha_text, alpha_color = f"Опережение +{alpha:.2f}%", (29, 163, 108)
    else:
        alpha_text, alpha_color = f"Отставание {alpha:.2f}%", (218, 78, 95)
    draw.text((pad + 24, by + 118), alpha_text, fill=alpha_color, font=metric_font)

    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return buf.getvalue()
