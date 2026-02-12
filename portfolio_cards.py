import io
from datetime import datetime
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFont

MSK_TZ = ZoneInfo("Europe/Moscow")


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
    neutral = (45, 57, 70)
    green = (0, 200, 83)
    red = (255, 23, 68)
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


def _treemap_partition(
    items: list[dict],
    x: float,
    y: float,
    w: float,
    h: float,
) -> list[tuple[dict, tuple[int, int, int, int]]]:
    if not items or w <= 0 or h <= 0:
        return []
    if len(items) == 1:
        return [(items[0], (int(x), int(y), int(x + w), int(y + h)))]

    total = sum(max(1e-9, float(i["weight"])) for i in items)
    acc = 0.0
    split_idx = 1
    half = total / 2.0
    for i, item in enumerate(items, 1):
        acc += max(1e-9, float(item["weight"]))
        if acc >= half:
            split_idx = i
            break
    left = items[:split_idx]
    right = items[split_idx:] or items[-1:]
    left_sum = sum(max(1e-9, float(i["weight"])) for i in left)
    ratio = left_sum / total if total > 0 else 0.5

    if w >= h:
        left_w = w * ratio
        return _treemap_partition(left, x, y, left_w, h) + _treemap_partition(right, x + left_w, y, w - left_w, h)
    top_h = h * ratio
    return _treemap_partition(left, x, y, w, top_h) + _treemap_partition(right, x, y + top_h, w, h - top_h)


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


def build_portfolio_map_png(tiles: list[dict]) -> bytes:
    width = 2800
    height = 1700
    pad = 16
    gap = 4
    bg = (242, 244, 247)
    image = Image.new("RGB", (width, height), color=bg)
    draw = ImageDraw.Draw(image)

    title_font = _load_font(66, bold=True)
    small_title_font = _load_font(34, bold=True)
    secid_font = _load_font(42, bold=True)
    name_font = _load_font(29, bold=False)
    value_font = _load_font(42, bold=True)
    stat_font = _load_font(30, bold=True)

    total_value = sum(float(t["value"]) for t in tiles)
    header_h = 145
    draw.text((pad, 12), "Карта портфеля", fill=(32, 36, 40), font=title_font)
    draw.text(
        (pad, 86),
        f"Инструментов: {len(tiles)}   Общая стоимость: {money(total_value)} RUB",
        fill=(82, 90, 98),
        font=small_title_font,
    )

    chart_x = pad
    chart_y = header_h
    chart_w = width - pad * 2
    chart_h = height - header_h - pad

    sorted_tiles = sorted(tiles, key=lambda x: x["weight"], reverse=True)
    placements = _treemap_partition(sorted_tiles, chart_x, chart_y, chart_w, chart_h)

    for tile, rect in placements:
        x1, y1, x2, y2 = rect
        w0 = x2 - x1
        h0 = y2 - y1
        local_gap = gap if min(w0, h0) >= 20 else 1
        x1 += local_gap
        y1 += local_gap
        x2 -= local_gap
        y2 -= local_gap
        if x2 <= x1 or y2 <= y1:
            x2 = max(x2, x1 + 1)
            y2 = max(y2, y1 + 1)

        bg_color = _tile_color_by_pnl_pct(tile.get("pnl_pct"))
        fg_color = _text_color_for_bg(bg_color)
        draw.rectangle((x1, y1, x2, y2), fill=bg_color)

        inner_w = x2 - x1
        inner_h = y2 - y1
        if inner_w < 70 or inner_h < 46:
            continue
        px = x1 + 10
        py = y1 + 8

        secid = _fit_line(draw, str(tile["secid"]), secid_font, inner_w - 20)
        secid_h = _text_height(draw, secid, secid_font)
        if inner_w >= 180 and inner_h >= max(70, secid_h + 18):
            draw.text((px, py), secid, fill=fg_color, font=secid_font)
            py += secid_h + 6
        shortname = _fit_line(draw, str(tile.get("shortname") or ""), name_font, inner_w - 20)
        name_h = _text_height(draw, shortname, name_font) if shortname else 0
        if shortname and inner_w >= 220 and (y1 + inner_h - py) >= (name_h + 18):
            draw.text((px, py), shortname, fill=fg_color, font=name_font)
            py += name_h + 6

        if inner_w >= 240 and inner_h >= 150:
            val = f"{money(float(tile['value']))} RUB"
            draw.text(
                (px, max(py + 6, y1 + inner_h - 72)),
                _fit_line(draw, val, value_font, inner_w - 20),
                fill=fg_color,
                font=value_font,
            )

        pnl_pct = tile.get("pnl_pct")
        if inner_w >= 190 and inner_h >= 92:
            stat = "P&L: н/д" if pnl_pct is None else f"P&L {pnl_pct:+.2f}%"
            stat_w = int(draw.textlength(stat, font=stat_font))
            if stat_w <= (inner_w - 24):
                draw.text((x1 + inner_w - 12 - stat_w, y1 + 10), stat, fill=fg_color, font=stat_font)

    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return buf.getvalue()


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
