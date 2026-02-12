from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from defusedxml import ElementTree as ET

from moex_iss import ASSET_TYPE_METAL, ASSET_TYPE_STOCK

METAL_ISIN_TO_SECID = {
    "GLD": "GLDRUB_TOM",
}


@dataclass
class ParsedBrokerTrade:
    trade_no: str
    trade_date: str
    db_time: str
    isin_reg: str
    asset_name: str
    qty: float
    price: float
    commission: float
    asset_type: str
    metal_secid: str | None = None


def _parse_float(value: str | None) -> float:
    raw = (value or "").strip().replace(",", ".")
    return float(raw) if raw else 0.0


def _text(node: ET.Element, tag: str) -> str:
    child = node.find(tag)
    if child is None or child.text is None:
        return ""
    return child.text.strip()


def _to_ddmmyyyy(ts: str) -> str:
    # Input format in statement: 2026-01-29T13:24:48
    return datetime.fromisoformat(ts).strftime("%d.%m.%Y")


def parse_broker_report_xml(xml_bytes: bytes) -> list[ParsedBrokerTrade]:
    try:
        xml_text = xml_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("Файл не похож на UTF-8 XML выписку") from exc

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ValueError("Не удалось разобрать XML") from exc

    if root.tag != "report_broker":
        raise ValueError("Неподдерживаемый формат файла: корневой тег не report_broker")

    out: list[ParsedBrokerTrade] = []
    for node in root.findall("./trades_finished/trade"):
        trade_no = _text(node, "trade_no")
        db_time = _text(node, "db_time")
        isin_reg = _text(node, "isin_reg").upper()
        asset_name = _text(node, "p_name")
        qty = _parse_float(_text(node, "qty"))
        price = _parse_float(_text(node, "Price"))
        commission = _parse_float(_text(node, "bank_tax"))

        if not trade_no or not db_time or not isin_reg:
            continue
        if abs(qty) <= 1e-12:
            continue
        if price <= 0:
            continue

        metal_secid = METAL_ISIN_TO_SECID.get(isin_reg)
        asset_type = ASSET_TYPE_METAL if metal_secid else ASSET_TYPE_STOCK

        out.append(
            ParsedBrokerTrade(
                trade_no=trade_no,
                trade_date=_to_ddmmyyyy(db_time),
                db_time=db_time,
                isin_reg=isin_reg,
                asset_name=asset_name,
                qty=qty,
                price=price,
                commission=max(0.0, commission),
                asset_type=asset_type,
                metal_secid=metal_secid,
            )
        )

    out.sort(key=lambda x: (x.db_time, x.trade_no))
    return out
