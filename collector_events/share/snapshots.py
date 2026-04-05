from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from forex_shared.database import get_db
from forex_shared.models import CurrencyStrength


def _safe_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def build_currency_strength_snapshot(currencies: list[str] | None = None) -> dict[str, Any]:
    db = next(get_db())
    try:
        rows: list[CurrencyStrength] = []
        currency_list = currencies or []

        if currency_list:
            for currency in currency_list:
                row = (
                    db.query(CurrencyStrength)
                    .filter(CurrencyStrength.currency == currency)
                    .order_by(CurrencyStrength.calculated_at.desc())
                    .first()
                )
                if row is not None:
                    rows.append(row)
        else:
            # Best-effort: take the latest row for every currency found.
            all_currencies = [r[0] for r in db.query(CurrencyStrength.currency).distinct().all()]
            for currency in all_currencies:
                row = (
                    db.query(CurrencyStrength)
                    .filter(CurrencyStrength.currency == currency)
                    .order_by(CurrencyStrength.calculated_at.desc())
                    .first()
                )
                if row is not None:
                    rows.append(row)

        items = [
            {
                "currency": r.currency,
                "strength_score": float(r.strength_score or 0.0),
                "momentum": float(r.momentum or 0.0),
                "trend": r.trend,
                "confidence": float(r.confidence or 0.0),
                "calculated_at": _safe_iso(r.calculated_at),
            }
            for r in rows
        ]

        as_of = None
        if rows:
            as_of = _safe_iso(max((r.calculated_at for r in rows if r.calculated_at is not None), default=None))

        items_sorted = sorted(items, key=lambda x: x["strength_score"], reverse=True)
        top_strong = items_sorted[:3]
        top_weak = list(reversed(items_sorted[-3:])) if len(items_sorted) >= 3 else list(reversed(items_sorted))

        return {
            "type": "currency_strength_snapshot",
            "as_of": as_of,
            "items": items_sorted,
            "top": {"strongest": top_strong, "weakest": top_weak},
        }
    finally:
        db.close()

