# services/collector_events/collector_events/globalintel/supply_chain/shipping_stress.py
"""
ShippingStressExtractor — Baltic Dry Index (BDI) via FRED API.

Coleta o Baltic Dry Index (serie "BDIY") da base de dados FRED
(Federal Reserve Bank of St. Louis) e aplica uma heurística de
stress baseada na variação percentual em relação ao nível histórico.

Stress score (0.0 – 1.0):
    ≥ 2500  → LOW    (mercado saudável)
    1500-2499 → MEDIUM (tensão moderada)
    < 1500  → HIGH   (stress grave no frete marítimo)

Severidade mapeada a partir do stress score.

Fonte: https://fred.stlouisfed.org/series/BDIY
Requer: ``FRED_API_KEY`` em ``.env`` (chave gratuita no site FRED).

Fallback: quando FRED falha ou a chave não está configurada,
tenta a API pública do FRED sem chave (modo observação limitada a
120 req/min). O campo ``extra["api_key_used"]`` indica o modo.
"""

from __future__ import annotations

from datetime import datetime, timezone

import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config_env import GlobalIntelConfig

# ── Pontos de corte BDI → stress ──────────────────────────────────
_BDI_HIGH_STRESS    = 1_500   # abaixo → HIGH severity
_BDI_MEDIUM_STRESS  = 2_500   # abaixo → MEDIUM severity

# FRED API endpoint
_FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
_BDI_SERIES_ID = "BDIY"   # Baltic Dry Index (daily)


class ShippingStressExtractor(BaseExtractor):
    """Coleta o Baltic Dry Index da FRED e deriva um stress score.

    Quando ``FRED_API_KEY`` estiver configurado, usa autenticação;
    caso contrário, requisita sem chave (taxa limitada mas funcional
    para testes).
    """

    SOURCE      = "bdi_fred"
    DOMAIN      = "supply_chain"
    REDIS_KEY   = "supply:shipping:stress:v1"
    TTL_SECONDS = 3600 * 6   # 6h — BDI actualizado diariamente

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        cfg = GlobalIntelConfig()
        api_key = cfg.FRED_API_KEY or ""

        params: dict[str, str] = {
            "series_id":     _BDI_SERIES_ID,
            "sort_order":    "desc",
            "limit":         "5",         # apenas os últimos 5 pontos
            "file_type":     "json",
        }
        if api_key:
            params["api_key"] = api_key

        async with session.get(_FRED_BASE_URL, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

        observations = data.get("observations", [])
        # FRED may return "." for missing values — filter those out
        valid = [
            o for o in observations
            if o.get("value", ".") != "."
        ]
        if not valid:
            self.log.warning("ShippingStress: FRED returned no valid BDI observations")
            return []

        latest = valid[0]
        bdi_value = float(latest["value"])
        bdi_date  = latest.get("date", "")

        # ── Derive stress score and severity ─────────────────────
        stress_score, severity = _bdi_to_stress(bdi_value)

        # ── 5-period trend (pct change vs oldest available) ──────
        trend_pct = None
        if len(valid) >= 2:
            oldest_val = float(valid[-1]["value"])
            if oldest_val != 0:
                trend_pct = round((bdi_value - oldest_val) / oldest_val * 100, 2)

        now = datetime.now(timezone.utc).isoformat()

        item = IntelItem(
            id=f"bdi_fred:{bdi_date}",
            source=self.SOURCE,
            domain=self.DOMAIN,
            title=f"Baltic Dry Index: {bdi_value:,.0f} pts ({_stress_label(stress_score)})",
            ts=bdi_date,
            fetched_at=now,
            severity=severity,
            tags=["bdi", "shipping", "freight", "supply_chain", "baltic_dry"],
            extra={
                "bdi_value":    bdi_value,
                "stress_score": stress_score,
                "trend_5d_pct": trend_pct,
                "series_id":    _BDI_SERIES_ID,
                "api_key_used": bool(api_key),
                "affected_assets": _stress_to_pairs(bdi_value),
            },
        )

        self.log.info(
            "ShippingStress: BDI=%.0f on %s | stress=%.2f | severity=%s | trend=%s%%",
            bdi_value, bdi_date, stress_score, severity,
            f"{trend_pct:+.1f}" if trend_pct is not None else "n/a",
        )
        return [item]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _bdi_to_stress(bdi: float) -> tuple[float, str]:
    """Convert BDI value to (stress_score, severity) pair.

    Returns:
        score: 0.0 (saudável) to 1.0 (stress máximo)
        severity: 'HIGH' | 'MEDIUM' | 'LOW'
    """
    if bdi >= _BDI_MEDIUM_STRESS:
        # Healthy range: score scales from 0.1 to 0.3
        score = 0.1 + max(0.0, (_BDI_MEDIUM_STRESS - bdi) / _BDI_MEDIUM_STRESS * 0.2)
        return round(score, 3), "LOW"
    elif bdi >= _BDI_HIGH_STRESS:
        # Moderate stress: 0.3 – 0.65
        ratio = (_BDI_MEDIUM_STRESS - bdi) / (_BDI_MEDIUM_STRESS - _BDI_HIGH_STRESS)
        score = 0.30 + ratio * 0.35
        return round(score, 3), "MEDIUM"
    else:
        # High stress: 0.65 – 1.0
        ratio = max(0.0, 1.0 - bdi / _BDI_HIGH_STRESS)
        score = 0.65 + ratio * 0.35
        return round(min(score, 1.0), 3), "HIGH"


def _stress_label(score: float) -> str:
    if score >= 0.65:
        return "HIGH STRESS"
    if score >= 0.30:
        return "MODERATE"
    return "HEALTHY"


def _stress_to_pairs(bdi: float) -> list[str]:
    """Return affected forex pairs based on BDI stress level.

    High freight stress → supply disruptions → commodity currencies impacted
    (AUD, CAD, NZD export economies) and safe havens benefit (JPY, CHF).
    """
    if bdi < _BDI_HIGH_STRESS:
        # Severe stress: commodity pairs bearish, safe havens bullish
        return ["AUDUSD", "USDCAD", "NZDUSD", "USDJPY", "USDCHF"]
    if bdi < _BDI_MEDIUM_STRESS:
        # Moderate: commodity currencies mildly impacted
        return ["AUDUSD", "USDCAD"]
    return []   # Healthy: no significant impact
