# services/collector_events/collector_events/globalintel/global_tag_manager.py
"""
GlobalTagManager — converte Intel items de alto impacto em GlobalTag entries.

Responsabilidades:
    1. Receber ``ExtractionResult`` (via ``IntelOrchestrator._run_entry()``)
    2. Filtrar itens de severidade HIGH (configurável)
    3. Inferir ativo forex afetado (via ``extra["affected_assets"]`` ou mapeamento interno)
    4. Criar/atualizar ``GlobalTag`` no Redis com TTL
    5. Publicar ``GLOBAL_TAG_UPDATED`` no tópico ``intel.global_tags`` via MQ

Uso::

    from collector_events.globalintel.global_tag_manager import GlobalTagManager

    tag_mngr = GlobalTagManager(redis_client=redis, mq=mq_provider)
    tags = await tag_mngr.process_result(extraction_result)

Ciclo de vida de uma GlobalTag:
    IntelItem (HIGH severity) → GlobalTag (Redis + MQ) → expira via TTL do Redis

O Redis gerencia o TTL automaticamente — não há polling de expiração neste módulo.
Para publicar eventos EXPIRED, um processo separado (ou WebSocket bridge) deve
monitorar as chaves Redis com ``KEYSPACE_NOTIFICATION``.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import redis.asyncio as aioredis

from forex_shared.domain.intel import (
    ExtractionResult,
    GlobalTag,
    IntelBias,
    IntelItem,
    IntelSeverity,
)
from forex_shared.logging.loggable import Loggable
from forex_shared.providers.mq.topics import IntelTopics


class GlobalTagManager(Loggable):
    """Converte intel items de alto impacto em GlobalTag entries (Redis + MQ).

    O ``GlobalTagManager`` é chamado pelo ``IntelOrchestrator`` após cada
    extração bem-sucedida. Itens com severidade ``HIGH`` (por padrão) são
    convertidos em ``GlobalTag`` e armazenados no Redis com um TTL por domínio.

    Se um ``MQProviderAsync`` for injetado, cada tag criada é publicada
    imediatamente no tópico ``intel.global_tags`` com ``event_type: GLOBAL_TAG_UPDATED``.

    Mapeamento item → ativo forex:
        1. ``item.extra["affected_assets"]`` — lista explícita de pares (prioridade)
        2. ``item.country`` — mapeado para moeda base/quote via ``COUNTRY_CURRENCY_MAP``
        3. ``item.tags`` — pesquisa textual de países/moedas nos tags

    Bias e risk_score:
        - ``item.extra["bias"]`` — bias explícito (prioridade)
        - ``item.extra["risk_score"]`` — score explícito (prioridade)
        - Fallback: ``DOMAIN_BIAS`` e ``SEVERITY_SCORE`` por domínio/severidade
    """

    # ── TTL por domínio (segundos) ─────────────────────────────────────────
    DOMAIN_TTL: dict[str, int] = {
        "conflict":    6 * 3600,    # 6h  — conflitos evoluem rapidamente
        "cyber":       4 * 3600,    # 4h  — ameaças cyber têm janela curta
        "economic":    12 * 3600,   # 12h — dados macro duram mais
        "environment": 6 * 3600,    # 6h  — desastres naturais
        "sanctions":   48 * 3600,   # 48h — sanções têm impacto prolongado
        "social":      2 * 3600,    # 2h  — sentimento muda rápido
        "trade":       24 * 3600,   # 24h — restrições comerciais duram mais
        "supply_chain": 12 * 3600,  # 12h
        "advisories":  12 * 3600,   # 12h
        "feeds":       1 * 3600,    # 1h  — feeds genéricos
        "market":      4 * 3600,    # 4h  — eventos de mercado
        "reference":   24 * 3600,   # 24h
    }
    _DEFAULT_TTL = 6 * 3600

    # ── Risk score por severidade ──────────────────────────────────────────
    SEVERITY_SCORE: dict[str, float] = {
        IntelSeverity.HIGH:   0.85,
        IntelSeverity.MEDIUM: 0.55,
        IntelSeverity.LOW:    0.25,
        "":                   0.30,
    }

    # ── Bias padrão por domínio ────────────────────────────────────────────
    # Eventos de conflito, cyber e sanções tendem a ser bearish para moedas afetadas;
    # eventos econômicos e de mercado são neutros até análise adicional.
    DOMAIN_BIAS: dict[str, str] = {
        "conflict":    IntelBias.BEARISH,
        "cyber":       IntelBias.BEARISH,
        "sanctions":   IntelBias.BEARISH,
        "supply_chain": IntelBias.BEARISH,
        "environment": IntelBias.BEARISH,
        "advisories":  IntelBias.BEARISH,
        "trade":       IntelBias.BEARISH,
        "economic":    IntelBias.NEUTRAL,
        "market":      IntelBias.NEUTRAL,
        "social":      IntelBias.NEUTRAL,
        "feeds":       IntelBias.NEUTRAL,
        "reference":   IntelBias.NEUTRAL,
    }

    # ── Mapeamento país/região → moeda ────────────────────────────────────
    # Chave: string de busca (lowercase). Valor: código ISO 4217.
    COUNTRY_CURRENCY_MAP: dict[str, str] = {
        # Americas
        "united states": "USD", "us": "USD", "usa": "USD",
        "canada": "CAD",
        "mexico": "MXN",
        "brazil": "BRL",
        # Europe
        "eurozone": "EUR", "euro area": "EUR",
        "germany": "EUR", "france": "EUR", "italy": "EUR", "spain": "EUR",
        "netherlands": "EUR", "belgium": "EUR", "portugal": "EUR",
        "united kingdom": "GBP", "uk": "GBP", "britain": "GBP", "england": "GBP",
        "switzerland": "CHF",
        "norway": "NOK",
        "sweden": "SEK",
        "denmark": "DKK",
        "poland": "PLN",
        # Asia-Pacific
        "japan": "JPY",
        "china": "CNY", "prc": "CNY",
        "australia": "AUD",
        "new zealand": "NZD",
        "singapore": "SGD",
        "south korea": "KRW",
        "india": "INR",
        # Middle East / Africa (safe havens triggered by crises here)
        "russia": "RUB",
        "ukraine": "UAH",
        "iran": "IRR",
        "turkey": "TRY",
        "saudi arabia": "SAR",
    }

    # Major forex pairs that can receive tags
    MAJOR_PAIRS: list[str] = [
        "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD",
        "USDCHF", "NZDUSD", "EURGBP", "EURJPY", "GBPJPY",
    ]

    # Safe-haven currencies — benefit when geopolitical risk spikes
    SAFE_HAVEN_CURRENCIES: frozenset[str] = frozenset({"USD", "JPY", "CHF"})

    def __init__(
        self,
        redis_client: aioredis.Redis,
        mq: Any | None = None,  # MQProviderAsync — typed loosely to avoid circular import
        min_severity: str = IntelSeverity.HIGH,
    ) -> None:
        """
        Args:
            redis_client: Redis async client (already connected).
            mq: Optional MQProviderAsync for publishing GlobalTag events.
            min_severity: Minimum severity to trigger tag creation (HIGH by default).
        """
        self._r = redis_client
        self._mq = mq
        self._min_severity = min_severity

    # ── Public API ────────────────────────────────────────────────────────

    async def process_result(self, result: ExtractionResult) -> list[GlobalTag]:
        """Process all high-severity items from an ExtractionResult.

        Returns the list of GlobalTag objects that were created/updated.
        Tags stored in Redis and published to MQ (if mq is set).
        """
        if not result.ok or not result.items:
            return []

        tags: list[GlobalTag] = []
        for item in result.items:
            if self._should_tag(item):
                tag = await self._create_tag(item)
                if tag:
                    tags.append(tag)
        return tags

    # ── Internal helpers ─────────────────────────────────────────────────

    def _should_tag(self, item: IntelItem) -> bool:
        """True if item severity meets the minimum threshold."""
        if self._min_severity == IntelSeverity.HIGH:
            return item.severity == IntelSeverity.HIGH
        if self._min_severity == IntelSeverity.MEDIUM:
            return item.severity in {IntelSeverity.HIGH, IntelSeverity.MEDIUM}
        return True   # LOW — tag everything

    async def _create_tag(self, item: IntelItem) -> GlobalTag | None:
        """Create a GlobalTag from an IntelItem, store in Redis, and publish to MQ."""
        affected = self._resolve_assets(item)
        if not affected:
            self.log.debug(
                "GlobalTagManager: no affected assets for %s (%s/%s) — skipping",
                item.id, item.domain, item.source,
            )
            return None

        asset = affected[0]  # Primary asset — can extend to multi-asset in future

        bias = str(item.extra.get("bias") or self.DOMAIN_BIAS.get(item.domain, IntelBias.NEUTRAL))
        risk_score = float(item.extra.get("risk_score") or self.SEVERITY_SCORE.get(item.severity, 0.5))
        ttl = self.DOMAIN_TTL.get(item.domain, self._DEFAULT_TTL)

        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=ttl)

        tag = GlobalTag(
            asset=asset,
            bias=bias,
            risk_score=risk_score,
            trigger_event_id=item.id,
            established_at=now.isoformat(),
            expires_at=expires_at.isoformat(),
        )

        redis_key = tag.redis_key()   # "alert_global:{asset}"
        await self._r.setex(redis_key, ttl, json.dumps(tag.to_dict()))
        self.log.info(
            "GlobalTag stored: %s | bias=%s risk=%.2f ttl=%dh | trigger=%s",
            tag.asset, tag.bias, tag.risk_score, ttl // 3600, item.id,
        )

        if self._mq is not None:
            payload = tag.to_mq_payload("GLOBAL_TAG_UPDATED")
            await self._mq.publish(IntelTopics.GLOBAL_TAGS, payload)
            self.log.debug("GlobalTag published to MQ: %s", tag.asset)

        return tag

    def _resolve_assets(self, item: IntelItem) -> list[str]:
        """Determine which forex pairs are affected by this item.

        Priority:
            1. ``item.extra["affected_assets"]`` — explicit list
            2. ``item.country`` — country → currency → pairs
            3. ``item.tags`` — text search for country/currency names

        Returns list of forex pair strings (e.g., ["EURUSD"]).
        """
        # 1. Explicit assets from extractor
        explicit = item.extra.get("affected_assets")
        if explicit and isinstance(explicit, list):
            return [str(a) for a in explicit if a]

        # 2. Country field → currency → pairs
        currencies: set[str] = set()
        if item.country:
            currency = self._country_to_currency(item.country)
            if currency:
                currencies.add(currency)

        # 3. Search item.tags for country/currency mentions
        if not currencies:
            lowered_tags = " ".join(item.tags).lower()
            for country_key, currency in self.COUNTRY_CURRENCY_MAP.items():
                if country_key in lowered_tags:
                    currencies.add(currency)

        if not currencies:
            return []

        return self._currencies_to_pairs(currencies, item.domain)

    def _country_to_currency(self, country: str) -> str | None:
        """Map country name/code to ISO 4217 currency code."""
        country_lower = country.lower().strip()
        return self.COUNTRY_CURRENCY_MAP.get(country_lower)

    def _currencies_to_pairs(self, currencies: set[str], domain: str) -> list[str]:
        """Convert currency codes to forex pair strings.

        Rules:
        - If USD is affected: produce pairs like EURUSD inverse
        - If EUR/GBP/AUD/NZD/CAD: produce {CCY}USD
        - If JPY/CHF: produce USD{CCY}
        - Safe-haven currencies get a bullish bias bump in conflict/environment domains
        """
        base_currencies = {"EUR", "GBP", "AUD", "NZD", "CAD"}
        quote_currencies = {"JPY", "CHF"}

        pairs: list[str] = []

        for ccy in currencies:
            if ccy == "USD":
                # USD affected → impact on all majors — skip, too broad
                continue
            elif ccy in base_currencies:
                pairs.append(f"{ccy}USD")
            elif ccy in quote_currencies:
                pairs.append(f"USD{ccy}")
            elif ccy in {"CNY", "KRW", "SGD", "INR"}:
                # EM currencies — use USD pair
                pairs.append(f"USD{ccy}" if ccy in {"CNY"} else f"{ccy}USD")
            # else: minor/exotic — skip

        # If geopolitical/conflict event and no clear currency: safe havens benefit
        if not pairs and domain in {"conflict", "environment", "sanctions", "cyber"}:
            pairs = ["USDJPY", "USDCHF"]  # safe havens strengthen vs risk currencies

        return pairs[:3]  # Limit to top 3 to avoid spam
