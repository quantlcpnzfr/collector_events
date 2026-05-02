# services/collector_events/collector_events/processors/tag_emitter.py
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from forex_shared.domain.intel import GlobalTag, IntelItem
from collector_events.processors.country_resolver import CountryResolver

try:
    # Caminho mais genérico usado pelo orchestrator.
    from forex_shared.providers.mq.mq_provider_async import MQProviderAsync
except Exception:  # pragma: no cover
    # Fallback compatível com sua versão antiga.
    from forex_shared.providers.mq.rabbitmq_provider_async import MQProviderAsync

log = logging.getLogger("GlobalTagEmitter")

_CONFIG_DIR = Path(__file__).parent / "config"
_PREDICTION_FILE = _CONFIG_DIR / "prediction_assets.json"

class GlobalTagEmitter:
    """
    Emissor de Global Tags.

    Papel correto:
    - Recebe um IntelItem já enriquecido pelo EventProcessor.
    - Usa danger_score + NLP/GLiNER + regex + config para inferir assets/pairs afetados.
    - Publica uma GlobalTag por ativo/par detectado.

    Observação arquitetural:
    - Isso é o roteamento rápido/heurístico.
    - O AI Trend Oracle ainda pode gerar directives mais refinadas depois.
    """

    def __init__(
        self,
        mq_provider: MQProviderAsync,
        prediction_file: str | Path = _PREDICTION_FILE,
    ):
        self.mq = mq_provider
        self.prediction_file = Path(prediction_file)

        self.config: dict[str, Any] = {}
        self.threshold: float = 0.65
        self.queue_name: str = "intel.global_tags"
        self.default_expiry_minutes: int = 240
        self.max_tags_per_event: int = 8
        self.max_pairs_per_currency: int = 4

        self.currencies: set[str] = set()
        self.pair_universe: list[str] = []
        self.asset_keywords: dict[str, list[str]] = {}
        self.currency_synonyms: dict[str, list[str]] = {}
        self.country_resolver = CountryResolver()
        self.exclude_keywords: list[str] = []
        self.risk_off_assets: dict[str, str] = {}
        self.asset_bias_rules: dict[str, Any] = {}

        self._load_prediction_config()

    # ─────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────

    async def emit_if_critical(self, item: IntelItem) -> None:
        score = self._safe_float((item.extra or {}).get("danger_score", 0.0))

        if score < self.threshold:
            return

        directives = self._predict_financial_directives(item)

        if not directives:
            directives = [
                {
                    "asset": "GLOBAL",
                    "bias": "risk_off" if score > 0.85 else "neutral",
                    "confidence": 0.25,
                    "reason": "fallback_global_no_asset_detected",
                }
            ]

        directives = directives[: self.max_tags_per_event]

        for directive in directives:
            asset = directive["asset"]
            bias = directive.get("bias") or ("risk_off" if score > 0.85 else "neutral")
            confidence = self._safe_float(directive.get("confidence", 1.0), 1.0)

            risk_score = self._clamp(score * max(confidence, 0.35), 0.0, 1.0)
            expiry_minutes = int(directive.get("expiry_minutes", self.default_expiry_minutes))

            tag = GlobalTag(
                asset=asset,
                bias=bias,
                risk_score=risk_score,
                trigger_event_id=item.id,
                established_at=datetime.now(timezone.utc).isoformat(),
                expires_at=(
                    datetime.now(timezone.utc) + timedelta(minutes=expiry_minutes)
                ).isoformat(),
                active=True,
            )

            try:
                await self.mq.publish(
                    queue_name=self.queue_name,
                    message=tag.to_mq_payload(),
                )
                log.info(
                    "🚀 GLOBAL TAG EMITIDA -> Asset=%s | Bias=%s | Risk=%.2f | "
                    "Confidence=%.2f | Reason=%s | Evento=%s",
                    asset,
                    bias,
                    risk_score,
                    confidence,
                    directive.get("reason", ""),
                    item.id,
                )
            except Exception as e:
                log.error("Erro ao publicar GlobalTag no MQ: %s", e)

    # ─────────────────────────────────────────────────────────────
    # Config
    # ─────────────────────────────────────────────────────────────

    def _load_prediction_config(self) -> None:
        try:
            with open(self.prediction_file, "r", encoding="utf-8") as f:
                loaded = json.load(f)

            self.config = self._normalize_config(loaded)
            self.threshold = float(self.config.get("threshold", self.threshold))
            self.queue_name = str(self.config.get("queue_name", self.queue_name))
            self.default_expiry_minutes = int(
                self.config.get("default_expiry_minutes", self.default_expiry_minutes)
            )
            self.max_tags_per_event = int(
                self.config.get("max_tags_per_event", self.max_tags_per_event)
            )
            self.max_pairs_per_currency = int(
                self.config.get("max_pairs_per_currency", self.max_pairs_per_currency)
            )

            self.currencies = set(self.config.get("currencies", []))
            self.pair_universe = list(self.config.get("pair_universe", []))
            self.asset_keywords = self.config.get("asset_keywords", {})
            self.currency_synonyms = self.config.get("currency_synonyms", {})
            self.exclude_keywords = [
                str(x).lower() for x in self.config.get("excludeKeywords", [])
            ]
            self.risk_off_assets = self.config.get("risk_off_assets", {})
            self.asset_bias_rules = self.config.get("asset_bias_rules", {})

            log.info(
                "Roteador de ativos carregado | assets=%s | pairs=%s | currencies=%s",
                len(self.asset_keywords),
                len(self.pair_universe),
                len(self.currencies),
            )

        except Exception as e:
            log.error("Falha ao carregar %s: %s", self.prediction_file, e)
            self.config = self._normalize_config({})

    def _normalize_config(self, raw: dict[str, Any]) -> dict[str, Any]:
        """
        Aceita tanto o formato novo quanto o formato legado:
        {
          "XAU": [...],
          "USD": [...],
          "excludeKeywords": [...]
        }
        """
        if not raw:
            raw = {}

        # Formato novo.
        if "asset_keywords" in raw or "pair_universe" in raw:
            return raw

        # Formato legado.
        asset_keywords = {
            asset: keywords
            for asset, keywords in raw.items()
            if asset != "excludeKeywords" and isinstance(keywords, list)
        }

        return {
            "threshold": 0.75,
            "queue_name": "intel.global_tags",
            "default_expiry_minutes": 240,
            "max_tags_per_event": 8,
            "max_pairs_per_currency": 4,
            "currencies": ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD"],
            "pair_universe": [
                "EUR/USD",
                "GBP/USD",
                "USD/JPY",
                "USD/CHF",
                "USD/CAD",
                "AUD/USD",
                "NZD/USD",
                "EUR/GBP",
                "EUR/JPY",
                "EUR/NZD",
                "GBP/JPY",
                "AUD/JPY",
                "XAU/USD",
                "XAG/USD",
            ],
            "asset_keywords": asset_keywords,
            "currency_synonyms": {},
            "excludeKeywords": raw.get("excludeKeywords", []),
            "risk_off_assets": {
                "XAU": "bullish",
                "XAU/USD": "bullish",
                "OIL": "bullish",
                "BRENT": "bullish",
                "WTI": "bullish",
                "NQ": "bearish",
                "SPX": "bearish",
            },
            "asset_bias_rules": {},
        }

    # ─────────────────────────────────────────────────────────────
    # Prediction core
    # ─────────────────────────────────────────────────────────────

    def _predict_financial_directives(self, item: IntelItem) -> list[dict[str, Any]]:
        text = self._build_combined_text(item)
        text_lower = text.lower()

        if self._is_excluded(text_lower):
            return []

        directives: list[dict[str, Any]] = []

        explicit_pairs = self._extract_explicit_pairs(text)
        explicit_currencies = self._extract_explicit_currencies(text)
        country_currencies = self._extract_country_currencies(item)
        keyword_assets = self._extract_keyword_assets(text)

        # 1. Pares explícitos: EUR/USD signals bullish, USDJPY drops, XAU/USD spikes.
        for pair, context in explicit_pairs:
            bias = self._extract_bias_from_context(context, default="neutral")
            directives.append(
                self._directive(
                    asset=pair,
                    bias=bias,
                    confidence=0.95,
                    reason="explicit_pair",
                )
            )

        # 2. Assets/commodities/índices por keyword: oil refinery -> OIL/BRENT/WTI, war -> XAU etc.
        for asset, confidence, reason in keyword_assets:
            bias = self._infer_asset_bias(asset, text_lower)
            directives.append(
                self._directive(
                    asset=asset,
                    bias=bias,
                    confidence=confidence,
                    reason=reason,
                )
            )

        # 3. Moedas explícitas ou inferidas por país.
        currencies = self._dedupe_keep_order(explicit_currencies + country_currencies)

        for currency in currencies:
            currency_bias = self._extract_currency_bias(text, currency)
            directives.extend(self._currency_to_pair_directives(currency, currency_bias))

        # 4. Regra macro/risk-off usando categoria e GLiNER.
        directives.extend(self._risk_off_directives(item, text_lower))

        return self._dedupe_directives(directives)

    def _build_combined_text(self, item: IntelItem) -> str:
        extra = item.extra or {}
        nlp_features = extra.get("nlp_features", {}) or {}
        gliner_tactical = extra.get("gliner_tactical", {}) or nlp_features.get("gliner_graph", {}) or {}

        parts = [
            getattr(item, "title", "") or "",
            getattr(item, "body", "") or "",
            str(extra.get("impact_category", "")),
        ]

        for values in gliner_tactical.values():
            if isinstance(values, list):
                parts.append(" ".join(str(v) for v in values))

        for ent in nlp_features.get("entities", []) or []:
            if isinstance(ent, dict):
                parts.append(str(ent.get("text", "")))

        return " ".join(part for part in parts if part).strip()

    def _extract_explicit_pairs(self, text: str) -> list[tuple[str, str]]:
        if not self.currencies:
            return []

        ccys = sorted(self.currencies, key=len, reverse=True)
        ccy_group = "|".join(re.escape(c) for c in ccys)

        # EUR/USD, EUR-USD, EURUSD
        pattern = re.compile(
            rf"\b({ccy_group})\s*[/\-]?\s*({ccy_group})\b",
            flags=re.IGNORECASE,
        )

        pairs: list[tuple[str, str]] = []
        for match in pattern.finditer(text):
            base = match.group(1).upper()
            quote = match.group(2).upper()
            if base == quote:
                continue

            pair = f"{base}/{quote}"
            if pair not in self.pair_universe:
                continue

            start = max(match.start() - 100, 0)
            end = min(match.end() + 100, len(text))
            context = text[start:end]
            pairs.append((pair, context))

        return pairs

    def _extract_explicit_currencies(self, text: str) -> list[str]:
        found: list[str] = []
        upper = text.upper()
        lower = text.lower()

        for currency in self.currencies:
            if re.search(rf"\b{re.escape(currency)}\b", upper):
                found.append(currency)

        for currency, aliases in self.currency_synonyms.items():
            for alias in aliases:
                if str(alias).lower() in lower:
                    found.append(currency.upper())
                    break

        return self._dedupe_keep_order(found)

    def _extract_country_currencies(self, item: IntelItem) -> list[str]:
        """
        Converte países detectados em moedas usando CountryResolver.

        Fonte única de verdade:
        - item.country / source_countries / actor_countries / target_countries / mentioned_countries
        - fallback: resolve o texto bruto com CountryResolver.resolve()
        - CountryResolver.get_currency() usa unified-countries_main.json

        Isso evita duplicar country_to_currency dentro de prediction_assets.json.
        """
        country_codes: list[str] = []

        for attr in (
            "country",
            "source_countries",
            "actor_countries",
            "target_countries",
            "mentioned_countries",
        ):
            values = getattr(item, attr, None)
            if isinstance(values, str):
                country_codes.append(values)
            elif isinstance(values, list):
                country_codes.extend(str(v) for v in values)

        # Fallback importante: se o EventProcessor ainda não preencheu country
        # ou se o item tem entidades textuais que não viraram códigos ISO.
        if not country_codes:
            text = f"{getattr(item, 'title', '')} {getattr(item, 'body', '')}"
            country_codes.extend(self.country_resolver.resolve(text))

        currencies: list[str] = []
        for country_code in self._dedupe_keep_order(country_codes):
            currency = self.country_resolver.get_currency(country_code)
            if not currency:
                continue

            currency = currency.upper().strip()

            # Não emite moedas que não pertencem ao universo operável,
            # a menos que estejam explícitas na config.
            if currency in self.currencies:
                currencies.append(currency)

        return self._dedupe_keep_order(currencies)

    def _extract_keyword_assets(self, text: str) -> list[tuple[str, float, str]]:
        lower = text.lower()
        results: list[tuple[str, float, str]] = []

        for asset, keywords in self.asset_keywords.items():
            for keyword in keywords:
                kw = str(keyword).lower().strip()
                if not kw:
                    continue
                if kw in lower:
                    results.append((asset, 0.80, f"keyword:{kw}"))
                    break

        return results

    def _currency_to_pair_directives(
        self,
        currency: str,
        currency_bias: str,
    ) -> list[dict[str, Any]]:
        pairs = [
            pair
            for pair in self.pair_universe
            if self._pair_contains_currency(pair, currency)
        ]

        pairs = self._prioritize_pairs(currency, pairs)
        pairs = pairs[: self.max_pairs_per_currency]

        directives: list[dict[str, Any]] = []
        for pair in pairs:
            pair_bias = self._pair_bias_from_currency_bias(pair, currency, currency_bias)
            directives.append(
                self._directive(
                    asset=pair,
                    bias=pair_bias,
                    confidence=0.65,
                    reason=f"currency:{currency}",
                )
            )

        # Também emite moeda isolada como estado macro, se downstream suportar sessão por moeda.
        directives.append(
            self._directive(
                asset=currency,
                bias=currency_bias,
                confidence=0.55,
                reason=f"currency_index:{currency}",
            )
        )

        return directives

    def _risk_off_directives(self, item: IntelItem, text_lower: str) -> list[dict[str, Any]]:
        extra = item.extra or {}
        category = str(extra.get("impact_category", "")).lower()
        nlp_features = extra.get("nlp_features", {}) or {}
        gliner_graph = extra.get("gliner_tactical", {}) or nlp_features.get("gliner_graph", {}) or {}

        risk_terms = [
            "war",
            "attack",
            "missile",
            "strike",
            "invasion",
            "nuclear",
            "terrorist",
            "explosion",
            "black swan",
            "market shock",
            "geopolitical",
        ]

        tactical_signal = any(
            gliner_graph.get(label)
            for label in (
                "kinetic or military action",
                "weapon or military vehicle",
                "macroeconomic infrastructure or strategic target",
                "strategic infrastructure",
                "geopolitical event",
            )
        )

        is_risk_off = tactical_signal or any(term in text_lower for term in risk_terms)
        if not is_risk_off and "military" not in category and "war" not in category:
            return []

        directives = []
        for asset, bias in self.risk_off_assets.items():
            directives.append(
                self._directive(
                    asset=asset,
                    bias=bias,
                    confidence=0.70,
                    reason="risk_off_macro",
                )
            )

        return directives

    # ─────────────────────────────────────────────────────────────
    # Bias
    # ─────────────────────────────────────────────────────────────

    def _extract_bias_from_context(self, context: str, default: str = "neutral") -> str:
        lower = context.lower()

        strong = any(
            x in lower
            for x in ("strong", "massive", "sharp", "aggressive", "extreme", "explosive")
        )

        bullish_terms = [
            "bullish",
            "buy",
            "long",
            "rally",
            "rallies",
            "rise",
            "rises",
            "gain",
            "gains",
            "surge",
            "surges",
            "jump",
            "jumps",
            "climb",
            "climbs",
            "soar",
            "soars",
            "up",
            "strengthen",
            "strengthens",
            "stronger",
        ]

        bearish_terms = [
            "bearish",
            "sell",
            "short",
            "drop",
            "drops",
            "fall",
            "falls",
            "plunge",
            "plunges",
            "slide",
            "slides",
            "tumble",
            "tumbles",
            "dip",
            "dips",
            "down",
            "weaken",
            "weakens",
            "weaker",
            "crash",
            "crashes",
        ]

        bullish = any(re.search(rf"\b{re.escape(term)}\b", lower) for term in bullish_terms)
        bearish = any(re.search(rf"\b{re.escape(term)}\b", lower) for term in bearish_terms)

        if bullish and not bearish:
            return "strong_bullish" if strong else "bullish"

        if bearish and not bullish:
            return "strong_bearish" if strong else "bearish"

        return default

    def _extract_currency_bias(self, text: str, currency: str) -> str:
        upper = text.upper()

        # Contexto próximo da moeda explícita.
        pattern = re.compile(rf"\b{re.escape(currency)}\b", flags=re.IGNORECASE)
        contexts: list[str] = []

        for match in pattern.finditer(text):
            start = max(match.start() - 80, 0)
            end = min(match.end() + 120, len(text))
            contexts.append(text[start:end])

        if not contexts:
            return "neutral"

        # Se qualquer contexto tiver direção clara, usa.
        for ctx in contexts:
            bias = self._extract_bias_from_context(ctx, default="neutral")
            if bias != "neutral":
                return bias

        return "neutral"

    def _infer_asset_bias(self, asset: str, text_lower: str) -> str:
        explicit = self._extract_bias_from_context(text_lower, default="neutral")
        if explicit != "neutral":
            return explicit

        # Commodities/porto/refinaria/ataque normalmente implicam alta de petróleo/ouro.
        if asset in {"XAU", "XAU/USD", "GOLD"}:
            return "bullish"

        if asset in {"OIL", "BRENT", "WTI", "CRUDE OIL", "BRENT OIL"}:
            return "bullish"

        if asset in {"NQ", "SPX", "NASDAQ", "US500"} and any(
            x in text_lower for x in ("war", "attack", "nuclear", "terrorist", "recession")
        ):
            return "bearish"

        return "neutral"

    def _pair_bias_from_currency_bias(self, pair: str, currency: str, currency_bias: str) -> str:
        if currency_bias == "neutral":
            return "neutral"

        base, quote = pair.split("/")

        if currency == base:
            return currency_bias

        if currency == quote:
            return self._invert_bias(currency_bias)

        return "neutral"

    def _invert_bias(self, bias: str) -> str:
        mapping = {
            "bullish": "bearish",
            "strong_bullish": "strong_bearish",
            "bearish": "bullish",
            "strong_bearish": "strong_bullish",
        }
        return mapping.get(bias, bias)

    # ─────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────

    def _directive(
        self,
        *,
        asset: str,
        bias: str,
        confidence: float,
        reason: str,
        expiry_minutes: int | None = None,
    ) -> dict[str, Any]:
        result = {
            "asset": asset,
            "bias": bias,
            "confidence": self._clamp(confidence, 0.0, 1.0),
            "reason": reason,
        }
        if expiry_minutes is not None:
            result["expiry_minutes"] = expiry_minutes
        return result

    def _dedupe_directives(self, directives: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        by_asset: dict[str, dict[str, Any]] = {}

        bias_rank = {
            "strong_bullish": 4,
            "strong_bearish": 4,
            "bullish": 3,
            "bearish": 3,
            "risk_off": 2,
            "neutral": 1,
        }

        for directive in directives:
            asset = str(directive.get("asset", "")).strip().upper()
            if not asset:
                continue

            # Preserva slash em pares.
            asset = asset.replace("-", "/") if "/" in asset or "-" in asset else asset
            directive = {**directive, "asset": asset}

            existing = by_asset.get(asset)
            if existing is None:
                by_asset[asset] = directive
                continue

            existing_score = (
                self._safe_float(existing.get("confidence", 0.0))
                + 0.05 * bias_rank.get(str(existing.get("bias", "neutral")), 1)
            )
            new_score = (
                self._safe_float(directive.get("confidence", 0.0))
                + 0.05 * bias_rank.get(str(directive.get("bias", "neutral")), 1)
            )

            if new_score > existing_score:
                by_asset[asset] = directive

        result = list(by_asset.values())
        result.sort(key=lambda d: self._safe_float(d.get("confidence", 0.0)), reverse=True)
        return result

    def _prioritize_pairs(self, currency: str, pairs: Sequence[str]) -> list[str]:
        # Ordem pensada para liquidez e utilidade macro.
        quote_priority = ["USD", "JPY", "CHF", "GBP", "EUR", "CAD", "AUD", "NZD"]
        explicit_priority = {
            "EUR": ["EUR/USD", "EUR/JPY", "EUR/GBP", "EUR/NZD"],
            "GBP": ["GBP/USD", "EUR/GBP", "GBP/JPY"],
            "JPY": ["USD/JPY", "EUR/JPY", "GBP/JPY", "AUD/JPY"],
            "CHF": ["USD/CHF", "EUR/CHF", "GBP/CHF"],
            "CAD": ["USD/CAD", "CAD/JPY", "EUR/CAD"],
            "AUD": ["AUD/USD", "AUD/JPY", "EUR/AUD"],
            "NZD": ["NZD/USD", "EUR/NZD", "AUD/NZD"],
            "USD": ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "USD/CAD", "AUD/USD", "NZD/USD", "XAU/USD"],
        }

        preferred = [p for p in explicit_priority.get(currency, []) if p in pairs]
        remaining = [p for p in pairs if p not in preferred]

        def sort_key(pair: str) -> tuple[int, int, str]:
            base, quote = pair.split("/")
            # Pares com USD primeiro, depois safe havens/liquidez.
            usd_score = 0 if "USD" in (base, quote) else 1
            quote_score = quote_priority.index(quote) if quote in quote_priority else 99
            return (usd_score, quote_score, pair)

        remaining.sort(key=sort_key)
        return preferred + remaining

    def _pair_contains_currency(self, pair: str, currency: str) -> bool:
        if "/" not in pair:
            return False
        base, quote = pair.split("/")
        return currency in {base, quote}

    def _is_excluded(self, text_lower: str) -> bool:
        return any(keyword in text_lower for keyword in self.exclude_keywords)

    def _dedupe_keep_order(self, values: Iterable[str]) -> list[str]:
        seen = set()
        result = []
        for value in values:
            clean = str(value).upper().strip()
            if clean and clean not in seen:
                seen.add(clean)
                result.append(clean)
        return result

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(min(value, high), low)
