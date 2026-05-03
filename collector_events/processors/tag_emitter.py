# services/collector_events/collector_events/processors/tag_emitter.py
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence

from forex_shared.domain.intel import GlobalTag, IntelItem

try:
    from forex_shared.providers.mq.mq_provider_async import MQProviderAsync
except Exception:  # pragma: no cover
    from forex_shared.providers.mq.rabbitmq_provider_async import MQProviderAsync

from collector_events.processors.country_resolver import CountryResolver

log = logging.getLogger("GlobalTagEmitter")

_CONFIG_DIR = Path(__file__).parent / "config"
_PREDICTION_FILE = _CONFIG_DIR / "prediction_assets.json"


class GlobalTagEmitter:
    """Emite GlobalTags operacionais e manda eventos ambíguos para intel.oracle.review."""

    def __init__(self, mq_provider: MQProviderAsync | Any, prediction_file: str | Path = _PREDICTION_FILE, *, dry_run: bool = False):
        self.mq = mq_provider
        self.prediction_file = Path(prediction_file)
        self.dry_run = dry_run
        self.country_resolver = CountryResolver()

        self.config: dict[str, Any] = {}
        self.trade_emit_threshold: float = 0.85
        self.oracle_review_threshold: float = 0.60
        self.min_tag_risk_score: float = 0.50
        self.emit_global_fallback: bool = False
        self.queue_name: str = "intel.global_tags"
        self.oracle_review_queue_name: str = "intel.oracle.review"
        self.default_expiry_minutes: int = 240
        self.max_tags_per_event: int = 8
        self.max_pairs_per_currency: int = 4

        self.currencies: set[str] = set()
        self.pair_universe: list[str] = []
        self.asset_keywords: dict[str, list[str]] = {}
        self.currency_synonyms: dict[str, list[str]] = {}
        self.exclude_keywords: list[str] = []
        self.risk_off_asset_groups: dict[str, dict[str, str]] = {}
        self.asset_bias_rules: dict[str, Any] = {}

        self._load_prediction_config()

    async def emit_if_critical(self, item: IntelItem) -> dict[str, Any]:
        if item.extra is None:
            item.extra = {}

        score = self._safe_float(item.extra.get("danger_score", 0.0))
        risk_bucket = str(item.extra.get("risk_bucket", ""))
        report: dict[str, Any] = {
            "checked": True,
            "dry_run": self.dry_run,
            "threshold": self.trade_emit_threshold,
            "oracle_review_threshold": self.oracle_review_threshold,
            "danger_score": score,
            "risk_bucket": risk_bucket,
            "emitted": False,
            "emitted_count": 0,
            "oracle_review_requested": False,
            "oracle_review_queue_name": self.oracle_review_queue_name,
            "queue_name": self.queue_name,
            "tags": [],
            "reason": "",
            "trade_emit_score": 0.0,
            "multidimensional_scores": {},
        }

        # Sprint 3: Multidimensional Scores
        scores = item.extra.get("scores", {})
        if scores:
            m_impact = scores.get("market_impact_score", 0.0)
            a_route = scores.get("asset_route_confidence_score", 0.5)
            d_conf = scores.get("directional_confidence_score", 0.0)
            noise = scores.get("noise_score", 0.0)
            
            # 1. Refinamos Asset Route Confidence com base nas diretrizes preditas (Emitter-side)
            directives = self._predict_financial_directives(item)
            a_route = self._refine_asset_route_confidence(a_route, directives, item)
            scores["asset_route_confidence_score"] = a_route
            
            # 2. A GRANDE FÓRMULA (Sprint 3)
            trade_emit_score = m_impact * a_route * d_conf * (1.0 - noise)
            report["trade_emit_score"] = self._clamp(trade_emit_score, 0.0, 0.99)
            report["multidimensional_scores"] = scores
            
            oracle_review_score = scores.get("oracle_review_score", 0.0)
            geo_severity = scores.get("geopolitical_severity_score", 0.0)
        else:
            # Fallback legacy
            trade_emit_score = score
            oracle_review_score = score
            geo_severity = 0.0
            directives = self._predict_financial_directives(item)
            report["trade_emit_score"] = score
            log.warning("Multidimensional scores not found for event %s. Using legacy danger_score.", item.id)

        if self._is_excluded(self._build_combined_text(item).lower()):
            report["reason"] = "excluded_keyword"
            item.extra["global_tag_emission"] = report
            return report

        # 3. Decision Tree (Contrato Sprint 3)
        if trade_emit_score >= self.trade_emit_threshold:
            # Prossiga para emissão abaixo
            pass
        elif oracle_review_score >= self.oracle_review_threshold:
            report["reason"] = "below_trade_above_oracle_threshold"
            await self._request_oracle_review(item, report, reason="elevated_oracle_review_score", directives=directives)
            item.extra["global_tag_emission"] = report
            log.info("🧠 Oracle Review solicitado | trade_emit=%.3f | oracle_score=%.3f | event=%s", trade_emit_score, oracle_review_score, item.id)
            return report
        elif geo_severity >= 0.85:
            # Fallback para eventos geopolíticos críticos sem ativo claro
            report["reason"] = "critical_geopolitical_severity_no_trade"
            await self._request_oracle_review(item, report, reason="critical_geopolitical_severity", directives=directives)
            item.extra["global_tag_emission"] = report
            return report
        else:
            report["reason"] = "below_global_tag_threshold"
            item.extra["global_tag_emission"] = report
            log.info("🏷️ GlobalTag NÃO emitida | trade_emit_score=%.3f < threshold=%.3f | danger_score=%.3f | event=%s", trade_emit_score, self.trade_emit_threshold, score, item.id)
            return report

        if not directives:
            if self.emit_global_fallback:
                directives = [self._directive(asset="GLOBAL", bias="risk_off" if score >= 0.85 else "neutral", confidence=0.25, reason="fallback_global_no_asset_detected")]
            else:
                report["reason"] = "no_asset_directives_oracle_review"
                await self._request_oracle_review(item, report, reason="critical_score_without_operational_asset", directives=[])
                item.extra["global_tag_emission"] = report
                return report

        directives = directives[: self.max_tags_per_event]
        skipped_low_risk = []

        for directive in directives:
            asset = str(directive["asset"])
            bias = str(directive.get("bias") or "neutral")
            confidence = self._safe_float(directive.get("confidence", 1.0), 1.0)
            # trade_emit_score já é modulado por route_confidence e noise
            risk_score = self._clamp(trade_emit_score * max(confidence, 0.35), 0.0, 0.99)
            if risk_score < self.min_tag_risk_score:
                skipped_low_risk.append({"asset": asset, "risk_score": risk_score, "reason": directive.get("reason", "")})
                continue

            expiry_minutes = int(directive.get("expiry_minutes", self.default_expiry_minutes))
            now = datetime.now(timezone.utc)
            tag = GlobalTag(
                asset=asset,
                bias=bias,
                risk_score=risk_score,
                trigger_event_id=item.id,
                established_at=now.isoformat(),
                expires_at=(now + timedelta(minutes=expiry_minutes)).isoformat(),
                active=True,
            )
            payload = tag.to_mq_payload()
            tag_report = {
                "asset": asset,
                "bias": bias,
                "risk_score": risk_score,
                "confidence": confidence,
                "reason": directive.get("reason", ""),
                "expiry_minutes": expiry_minutes,
                "payload": payload,
                "published": False,
            }
            try:
                if self.dry_run:
                    tag_report["dry_run"] = True
                    log.info("🧪 GLOBAL TAG DRY-RUN -> Asset=%s | Bias=%s | Risk=%.2f | Confidence=%.2f | Reason=%s | Evento=%s", asset, bias, risk_score, confidence, directive.get("reason", ""), item.id)
                else:
                    await self.mq.publish(queue_name=self.queue_name, message=payload)
                    tag_report["published"] = True
                    log.info("🚀 GLOBAL TAG EMITIDA -> Asset=%s | Bias=%s | Risk=%.2f | Confidence=%.2f | Reason=%s | Evento=%s", asset, bias, risk_score, confidence, directive.get("reason", ""), item.id)
                report["tags"].append(tag_report)
            except Exception as e:
                tag_report["error"] = str(e)
                report["tags"].append(tag_report)
                log.error("Erro ao publicar GlobalTag no MQ: %s", e)

        report["skipped_low_risk_tags"] = skipped_low_risk
        report["emitted_count"] = len(report["tags"])
        report["emitted"] = report["emitted_count"] > 0

        if report["emitted"]:
            report["reason"] = "emitted"
        else:
            report["reason"] = "directives_below_min_tag_risk"
            await self._request_oracle_review(item, report, reason="directives_below_min_tag_risk", directives=directives)

        item.extra["global_tag_emission"] = report
        if report["emitted"]:
            log.info("🏷️ GlobalTag report | emitted=%s | count=%s | assets=%s | event=%s", report["emitted"], report["emitted_count"], [tag["asset"] for tag in report["tags"]], getattr(item, "id", "<sem-id>"))
        return report

    def _load_prediction_config(self) -> None:
        try:
            with open(self.prediction_file, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            self.config = self._normalize_config(loaded)
            self.trade_emit_threshold = float(self.config.get("trade_emit_threshold", self.config.get("danger_score_threshold", self.trade_emit_threshold)))
            self.oracle_review_threshold = float(self.config.get("oracle_review_threshold", self.oracle_review_threshold))
            self.min_tag_risk_score = float(self.config.get("min_tag_risk_score", self.min_tag_risk_score))
            self.emit_global_fallback = bool(self.config.get("emit_global_fallback", self.emit_global_fallback))
            self.queue_name = str(self.config.get("queue_name", self.queue_name))
            self.oracle_review_queue_name = str(self.config.get("oracle_review_queue_name", self.oracle_review_queue_name))
            self.default_expiry_minutes = int(self.config.get("default_expiry_minutes", self.default_expiry_minutes))
            self.max_tags_per_event = int(self.config.get("max_tags_per_event", self.max_tags_per_event))
            self.max_pairs_per_currency = int(self.config.get("max_pairs_per_currency", self.max_pairs_per_currency))
            self.currencies = set(self.config.get("currencies", []))
            self.pair_universe = list(self.config.get("pair_universe", []))
            self.asset_keywords = self.config.get("asset_keywords", {})
            self.currency_synonyms = self.config.get("currency_synonyms", {})
            self.exclude_keywords = [str(x).lower() for x in self.config.get("excludeKeywords", [])]
            self.risk_off_asset_groups = self.config.get("risk_off_asset_groups", {})
            if not self.risk_off_asset_groups and self.config.get("risk_off_assets"):
                self.risk_off_asset_groups = {"safe_haven": self.config.get("risk_off_assets", {})}
            self.asset_bias_rules = self.config.get("asset_bias_rules", {})
            log.info("Roteador de ativos carregado | assets=%s | pairs=%s | currencies=%s | dry_run=%s | trade_emit_threshold=%.2f | oracle_threshold=%.2f", len(self.asset_keywords), len(self.pair_universe), len(self.currencies), self.dry_run, self.trade_emit_threshold, self.oracle_review_threshold)
        except Exception as e:
            log.error("Falha ao carregar %s: %s", self.prediction_file, e)
            self.config = self._normalize_config({})

    def _normalize_config(self, raw: dict[str, Any]) -> dict[str, Any]:
        if not raw:
            raw = {}
        if "asset_keywords" in raw or "pair_universe" in raw:
            return raw
        asset_keywords = {asset: keywords for asset, keywords in raw.items() if asset != "excludeKeywords" and isinstance(keywords, list)}
        return {
            "danger_score_threshold": 0.85,
            "oracle_review_threshold": 0.70,
            "min_tag_risk_score": 0.50,
            "emit_global_fallback": False,
            "queue_name": "intel.global_tags",
            "oracle_review_queue_name": "intel.oracle.review",
            "default_expiry_minutes": 240,
            "max_tags_per_event": 8,
            "max_pairs_per_currency": 4,
            "currencies": ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD", "XAU", "XAG"],
            "pair_universe": ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "USD/CAD", "AUD/USD", "NZD/USD", "EUR/GBP", "EUR/JPY", "EUR/NZD", "GBP/JPY", "AUD/JPY", "XAU/USD", "XAG/USD"],
            "asset_keywords": asset_keywords,
            "currency_synonyms": {},
            "excludeKeywords": raw.get("excludeKeywords", []),
            "risk_off_asset_groups": {
                "safe_haven": {"XAU": "bullish", "XAU/USD": "bullish", "JPY": "bullish", "CHF": "bullish"},
                "oil_shock": {"OIL": "bullish", "BRENT": "bullish", "WTI": "bullish"},
                "equity_risk": {"SPX": "bearish", "NQ": "bearish"},
            },
            "asset_bias_rules": {},
        }

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

        for pair, context in explicit_pairs:
            directives.append(self._directive(asset=pair, bias=self._extract_bias_from_context(context, default="neutral"), confidence=0.95, reason="explicit_pair"))
        for asset, confidence, reason in keyword_assets:
            directives.append(self._directive(asset=asset, bias=self._infer_asset_bias(asset, text_lower), confidence=confidence, reason=reason))

        currencies = self._dedupe_keep_order(explicit_currencies + country_currencies)
        for currency in currencies:
            directives.extend(self._currency_to_pair_directives(currency, self._extract_currency_bias(text, currency)))

        directives.extend(self._risk_off_directives(item, text_lower))
        return self._dedupe_directives(directives)

    async def _request_oracle_review(self, item: IntelItem, report: dict[str, Any], *, reason: str, directives: Sequence[dict[str, Any]]) -> None:
        extra = item.extra or {}
        payload = {
            "event_type": "ORACLE_REVIEW_REQUESTED",
            "trigger_event_id": getattr(item, "id", None),
            "reason": reason,
            "title": getattr(item, "title", ""),
            "body": getattr(item, "body", ""),
            "published_at": getattr(item, "published_at", ""),
            "source": getattr(item, "source", ""),
            "domain": getattr(item, "domain", ""),
            "danger_score_legacy": self._safe_float(extra.get("danger_score", 0.0)),
            "risk_bucket": extra.get("risk_bucket"),
            "impact_category": extra.get("impact_category"),
            "scores": extra.get("scores", {}),
            "trade_emit_score": report.get("trade_emit_score", 0.0),
            "candidate_directives": list(directives or []),
            "score_breakdown": extra.get("score_breakdown"),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        report["oracle_review_requested"] = True
        report["oracle_review_reason"] = reason
        report["oracle_review_payload"] = payload
        try:
            if self.dry_run:
                report["oracle_review_published"] = False
                report["oracle_review_dry_run"] = True
                log.info("🧪 ORACLE REVIEW DRY-RUN -> reason=%s | event=%s", reason, getattr(item, "id", "<sem-id>"))
            else:
                await self.mq.publish(queue_name=self.oracle_review_queue_name, message=payload)
                report["oracle_review_published"] = True
                log.info("🧠 ORACLE REVIEW ENVIADO -> reason=%s | event=%s", reason, getattr(item, "id", "<sem-id>"))
        except Exception as e:
            report["oracle_review_error"] = str(e)
            log.error("Erro ao publicar Oracle Review no MQ: %s", e)

    def _build_combined_text(self, item: IntelItem) -> str:
        extra = item.extra or {}
        nlp_features = extra.get("nlp_features", {}) or {}
        gliner_tactical = extra.get("gliner_tactical", {}) or nlp_features.get("gliner_graph", {}) or {}
        parts = [getattr(item, "title", "") or "", getattr(item, "body", "") or "", str(extra.get("impact_category", "")), str(extra.get("risk_bucket", ""))]
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
        pattern = re.compile(rf"\b({ccy_group})\s*[/\-]?\s*({ccy_group})\b", flags=re.IGNORECASE)
        pairs = []
        for match in pattern.finditer(text):
            base, quote = match.group(1).upper(), match.group(2).upper()
            if base == quote:
                continue
            pair = f"{base}/{quote}"
            if pair not in self.pair_universe:
                continue
            start, end = max(match.start() - 100, 0), min(match.end() + 100, len(text))
            pairs.append((pair, text[start:end]))
        return pairs

    def _extract_explicit_currencies(self, text: str) -> list[str]:
        found = []
        upper, lower = text.upper(), text.lower()
        for currency in self.currencies:
            if re.search(rf"\b{re.escape(currency)}\b", upper):
                found.append(currency)
        for currency, aliases in self.currency_synonyms.items():
            for alias in aliases:
                if self._keyword_matches(lower, str(alias).lower()):
                    found.append(currency.upper())
                    break
        return self._dedupe_keep_order(found)

    def _extract_country_currencies(self, item: IntelItem) -> list[str]:
        countries: list[str] = []
        for attr in ("country", "source_countries", "actor_countries", "target_countries", "mentioned_countries"):
            values = getattr(item, attr, None)
            if isinstance(values, str):
                countries.append(values)
            elif isinstance(values, list):
                countries.extend(str(v) for v in values)
        if not countries:
            countries = self.country_resolver.resolve(self._build_combined_text(item))
        currencies = []
        for country_code in countries:
            currency = self.country_resolver.get_currency(str(country_code))
            if currency and currency in self.currencies:
                currencies.append(currency)
        return self._dedupe_keep_order(currencies)

    def _extract_keyword_assets(self, text: str) -> list[tuple[str, float, str]]:
        lower = text.lower()
        results = []
        for asset, keywords in self.asset_keywords.items():
            for keyword in keywords:
                kw = str(keyword).lower().strip()
                if kw and self._keyword_matches(lower, kw):
                    results.append((asset, 0.82, f"keyword:{kw}"))
                    break
        return results

    def _currency_to_pair_directives(self, currency: str, currency_bias: str) -> list[dict[str, Any]]:
        if currency not in self.currencies:
            return []
        pairs = [pair for pair in self.pair_universe if self._pair_contains_currency(pair, currency)]
        pairs = self._prioritize_pairs(currency, pairs)[: self.max_pairs_per_currency]
        directives = []
        for pair in pairs:
            directives.append(self._directive(asset=pair, bias=self._pair_bias_from_currency_bias(pair, currency, currency_bias), confidence=0.64, reason=f"currency:{currency}"))
        directives.append(self._directive(asset=currency, bias=currency_bias, confidence=0.52, reason=f"currency_index:{currency}"))
        return directives

    def _risk_off_directives(self, item: IntelItem, text_lower: str) -> list[dict[str, Any]]:
        extra = item.extra or {}
        breakdown = extra.get("score_breakdown", {}) or {}
        context = breakdown.get("context_filters", {}) or {}
        if context.get("local_incident") or context.get("historical_reference"):
            return []

        directives = []
        if self._is_safe_haven_risk_off(text_lower, extra, context):
            for asset, bias in self.risk_off_asset_groups.get("safe_haven", {}).items():
                directives.append(self._directive(asset=asset, bias=bias, confidence=0.70, reason="risk_off_safe_haven"))
        if self._is_oil_shock(text_lower):
            for asset, bias in self.risk_off_asset_groups.get("oil_shock", {}).items():
                directives.append(self._directive(asset=asset, bias=bias, confidence=0.78, reason="oil_shock"))
        if self._is_equity_risk(text_lower, extra):
            for asset, bias in self.risk_off_asset_groups.get("equity_risk", {}).items():
                directives.append(self._directive(asset=asset, bias=bias, confidence=0.66, reason="equity_risk"))
        return directives

    def _is_safe_haven_risk_off(self, text_lower: str, extra: dict, context: dict) -> bool:
        terms = self.asset_bias_rules.get("safe_haven_keywords", ["war", "attack", "missile", "invasion", "nuclear", "terrorist", "black swan", "market shock", "blockade"])
        category = str(extra.get("impact_category", "")).lower()
        return any(self._keyword_matches(text_lower, str(t).lower()) for t in terms) or "military" in category or "nuclear" in category

    def _is_oil_shock(self, text_lower: str) -> bool:
        terms = self.asset_bias_rules.get("oil_shock_keywords", ["oil", "oil prices", "refinery", "refineries", "pipeline", "hormuz", "opec", "tanker", "suez", "red sea", "chokepoint", "oil terminal", "oil export"])
        return any(self._keyword_matches(text_lower, str(t).lower()) for t in terms)

    def _is_equity_risk(self, text_lower: str, extra: dict) -> bool:
        terms = self.asset_bias_rules.get("equity_risk_keywords", ["global crisis", "black swan", "market shock", "nuclear", "invasion", "recession", "stock market crash", "direct confrontation"])
        major_actor = any(self._keyword_matches(text_lower, actor) for actor in ["united states", "u.s.", "us", "russia", "china", "iran", "israel", "nato"])
        return any(self._keyword_matches(text_lower, str(t).lower()) for t in terms) or (major_actor and any(self._keyword_matches(text_lower, t) for t in ["attack", "strike", "missile", "blockade"]))

    def _extract_bias_from_context(self, context: str, default: str = "neutral") -> str:
        lower = context.lower()
        strong = any(x in lower for x in ("strong", "massive", "sharp", "aggressive", "extreme", "explosive", "dramatically"))
        bullish_terms = ["bullish", "buy", "long", "rally", "rallies", "rise", "rises", "risen", "rose", "gain", "gains", "surge", "surges", "jump", "jumps", "climb", "climbs", "soar", "soars", "up", "strengthen", "strengthens", "stronger"]
        bearish_terms = ["bearish", "sell", "short", "drop", "drops", "fall", "falls", "fell", "plunge", "plunges", "slide", "slides", "tumble", "tumbles", "dip", "dips", "down", "weaken", "weakens", "weaker", "crash", "crashes", "declined", "declines"]
        bullish = any(re.search(rf"\b{re.escape(term)}\b", lower) for term in bullish_terms)
        bearish = any(re.search(rf"\b{re.escape(term)}\b", lower) for term in bearish_terms)
        if bullish and not bearish:
            return "strong_bullish" if strong else "bullish"
        if bearish and not bullish:
            return "strong_bearish" if strong else "bearish"
        return default

    def _extract_currency_bias(self, text: str, currency: str) -> str:
        contexts = []
        for match in re.finditer(rf"\b{re.escape(currency)}\b", text, flags=re.IGNORECASE):
            contexts.append(text[max(match.start() - 80, 0): min(match.end() + 120, len(text))])
        # synonyms context
        for alias in self.currency_synonyms.get(currency, []):
            for match in re.finditer(rf"\b{re.escape(str(alias))}\b", text, flags=re.IGNORECASE):
                contexts.append(text[max(match.start() - 80, 0): min(match.end() + 120, len(text))])
        for ctx in contexts:
            bias = self._extract_bias_from_context(ctx, default="neutral")
            if bias != "neutral":
                return bias
        return "neutral"

    def _infer_asset_bias(self, asset: str, text_lower: str) -> str:
        # P7: Sanctions on oil-exporting countries (Iran, Russia) -> strong_bullish (supply restriction)
        is_oil = asset in {"OIL", "BRENT", "WTI", "CRUDE OIL", "BRENT OIL"}
        if is_oil:
            has_sanctions = any(x in text_lower for x in ["sanction", "embargo", "maximum pressure", "economic pressure"])
            has_exporter = any(self._keyword_matches(text_lower, country) for country in ["iran", "russia", "russians", "iranian", "kremlin", "tehran"])
            if has_sanctions and has_exporter:
                return "strong_bullish"

        # P15: BTC Sanctions Evasion context
        if asset == "BTC" and any(x in text_lower for x in ["sanction", "evasion", "pressure"]):
             # If it's just about using crypto for evasion, don't be bearish
             return "neutral"

        explicit = self._extract_bias_from_context(text_lower, default="neutral")
        if explicit != "neutral":
            return explicit
            
        if asset in {"XAU", "XAU/USD", "GOLD"}:
            return "bullish"
        if is_oil:
            return "bullish"
            
        if asset in {"NQ", "SPX", "NASDAQ", "US500"} and any(self._keyword_matches(text_lower, x) for x in ("war", "attack", "nuclear", "terrorist", "recession", "invasion")):
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
        return {"bullish": "bearish", "strong_bullish": "strong_bearish", "bearish": "bullish", "strong_bearish": "strong_bullish"}.get(bias, bias)

    def _directive(self, *, asset: str, bias: str, confidence: float, reason: str, expiry_minutes: int | None = None) -> dict[str, Any]:
        result = {"asset": asset, "bias": bias, "confidence": self._clamp(confidence, 0.0, 1.0), "reason": reason}
        if expiry_minutes is not None:
            result["expiry_minutes"] = expiry_minutes
        return result

    def _dedupe_directives(self, directives: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        by_asset: dict[str, dict[str, Any]] = {}
        bias_rank = {"strong_bullish": 4, "strong_bearish": 4, "bullish": 3, "bearish": 3, "risk_off": 2, "neutral": 1}
        for directive in directives:
            asset = str(directive.get("asset", "")).strip().upper()
            if not asset:
                continue
            asset = asset.replace("-", "/") if "/" in asset or "-" in asset else asset
            directive = {**directive, "asset": asset}
            existing = by_asset.get(asset)
            if existing is None:
                by_asset[asset] = directive
                continue
            existing_score = self._safe_float(existing.get("confidence", 0.0)) + 0.05 * bias_rank.get(str(existing.get("bias", "neutral")), 1)
            new_score = self._safe_float(directive.get("confidence", 0.0)) + 0.05 * bias_rank.get(str(directive.get("bias", "neutral")), 1)
            if new_score > existing_score:
                by_asset[asset] = directive
        result = list(by_asset.values())
        result.sort(key=lambda d: self._safe_float(d.get("confidence", 0.0)), reverse=True)
        return result

    def _prioritize_pairs(self, currency: str, pairs: Sequence[str]) -> list[str]:
        explicit_priority = {
            "EUR": ["EUR/USD", "EUR/JPY", "EUR/GBP", "EUR/NZD"],
            "GBP": ["GBP/USD", "EUR/GBP", "GBP/JPY"],
            "JPY": ["USD/JPY", "EUR/JPY", "GBP/JPY", "AUD/JPY"],
            "CHF": ["USD/CHF", "EUR/CHF", "GBP/CHF"],
            "CAD": ["USD/CAD", "CAD/JPY", "EUR/CAD"],
            "AUD": ["AUD/USD", "AUD/JPY", "EUR/AUD"],
            "NZD": ["NZD/USD", "EUR/NZD", "AUD/NZD"],
            "USD": ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "USD/CAD", "AUD/USD", "NZD/USD", "XAU/USD"],
            "XAU": ["XAU/USD"], "XAG": ["XAG/USD"],
        }
        preferred = [p for p in explicit_priority.get(currency, []) if p in pairs]
        remaining = [p for p in pairs if p not in preferred]
        remaining.sort()
        return preferred + remaining

    def _pair_contains_currency(self, pair: str, currency: str) -> bool:
        if "/" not in pair:
            return False
        base, quote = pair.split("/")
        return currency in {base, quote}

    def _is_excluded(self, text_lower: str) -> bool:
        return any(self._keyword_matches(text_lower, keyword) for keyword in self.exclude_keywords)

    def _keyword_matches(self, text_lower: str, keyword_lower: str) -> bool:
        kw = str(keyword_lower).lower().strip()
        if not kw:
            return False
        if len(kw) <= 3 or kw.isalpha():
            return re.search(rf"\b{re.escape(kw)}\b", text_lower, flags=re.IGNORECASE) is not None
        return kw in text_lower

    def _dedupe_keep_order(self, values: Iterable[str]) -> list[str]:
        seen, result = set(), []
        for value in values:
            clean = str(value).upper().strip()
            if clean and clean not in seen:
                seen.add(clean)
                result.append(clean)
        return result

    def _refine_asset_route_confidence(self, base_conf: float, directives: list[dict[str, Any]], item: IntelItem) -> float:
        """Refina a confiança do roteamento com base nas diretrizes reais encontradas."""
        if not directives:
            return 0.0
        
        best_conf = 0.0
        for d in directives:
            conf = float(d.get("confidence", 0.0))
            reason = str(d.get("reason", ""))
            
            # Peso qualitativo baseado na origem da diretriz
            if "explicit_pair" in reason:
                conf *= 1.0
            elif "keyword" in reason:
                conf *= 0.9
            elif "currency" in reason:
                conf *= 0.75
            elif "risk_off" in reason:
                conf *= 0.65
                
            if conf > best_conf:
                best_conf = conf
                
        # Combina a estimativa do processador com a evidência do emissor
        refined = max(base_conf, best_conf)
        
        # P15: Especial para Oil Shock / Hormuz
        text_lower = self._build_combined_text(item).lower()
        if "hormuz" in text_lower or ("oil" in text_lower and "blockade" in text_lower):
            refined = max(refined, 0.85)
            
        return self._clamp(refined, 0.0, 0.99)

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(min(value, high), low)
