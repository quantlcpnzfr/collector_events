# globalintel/tagging/llm_enricher.py
"""
LLMEnricher — AI Trend Oracle for refining GlobalTag bias and risk_score.

Uses DeepSeek as primary provider; falls back to Claude when no DeepSeek key.
Returns refined bias, risk_score, and reasoning — or None on failure.

Extracted from GlobalTagManager to isolate LLM API calls and prompt logic.
"""

from __future__ import annotations

import json
from typing import Any

import aiohttp

from forex_shared.config.categories import AIConfig
from forex_shared.domain.intel import IntelBias, IntelItem
from forex_shared.logging.loggable import Loggable


class LLMEnricher(Loggable):
    """Refine tag bias/risk_score via LLM API call."""

    async def enrich(
        self,
        item: IntelItem,
        asset: str,
        current_bias: str,
        current_risk: float,
    ) -> dict[str, Any] | None:
        """Call LLM API to refine bias and risk_score for a GlobalTag.

        Returns dict with ``bias``, ``risk_score``, ``reasoning`` or None.
        """
        ai_cfg = AIConfig()
        api_key = ai_cfg.DEEPSEEK_API_KEY or ""
        provider = "deepseek"
        if not api_key:
            api_key = ai_cfg.CLAUDE_API_KEY or ""
            provider = "claude"
        if not api_key:
            self.log.debug("LLMEnricher: no API key — skipping enrichment")
            return None

        prompt = _build_oracle_prompt(item, asset, current_bias, current_risk)

        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20)
            ) as session:
                if provider == "deepseek":
                    result = await _call_deepseek(session, api_key, prompt, ai_cfg)
                else:
                    result = await _call_claude(session, api_key, prompt, ai_cfg)

            if result:
                self.log.info(
                    "AI Trend Oracle [%s] enriched tag %s: bias=%s->%s risk=%.2f->%.2f",
                    provider, asset, current_bias, result.get("bias", current_bias),
                    current_risk, result.get("risk_score", current_risk),
                )
            return result

        except Exception as exc:
            self.log.warning("LLMEnricher: enrichment failed for %s — %s", asset, exc)
            return None


# ── LLM helper functions ──────────────────────────────────────────────────

_BIAS_VALUES = ", ".join([
    "strong_bullish", "bullish", "neutral", "bearish", "strong_bearish",
])

_ORACLE_SYSTEM = (
    "You are an expert forex market analyst. Given a geopolitical or macroeconomic "
    "event, assess its short-term directional impact on a currency pair. Reply ONLY "
    "with a raw JSON object — no markdown, no explanation text — containing exactly "
    "three keys: \"bias\" (one of: strong_bullish, bullish, neutral, bearish, "
    "strong_bearish), \"risk_score\" (float 0.0-1.0), and \"reasoning\" (one sentence "
    "max 150 chars)."
)


def _build_oracle_prompt(
    item: IntelItem,
    asset: str,
    current_bias: str,
    current_risk: float,
) -> str:
    return (
        f"Event: {item.title}\n"
        f"Domain: {item.domain} | Severity: {item.severity} | Country: {','.join(item.country)}\n"
        f"Asset pair: {asset}\n"
        f"Initial rule-based assessment: bias={current_bias}, risk_score={current_risk:.2f}\n"
        f"Body (first 300 chars): {item.body[:300]}\n\n"
        f"Refine or confirm. Valid bias values: {_BIAS_VALUES}."
    )


async def _call_deepseek(
    session: aiohttp.ClientSession,
    api_key: str,
    prompt: str,
    ai_cfg: AIConfig,
) -> dict[str, Any] | None:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": _ORACLE_SYSTEM},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 200,
        "temperature": 0.2,
    }
    url = ai_cfg.DEEPSEEK_CHAT_URL
    async with session.post(url, headers=headers, json=body) as resp:
        resp.raise_for_status()
        data = await resp.json()

    content = data["choices"][0]["message"]["content"].strip()
    return _parse_oracle_response(content)


async def _call_claude(
    session: aiohttp.ClientSession,
    api_key: str,
    prompt: str,
    ai_cfg: AIConfig,
) -> dict[str, Any] | None:
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    body = {
        "model": "claude-3-5-haiku-20241022",
        "system": _ORACLE_SYSTEM,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 200,
    }
    url = ai_cfg.CLAUDE_MESSAGES_URL
    async with session.post(url, headers=headers, json=body) as resp:
        resp.raise_for_status()
        data = await resp.json()

    content = data["content"][0]["text"].strip()
    return _parse_oracle_response(content)


def _parse_oracle_response(raw: str) -> dict[str, Any] | None:
    """Parse LLM response JSON; return None if malformed or invalid bias."""
    try:
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        result = json.loads(raw.strip())
        bias = result.get("bias", "")
        if bias not in IntelBias.ALL:
            return None
        rs = float(result.get("risk_score", 0.5))
        result["risk_score"] = round(min(max(rs, 0.0), 1.0), 3)
        return result
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None
