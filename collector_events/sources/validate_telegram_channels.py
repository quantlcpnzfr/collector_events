from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

REQUIRED_FIELDS = (
    "id",
    "handle",
    "label",
    "sourceGroup",
    "channelSet",
    "topic",
    "region",
    "tier",
    "enabled",
    "maxMessages",
    "publicUrl",
    "captureMethod",
    "decisionUse",
)

TOPIC_VALUES = {
    "breaking",
    "conflict",
    "crypto",
    "cyber",
    "finance",
    "forex",
    "forex_signals",
    "geopolitics",
    "markets",
    "middleeast",
    "military_alert",
    "military_intel",
    "osint",
}

CHANNEL_SET_VALUES = {"full", "tech", "local"}
SOURCE_GROUP_VALUES = {"telegram"}
CAPTURE_METHOD_VALUES = {"telegram_mtproto_relay"}
BIAS_RISK_VALUES = {"low", "medium", "high", "unknown"}
AUTHENTICITY_CLASS_VALUES = {
    "official_brand",
    "independent_reporter",
    "osint_aggregator",
    "narrative_partisan",
    "market_tape",
    "retail_signal",
    "unofficial_mirror",
    "unknown",
}
SOURCE_ROLE_VALUES = {
    "breaking_news",
    "conflict_osint",
    "regional_news",
    "cyber_intel",
    "finance_tape",
    "forex_retail_signal",
    "crypto_flow",
    "osint_context",
}
ROUTING_FAMILY_VALUES = {
    "osint_breaking",
    "osint_conflict",
    "osint_geopolitics",
    "osint_middleeast",
    "cyber_news",
    "finance_tape",
    "crypto_flow",
    "forex_retail_signal",
}


def _catalog_path() -> Path:
    return Path(__file__).resolve().with_name("telegram_channels.json")


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _dump(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def _default_source_role(channel: dict[str, Any]) -> str:
    topic = str(channel.get("topic") or "").lower()
    if topic in {"finance", "markets"}:
        return "finance_tape"
    if topic in {"forex", "forex_signals"}:
        return "forex_retail_signal"
    if topic == "crypto":
        return "crypto_flow"
    if topic == "cyber":
        return "cyber_intel"
    if topic in {"osint"}:
        return "osint_context"
    if topic in {"middleeast", "geopolitics"}:
        return "regional_news"
    if topic in {"breaking"}:
        return "breaking_news"
    return "conflict_osint"


def _default_routing_family(channel: dict[str, Any]) -> str:
    topic = str(channel.get("topic") or "").lower()
    if topic == "cyber":
        return "cyber_news"
    if topic in {"finance", "markets"}:
        return "finance_tape"
    if topic == "crypto":
        return "crypto_flow"
    if topic in {"forex", "forex_signals"}:
        return "forex_retail_signal"
    if topic == "middleeast":
        return "osint_middleeast"
    if topic == "geopolitics":
        return "osint_geopolitics"
    if topic == "breaking":
        return "osint_breaking"
    return "osint_conflict"


def _default_bias_risk(channel: dict[str, Any]) -> str:
    handle = str(channel.get("handle") or "").lower()
    topic = str(channel.get("topic") or "").lower()
    if handle in {
        "abualiexpress",
        "englishabuali",
        "ddgeopolitics",
        "disclosetv",
        "disclosewt",
        "financialjuice",
        "intel_slava",
        "intelrepublic",
        "rybar",
        "thecradlemedia",
        "vahidonline",
        "zerohedge",
    }:
        return "high"
    if topic in {"forex", "forex_signals"}:
        return "high"
    if topic in {"finance", "markets", "cyber", "crypto"}:
        return "medium"
    return "medium"


def _default_authenticity_class(channel: dict[str, Any]) -> str:
    handle = str(channel.get("handle") or "").lower()
    topic = str(channel.get("topic") or "").lower()
    if handle == "financialjuice":
        return "unofficial_mirror"
    if handle in {"binance_announcements", "thehackernews"}:
        return "official_brand"
    if topic in {"finance", "markets"}:
        return "market_tape"
    if topic in {"forex", "forex_signals"}:
        return "retail_signal"
    if topic == "crypto":
        return "market_tape"
    if topic == "cyber":
        return "official_brand" if handle == "thehackernews" else "osint_aggregator"
    if handle in {"citeam", "vahidonline"}:
        return "independent_reporter"
    if handle in {"osintdefender", "osintlive", "osintupdates", "auroraintel", "clashreport"}:
        return "osint_aggregator"
    return "unknown"


def _default_verification_required(channel: dict[str, Any]) -> bool:
    topic = str(channel.get("topic") or "").lower()
    authenticity = _default_authenticity_class(channel)
    if authenticity in {"unofficial_mirror", "narrative_partisan", "unknown"}:
        return True
    return topic in {"forex", "forex_signals", "crypto", "cyber"}


def _default_can_trigger_trade(channel: dict[str, Any]) -> bool:
    topic = str(channel.get("topic") or "").lower()
    return topic in {"finance", "markets", "crypto"} and str(channel.get("handle") or "").lower() in {
        "binance_announcements",
    }


def _default_can_influence_sentiment(channel: dict[str, Any]) -> bool:
    return True


def _default_send_to_translator(channel: dict[str, Any]) -> bool:
    topic = str(channel.get("topic") or "").lower()
    return topic not in {"finance", "markets", "crypto"}


def _default_send_to_oracle(channel: dict[str, Any]) -> bool:
    topic = str(channel.get("topic") or "").lower()
    return topic not in {"forex", "forex_signals"}


def recalc_counts(channels: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "totalChannelOccurrences": len(channels),
        "enabled": sum(1 for channel in channels if bool(channel.get("enabled", True))),
        "bySet": dict(Counter(str(channel.get("channelSet") or "unknown") for channel in channels)),
        "byTopic": dict(Counter(str(channel.get("topic") or "unknown") for channel in channels)),
        "byTier": dict(Counter(str(channel.get("tier") if channel.get("tier") is not None else "unknown") for channel in channels)),
        "byRegion": dict(Counter(str(channel.get("region") or "unknown") for channel in channels)),
    }


def apply_governance_defaults(payload: dict[str, Any]) -> None:
    payload.setdefault("schemaVersion", 2)
    payload.setdefault("catalogType", "telegram_source_catalog")
    governance = payload.setdefault("governance", {})
    governance.setdefault("defaultsAppliedBy", "validate_telegram_channels.py")
    governance.setdefault(
        "notes",
        "Governance fields are additive in P0 and may be overridden by manual curation in later phases.",
    )

    for channel in payload.get("channels", []):
        channel.setdefault("sourceRole", _default_source_role(channel))
        channel.setdefault("routingFamily", _default_routing_family(channel))
        channel.setdefault("biasRisk", _default_bias_risk(channel))
        channel.setdefault("verificationRequired", _default_verification_required(channel))
        channel.setdefault("authenticityClass", _default_authenticity_class(channel))
        channel.setdefault("canTriggerTrade", _default_can_trigger_trade(channel))
        channel.setdefault("canInfluenceSentiment", _default_can_influence_sentiment(channel))
        channel.setdefault("sendToTranslator", _default_send_to_translator(channel))
        channel.setdefault("sendToOracle", _default_send_to_oracle(channel))


def validate_payload(payload: dict[str, Any], *, require_governance: bool) -> list[str]:
    errors: list[str] = []
    channels = payload.get("channels", [])
    if not isinstance(channels, list):
        return ["channels must be a list"]

    ids = Counter()
    handles = Counter()

    for index, channel in enumerate(channels):
        if not isinstance(channel, dict):
            errors.append(f"channel[{index}] must be an object")
            continue

        for field in REQUIRED_FIELDS:
            if field not in channel:
                errors.append(f"channel[{index}] missing required field: {field}")

        if channel.get("sourceGroup") not in SOURCE_GROUP_VALUES:
            errors.append(f"channel[{index}] invalid sourceGroup: {channel.get('sourceGroup')}")
        if channel.get("channelSet") not in CHANNEL_SET_VALUES:
            errors.append(f"channel[{index}] invalid channelSet: {channel.get('channelSet')}")
        if channel.get("topic") not in TOPIC_VALUES:
            errors.append(f"channel[{index}] invalid topic: {channel.get('topic')}")
        if channel.get("captureMethod") not in CAPTURE_METHOD_VALUES:
            errors.append(f"channel[{index}] invalid captureMethod: {channel.get('captureMethod')}")
        if not str(channel.get("publicUrl") or "").startswith("https://t.me/"):
            errors.append(f"channel[{index}] invalid publicUrl: {channel.get('publicUrl')}")
        if not isinstance(channel.get("decisionUse"), list):
            errors.append(f"channel[{index}] decisionUse must be a list")
        if not isinstance(channel.get("enabled"), bool):
            errors.append(f"channel[{index}] enabled must be bool")
        if not isinstance(channel.get("maxMessages"), int):
            errors.append(f"channel[{index}] maxMessages must be int")

        ids.update([str(channel.get("id") or "")])
        handles.update([str(channel.get("handle") or "").lower()])

        if require_governance:
            governance_fields = {
                "sourceRole": SOURCE_ROLE_VALUES,
                "routingFamily": ROUTING_FAMILY_VALUES,
                "biasRisk": BIAS_RISK_VALUES,
                "authenticityClass": AUTHENTICITY_CLASS_VALUES,
            }
            for field, allowed in governance_fields.items():
                if field not in channel:
                    errors.append(f"channel[{index}] missing governance field: {field}")
                elif channel[field] not in allowed:
                    errors.append(f"channel[{index}] invalid {field}: {channel[field]}")

            for field in (
                "verificationRequired",
                "canTriggerTrade",
                "canInfluenceSentiment",
                "sendToTranslator",
                "sendToOracle",
            ):
                if field not in channel:
                    errors.append(f"channel[{index}] missing governance field: {field}")
                elif not isinstance(channel[field], bool):
                    errors.append(f"channel[{index}] {field} must be bool")

    duplicate_ids = [value for value, count in ids.items() if value and count > 1]
    duplicate_handles = [value for value, count in handles.items() if value and count > 1]
    if duplicate_ids:
        errors.append(f"duplicate ids: {duplicate_ids}")
    if duplicate_handles:
        errors.append(f"duplicate handles: {duplicate_handles}")

    expected_counts = recalc_counts(channels)
    if payload.get("counts") != expected_counts:
        errors.append("counts block is out of sync with channels")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and optionally rewrite telegram_channels.json")
    parser.add_argument("--path", type=Path, default=_catalog_path())
    parser.add_argument("--write", action="store_true", help="Rewrite counts and add P0 governance defaults")
    parser.add_argument(
        "--require-governance",
        action="store_true",
        help="Fail when additive governance fields are missing or invalid",
    )
    args = parser.parse_args()

    payload = _load(args.path)
    if args.write:
        apply_governance_defaults(payload)
        payload["counts"] = recalc_counts(payload.get("channels", []))
        _dump(args.path, payload)

    errors = validate_payload(payload, require_governance=args.require_governance)
    if errors:
        print("Validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("Validation passed.")
    print(json.dumps(payload.get("counts", {}), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
