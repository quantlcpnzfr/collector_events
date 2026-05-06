from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Optional, List

@dataclass(frozen=True)
class TelegramChannel:
    """Config for a single Telegram channel source."""
    handle: str
    label: str
    topic: str = "other"
    region: str = ""
    tier: Optional[int] = None
    channel_set: str = "full"
    max_messages: int = 25
    enabled: bool = True
    source_role: str = ""
    routing_family: str = ""
    bias_risk: str = ""
    verification_required: bool = False
    authenticity_class: str = ""
    can_trigger_trade: bool = False
    can_influence_sentiment: bool = True
    send_to_translator: bool = True
    send_to_oracle: bool = True
    deployment_priority: str = "p1"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TelegramChannel:
        return cls(
            handle=str(data.get("handle") or ""),
            label=str(data.get("label") or ""),
            topic=str(data.get("topic") or "other"),
            region=str(data.get("region") or ""),
            tier=data.get("tier"),
            channel_set=str(data.get("channel_set") or data.get("channelSet") or "full"),
            max_messages=int(data.get("max_messages") or data.get("maxMessages") or 25),
            enabled=bool(data.get("enabled", True)),
            source_role=str(data.get("source_role") or data.get("sourceRole") or ""),
            routing_family=str(data.get("routing_family") or data.get("routingFamily") or ""),
            bias_risk=str(data.get("bias_risk") or data.get("biasRisk") or ""),
            verification_required=bool(data.get("verification_required") or data.get("verificationRequired", False)),
            authenticity_class=str(data.get("authenticity_class") or data.get("authenticityClass") or ""),
            can_trigger_trade=bool(data.get("can_trigger_trade") or data.get("canTriggerTrade", False)),
            can_influence_sentiment=bool(
                data.get("can_influence_sentiment")
                if "can_influence_sentiment" in data
                else data.get("canInfluenceSentiment", True)
            ),
            send_to_translator=bool(
                data.get("send_to_translator")
                if "send_to_translator" in data
                else data.get("sendToTranslator", True)
            ),
            send_to_oracle=bool(
                data.get("send_to_oracle")
                if "send_to_oracle" in data
                else data.get("sendToOracle", True)
            ),
            deployment_priority=str(
                data.get("deployment_priority")
                or data.get("deploymentPriority")
                or "p1"
            ),
        )

@dataclass(frozen=True)
class TelegramFeedItem:
    """Normalized item extracted from a Telethon message."""
    id: str
    source: str
    channel: str
    channelTitle: str
    url: str
    ts: str
    date: str
    timestamp: str
    text: str
    topic: str
    tags: List[str]
    earlySignal: bool
    views: int
    forwards: int
