from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Mapping


logger = logging.getLogger("collector_events.telemetry")


def track(event: str, properties: Mapping[str, Any] | None = None) -> None:
    payload = {
        "event": event,
        "properties": dict(properties or {}),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))

