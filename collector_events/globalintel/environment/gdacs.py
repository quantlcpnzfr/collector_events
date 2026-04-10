"""GDACS disaster alerting extractor."""

from __future__ import annotations

import logging
from xml.etree import ElementTree

import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load
from ..utils import safe_float

logger = logging.getLogger(__name__)

_CFG = _load("environment.json")
_GDACS_RSS_URL = _CFG["gdacs_rss_url"]

_ALERT_SEVERITY = {"Red": "HIGH", "Orange": "MEDIUM", "Green": "LOW"}


class GDACSExtractor(BaseExtractor):
    """Fetches current GDACS disaster alerts (RSS/XML with GeoRSS)."""

    SOURCE = "gdacs"
    DOMAIN = "environment"
    REDIS_KEY = "environment:disasters:v1"
    TTL_SECONDS = 7200

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        try:
            async with session.get(_GDACS_RSS_URL) as resp:
                resp.raise_for_status()
                text = await resp.text()
        except Exception as exc:
            logger.warning("GDACS fetch failed: %s", exc)
            return []

        try:
            root = ElementTree.fromstring(text)
        except ElementTree.ParseError as exc:
            logger.warning("GDACS XML parse failed: %s", exc)
            return []

        ns = {
            "gdacs": "http://www.gdacs.org",
            "georss": "http://www.georss.org/georss",
        }

        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()

            alert_level = (item.findtext("gdacs:alertlevel", namespaces=ns) or "").strip()
            event_type = (item.findtext("gdacs:eventtype", namespaces=ns) or "").strip()
            country_el = (item.findtext("gdacs:country", namespaces=ns) or "").strip()
            severity_val = (item.findtext("gdacs:severity", namespaces=ns) or "").strip()

            point = (item.findtext("georss:point", namespaces=ns) or "").strip()
            lat, lon = None, None
            if " " in point:
                parts = point.split()
                lat = safe_float(parts[0])
                lon = safe_float(parts[1])

            severity = _ALERT_SEVERITY.get(alert_level, "LOW")

            items.append(IntelItem(
                id=f"gdacs:{event_type}:{title[:60]}",
                source="gdacs",
                domain="environment",
                title=title[:300],
                url=link,
                ts=pub_date,
                lat=lat,
                lon=lon,
                country=country_el,
                severity=severity,
                tags=["disaster", event_type.lower()],
                extra={
                    "alert_level": alert_level,
                    "event_type": event_type,
                    "severity_text": severity_val,
                },
            ))
        return items
