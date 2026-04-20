"""OFAC Sanctions pressure extractor."""

from __future__ import annotations

from forex_shared.logging.get_logger import get_logger
from xml.etree import ElementTree

import aiohttp

from collector_events.globalintel.base import BaseExtractor, IntelItem
from collector_events.globalintel.config import _load

logger = get_logger(__name__)

_CFG = _load("sanctions.json")
_OFAC_SDN_URL = _CFG["ofac_sdn_url"]
_OFAC_CONS_URL = _CFG["ofac_cons_url"]


class OFACSanctionsExtractor(BaseExtractor):
    """OFAC SDN/Consolidated sanctions aggregated by country."""

    SOURCE = "ofac"
    DOMAIN = "sanctions"
    REDIS_KEY = "sanctions:pressure:v1"
    TTL_SECONDS = 54000  # 15h

    def __init__(self, include_consolidated: bool = True):
        super().__init__()
        self._include_cons = include_consolidated

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        country_agg: dict[str, dict] = {}

        await self._parse_list(session, _OFAC_SDN_URL, country_agg)
        if self._include_cons:
            await self._parse_list(session, _OFAC_CONS_URL, country_agg)

        items: list[IntelItem] = []
        for country, info in country_agg.items():
            count = info["count"]
            if count >= 100:
                severity = "HIGH"
            elif count >= 20:
                severity = "MEDIUM"
            else:
                severity = "LOW"

            items.append(IntelItem(
                id=f"ofac:{country}",
                source="ofac",
                domain="sanctions",
                title=f"OFAC: {country} ({count} entities)",
                country=[country] if country else [],
                severity=severity,
                tags=["sanctions", "ofac"],
                extra={
                    "entity_count": count,
                    "programs": sorted(info["programs"]),
                    "sample_names": info["names"][:10],
                },
            ))
        items.sort(key=lambda x: -x.extra.get("entity_count", 0))
        return items

    async def _parse_list(
        self,
        session: aiohttp.ClientSession,
        url: str,
        country_agg: dict[str, dict],
    ) -> None:
        """Download and parse OFAC XML (50MB+, 120s timeout)."""
        try:
            timeout = aiohttp.ClientTimeout(total=120)
            async with session.get(url, timeout=timeout) as resp:
                resp.raise_for_status()
                xml_bytes = await resp.read()
        except Exception as exc:
            logger.warning("OFAC download failed (%s): %s", url.split("/")[-1], exc)
            return

        try:
            root = ElementTree.fromstring(xml_bytes)
        except ElementTree.ParseError as exc:
            logger.warning("OFAC XML parse failed: %s", exc)
            return

        ns = root.tag.split("}")[0] + "}" if "}" in root.tag else ""

        for entry in root.findall(f".//{ns}sdnEntry"):
            sdn_type = (entry.findtext(f"{ns}sdnType") or "").strip()
            first = (entry.findtext(f"{ns}firstName") or "").strip()
            last = (entry.findtext(f"{ns}lastName") or "").strip()
            name = f"{first} {last}".strip() or "Unknown"

            programs: set[str] = set()
            for prog in entry.findall(f".//{ns}program"):
                if prog.text:
                    programs.add(prog.text.strip())

            country = ""
            for addr in entry.findall(f".//{ns}address"):
                c = (addr.findtext(f"{ns}country") or "").strip()
                if c:
                    country = c
                    break

            if not country:
                country = "Unknown"

            if country not in country_agg:
                country_agg[country] = {"count": 0, "programs": set(), "names": []}

            agg = country_agg[country]
            agg["count"] += 1
            agg["programs"].update(programs)
            if len(agg["names"]) < 10:
                agg["names"].append(name)
