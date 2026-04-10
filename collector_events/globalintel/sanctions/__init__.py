"""Sanctions data extractor (OFAC SDN + Consolidated lists).

Data source (mirrors worldmonitor seed-sanctions-pressure.mjs):

OFAC — Office of Foreign Assets Control (US Treasury)
    SDN list: https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML
    Consolidated: https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/CONS_PRIM.XML
    Auth: none
    Data: sanctioned entities (individuals, organizations, vessels)
    Worldmonitor uses SAX streaming parser due to file size (~50MB)

Post-collection structure:
    Country-level aggregation of entity counts by program,
    plus top sanctioned entities per country for geopolitical pressure scoring.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from xml.etree import ElementTree

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)

OFAC_SDN_URL = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML"
OFAC_CONS_URL = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/CONS_PRIM.XML"


class OFACSanctionsExtractor(BaseExtractor):
    """Fetches and aggregates OFAC sanctions data.

    Due to XML file size (50MB+), uses iterparse for memory efficiency.
    Aggregates to country-level pressure scores.

    Post-collection structure per item:
        id="sanctions:{country}", source="ofac", domain="sanctions",
        title="Sanctions pressure: {country}",
        country, severity based on entity count,
        extra={entity_count, programs, sample_entities}
    """

    SOURCE = "ofac"
    DOMAIN = "sanctions"
    REDIS_KEY = "sanctions:pressure:v1"
    TTL_SECONDS = 54000  # 15h

    def __init__(self, include_consolidated: bool = True):
        super().__init__()
        self._include_consolidated = include_consolidated

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        # Country → {count, programs, names}
        country_data: dict[str, dict] = defaultdict(
            lambda: {"count": 0, "programs": set(), "names": []}
        )

        await self._parse_list(session, OFAC_SDN_URL, country_data, "SDN")
        if self._include_consolidated:
            await self._parse_list(session, OFAC_CONS_URL, country_data, "CONS")

        # Build IntelItems per country
        items: list[IntelItem] = []
        for country, info in country_data.items():
            if not country or country == "Unknown":
                continue
            count = info["count"]
            severity = "HIGH" if count >= 100 else "MEDIUM" if count >= 20 else "LOW"
            items.append(IntelItem(
                id=f"sanctions:{country}",
                source="ofac",
                domain="sanctions",
                title=f"Sanctions pressure: {country}",
                country=country,
                severity=severity,
                tags=["sanctions", "ofac"],
                extra={
                    "entity_count": count,
                    "programs": sorted(info["programs"])[:10],
                    "sample_entities": info["names"][:5],
                },
            ))

        # Sort by entity count descending
        items.sort(key=lambda x: x.extra.get("entity_count", 0), reverse=True)
        return items

    async def _parse_list(
        self,
        session: aiohttp.ClientSession,
        url: str,
        country_data: dict,
        list_name: str,
    ) -> None:
        """Stream-parse OFAC XML iteratively to avoid loading 50MB+ into memory."""
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status != 200:
                    logger.warning("OFAC %s returned %d", list_name, resp.status)
                    return
                xml_bytes = await resp.read()
        except Exception as exc:
            logger.warning("OFAC %s download failed: %s", list_name, exc)
            return

        try:
            root = ElementTree.fromstring(xml_bytes)
        except ElementTree.ParseError as exc:
            logger.warning("OFAC %s XML parse failed: %s", list_name, exc)
            return

        # OFAC uses a default namespace
        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag.split("}")[0] + "}"

        for entry in root.iter(f"{ns}sdnEntry"):
            sd_type = (entry.findtext(f"{ns}sdnType") or "").strip()
            first = (entry.findtext(f"{ns}firstName") or "").strip()
            last = (entry.findtext(f"{ns}lastName") or "").strip()
            name = f"{first} {last}".strip() or "Unknown"

            # Programs
            programs: list[str] = []
            for prog_el in entry.iter(f"{ns}program"):
                if prog_el.text:
                    programs.append(prog_el.text.strip())

            # Address → country
            country = "Unknown"
            for addr in entry.iter(f"{ns}address"):
                c = (addr.findtext(f"{ns}country") or "").strip()
                if c:
                    country = c
                    break

            info = country_data[country]
            info["count"] += 1
            info["programs"].update(programs)
            if len(info["names"]) < 10:
                info["names"].append(name)
