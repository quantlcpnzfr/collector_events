"""Cyber threat intelligence extractors.

Data sources (mirrors worldmonitor seed-cyber-threats.mjs):

Feodo Tracker — abuse.ch botnet C2 blocklist
    URL: https://feodotracker.abuse.ch/downloads/ipblocklist.json
    Auth: none
    Data: IP addresses identified as botnet command & control servers

URLhaus — abuse.ch malicious URL database
    URL: https://urlhaus-api.abuse.ch/v1/urls/recent/limit/100/
    Auth: none
    Data: recently submitted malicious URLs

C2IntelFeeds — community C2 IP feed
    URL: https://raw.githubusercontent.com/drb-ra/C2IntelFeeds/master/feeds/IPC2s-30day.csv
    Auth: none
    Data: IP addresses identified as C2 servers in last 30 days

AlienVault OTX — Open Threat Exchange
    URL: https://otx.alienvault.com/api/v1/indicators/export
    Auth: X-OTX-API-KEY header
    Data: exported threat indicators

AbuseIPDB — IP abuse reports
    URL: https://api.abuseipdb.com/api/v2/blacklist
    Auth: Key header
    Data: most reported abusive IPs

Geo-enrichment: ipinfo.io (primary), freeipapi.com (fallback)
"""

from __future__ import annotations

import csv
import io
import logging

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)

FEODO_URL = "https://feodotracker.abuse.ch/downloads/ipblocklist.json"
URLHAUS_URL = "https://urlhaus-api.abuse.ch/v1/urls/recent/limit/100/"
C2INTEL_URL = "https://raw.githubusercontent.com/drb-ra/C2IntelFeeds/master/feeds/IPC2s-30day.csv"
OTX_URL = "https://otx.alienvault.com/api/v1/indicators/export"
ABUSEIPDB_URL = "https://api.abuseipdb.com/api/v2/blacklist"

SEVERITY_MAP = {"feodo": "HIGH", "urlhaus": "HIGH", "c2intel": "MEDIUM", "otx": "MEDIUM", "abuseipdb": "MEDIUM"}


class CyberThreatExtractor(BaseExtractor):
    """Aggregates indicators from 5 cyber threat feeds.

    Post-collection structure per item:
        id=<source>:<ip_or_url>, source=<feed_name>, domain="cyber",
        title=<indicator>, severity, tags=[threat_type],
        extra={indicator_type, port, malware, confidence, ...}
    """

    SOURCE = "cyber_threats"
    DOMAIN = "cyber"
    REDIS_KEY = "cyber:threats:v2"
    TTL_SECONDS = 10800  # 3h

    def __init__(self, otx_key: str = "", abuseipdb_key: str = "", limit: int = 200):
        super().__init__()
        self._otx_key = otx_key
        self._abuseipdb_key = abuseipdb_key
        self._limit = limit

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        items: list[IntelItem] = []
        items.extend(await self._fetch_feodo(session))
        items.extend(await self._fetch_urlhaus(session))
        items.extend(await self._fetch_c2intel(session))
        if self._otx_key:
            items.extend(await self._fetch_otx(session))
        if self._abuseipdb_key:
            items.extend(await self._fetch_abuseipdb(session))
        return items[:self._limit]

    async def _fetch_feodo(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        try:
            async with session.get(FEODO_URL) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
            items: list[IntelItem] = []
            for entry in data[:50]:
                ip = entry.get("ip_address", "")
                items.append(IntelItem(
                    id=f"feodo:{ip}",
                    source="feodo",
                    domain="cyber",
                    title=f"Botnet C2: {ip}",
                    severity="HIGH",
                    tags=["botnet", "c2"],
                    extra={
                        "indicator_type": "ip",
                        "port": entry.get("port"),
                        "malware": entry.get("malware"),
                        "status": entry.get("status"),
                        "first_seen": entry.get("first_seen"),
                        "last_online": entry.get("last_online"),
                    },
                ))
            return items
        except Exception as exc:
            logger.warning("Feodo fetch failed: %s", exc)
            return []

    async def _fetch_urlhaus(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        try:
            async with session.post(URLHAUS_URL) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
            items: list[IntelItem] = []
            for entry in data.get("urls", [])[:50]:
                url = entry.get("url", "")
                items.append(IntelItem(
                    id=f"urlhaus:{entry.get('id', '')}",
                    source="urlhaus",
                    domain="cyber",
                    title=f"Malicious URL: {url[:100]}",
                    url=url,
                    severity="HIGH",
                    tags=["malware", "url"],
                    extra={
                        "indicator_type": "url",
                        "threat": entry.get("threat", ""),
                        "url_status": entry.get("url_status", ""),
                        "host": entry.get("host", ""),
                        "date_added": entry.get("date_added", ""),
                    },
                ))
            return items
        except Exception as exc:
            logger.warning("URLhaus fetch failed: %s", exc)
            return []

    async def _fetch_c2intel(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        try:
            async with session.get(C2INTEL_URL) as resp:
                resp.raise_for_status()
                text = await resp.text()
            items: list[IntelItem] = []
            reader = csv.reader(io.StringIO(text))
            for i, row in enumerate(reader):
                if i == 0 or not row:
                    continue
                ip = row[0].strip()
                if not ip or ip.startswith("#"):
                    continue
                items.append(IntelItem(
                    id=f"c2intel:{ip}",
                    source="c2intel",
                    domain="cyber",
                    title=f"C2 Server: {ip}",
                    severity="MEDIUM",
                    tags=["c2"],
                    extra={"indicator_type": "ip"},
                ))
                if len(items) >= 50:
                    break
            return items
        except Exception as exc:
            logger.warning("C2IntelFeeds fetch failed: %s", exc)
            return []

    async def _fetch_otx(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        try:
            headers = {"X-OTX-API-KEY": self._otx_key}
            async with session.get(OTX_URL, headers=headers) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
            items: list[IntelItem] = []
            for entry in data.get("results", [])[:30]:
                indicator = entry.get("indicator", "")
                items.append(IntelItem(
                    id=f"otx:{indicator}",
                    source="otx",
                    domain="cyber",
                    title=f"OTX: {indicator}",
                    severity="MEDIUM",
                    tags=["otx", entry.get("type", "")],
                    extra={
                        "indicator_type": entry.get("type", ""),
                        "title": entry.get("title", ""),
                        "description": entry.get("description", "")[:200],
                    },
                ))
            return items
        except Exception as exc:
            logger.warning("OTX fetch failed: %s", exc)
            return []

    async def _fetch_abuseipdb(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        try:
            headers = {"Key": self._abuseipdb_key, "Accept": "application/json"}
            params = {"confidenceMinimum": "90", "limit": "30"}
            async with session.get(ABUSEIPDB_URL, headers=headers, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
            items: list[IntelItem] = []
            for entry in data.get("data", []):
                ip = entry.get("ipAddress", "")
                items.append(IntelItem(
                    id=f"abuseipdb:{ip}",
                    source="abuseipdb",
                    domain="cyber",
                    title=f"Abusive IP: {ip}",
                    severity="MEDIUM",
                    tags=["abuse"],
                    extra={
                        "indicator_type": "ip",
                        "abuse_confidence": entry.get("abuseConfidenceScore"),
                        "country": entry.get("countryCode", ""),
                        "total_reports": entry.get("totalReports"),
                    },
                ))
            return items
        except Exception as exc:
            logger.warning("AbuseIPDB fetch failed: %s", exc)
            return []
