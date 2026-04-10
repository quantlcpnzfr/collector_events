"""Government travel & health advisories extractor.

Data sources (mirrors worldmonitor seed-security-advisories.mjs):

US State Department — Travel advisories RSS
    URL: https://travel.state.gov/content/travel/en/traveladvisories/traveladvisories.xml/
    Auth: none

UK FCDO — Travel advice Atom feed
    URL: https://www.gov.uk/government/organisations/foreign-commonwealth-development-office.atom
    Auth: none

US Embassies — Security alerts RSS (13 high-risk locations)
    URLs: https://{country}.usembassy.gov/security-alerts/feed/
    Countries: iraq, afghanistan, ukraine, israel, haiti, southsudan, niger, chad,
               libya, sudan, yemen, somalia, syria

CDC — Health advisories
    URL: https://tools.cdc.gov/api/v2/resources/media/316422.rss
    Auth: none

ECDC — European Centre for Disease Prevention and Control (5 feeds)
    URLs: Multiple ECDC topic-specific RSS feeds

WHO — World Health Organization
    URLs: https://www.who.int/feeds/entity/don/en/rss.xml
           https://www.who.int/feeds/entity/csr/don/en/rss.xml
"""

from __future__ import annotations

import hashlib
import logging
from xml.etree import ElementTree

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)

# ─── Feed definitions ────────────────────────────────────────────────

ADVISORY_FEEDS: list[dict[str, str]] = [
    # US State Department
    {"name": "US State Dept Travel", "url": "https://travel.state.gov/content/travel/en/traveladvisories/traveladvisories.xml/", "category": "travel", "source_org": "US State Dept"},
    # UK FCDO
    {"name": "UK FCDO Travel", "url": "https://www.gov.uk/government/organisations/foreign-commonwealth-development-office.atom", "category": "travel", "source_org": "UK FCDO"},
    # US Embassies (high-risk)
    {"name": "Embassy Iraq", "url": "https://iraq.usembassy.gov/security-alerts/feed/", "category": "security", "source_org": "US Embassy"},
    {"name": "Embassy Afghanistan", "url": "https://afghanistan.usembassy.gov/security-alerts/feed/", "category": "security", "source_org": "US Embassy"},
    {"name": "Embassy Ukraine", "url": "https://ukraine.usembassy.gov/security-alerts/feed/", "category": "security", "source_org": "US Embassy"},
    {"name": "Embassy Israel", "url": "https://israel.usembassy.gov/security-alerts/feed/", "category": "security", "source_org": "US Embassy"},
    {"name": "Embassy Haiti", "url": "https://haiti.usembassy.gov/security-alerts/feed/", "category": "security", "source_org": "US Embassy"},
    {"name": "Embassy South Sudan", "url": "https://southsudan.usembassy.gov/security-alerts/feed/", "category": "security", "source_org": "US Embassy"},
    {"name": "Embassy Niger", "url": "https://niger.usembassy.gov/security-alerts/feed/", "category": "security", "source_org": "US Embassy"},
    {"name": "Embassy Chad", "url": "https://chad.usembassy.gov/security-alerts/feed/", "category": "security", "source_org": "US Embassy"},
    {"name": "Embassy Libya", "url": "https://libya.usembassy.gov/security-alerts/feed/", "category": "security", "source_org": "US Embassy"},
    {"name": "Embassy Sudan", "url": "https://sudan.usembassy.gov/security-alerts/feed/", "category": "security", "source_org": "US Embassy"},
    {"name": "Embassy Yemen", "url": "https://yemen.usembassy.gov/security-alerts/feed/", "category": "security", "source_org": "US Embassy"},
    {"name": "Embassy Somalia", "url": "https://somalia.usembassy.gov/security-alerts/feed/", "category": "security", "source_org": "US Embassy"},
    {"name": "Embassy Syria", "url": "https://syria.usembassy.gov/security-alerts/feed/", "category": "security", "source_org": "US Embassy"},
    # CDC
    {"name": "CDC Health Advisories", "url": "https://tools.cdc.gov/api/v2/resources/media/316422.rss", "category": "health", "source_org": "CDC"},
    # WHO
    {"name": "WHO Disease Outbreak", "url": "https://www.who.int/feeds/entity/don/en/rss.xml", "category": "health", "source_org": "WHO"},
    {"name": "WHO CSR", "url": "https://www.who.int/feeds/entity/csr/don/en/rss.xml", "category": "health", "source_org": "WHO"},
    # ECDC
    {"name": "ECDC News", "url": "https://www.ecdc.europa.eu/en/taxonomy/term/1/feed", "category": "health", "source_org": "ECDC"},
    {"name": "ECDC Threats", "url": "https://www.ecdc.europa.eu/en/taxonomy/term/10/feed", "category": "health", "source_org": "ECDC"},
    {"name": "ECDC Publications", "url": "https://www.ecdc.europa.eu/en/taxonomy/term/18/feed", "category": "health", "source_org": "ECDC"},
    {"name": "ECDC Surveillance", "url": "https://www.ecdc.europa.eu/en/taxonomy/term/8/feed", "category": "health", "source_org": "ECDC"},
    {"name": "ECDC Risk Assessment", "url": "https://www.ecdc.europa.eu/en/taxonomy/term/2/feed", "category": "health", "source_org": "ECDC"},
]


class AdvisoryExtractor(BaseExtractor):
    """Fetches travel and health advisories from government/health RSS feeds.

    Post-collection structure per item:
        id="advisory:{md5hash}", source=<org>, domain="advisories",
        title, url, ts, tags=[category],
        extra={feed_name, category, source_org, description}
    """

    SOURCE = "advisories"
    DOMAIN = "advisories"
    REDIS_KEY = "intelligence:advisories:v1"
    TTL_SECONDS = 10800  # 3h

    def __init__(self, feeds: list[dict] | None = None, max_concurrent: int = 10):
        super().__init__()
        self._feeds = feeds or ADVISORY_FEEDS
        self._max_concurrent = max_concurrent

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        import asyncio
        sem = asyncio.Semaphore(self._max_concurrent)
        tasks = [self._fetch_feed(session, sem, f) for f in self._feeds]
        results = await asyncio.gather(*tasks)
        all_items: list[IntelItem] = []
        for batch in results:
            all_items.extend(batch)
        return all_items

    async def _fetch_feed(
        self,
        session: aiohttp.ClientSession,
        sem: "asyncio.Semaphore",
        feed: dict,
    ) -> list[IntelItem]:
        async with sem:
            try:
                async with session.get(feed["url"], timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return []
                    text = await resp.text()
            except Exception as exc:
                logger.debug("Advisory feed %s failed: %s", feed["name"], exc)
                return []

        return self._parse_xml(text, feed)

    def _parse_xml(self, xml_text: str, feed: dict) -> list[IntelItem]:
        try:
            root = ElementTree.fromstring(xml_text)
        except ElementTree.ParseError:
            return []

        items: list[IntelItem] = []
        ns_atom = "{http://www.w3.org/2005/Atom}"

        # RSS 2.0
        for item_el in root.iter("item"):
            title = (item_el.findtext("title") or "").strip()
            link = (item_el.findtext("link") or "").strip()
            pub_date = (item_el.findtext("pubDate") or "").strip()
            desc = (item_el.findtext("description") or "").strip()
            item_id = hashlib.md5(f"{feed['url']}:{title}".encode()).hexdigest()
            items.append(IntelItem(
                id=f"advisory:{item_id}",
                source=feed.get("source_org", "advisory"),
                domain="advisories",
                title=title[:300],
                url=link,
                body=desc[:500],
                ts=pub_date,
                tags=[feed.get("category", "advisory")],
                extra={
                    "feed_name": feed["name"],
                    "category": feed.get("category", ""),
                    "source_org": feed.get("source_org", ""),
                },
            ))

        # Atom
        for entry in root.iter(f"{ns_atom}entry"):
            title = (entry.findtext(f"{ns_atom}title") or "").strip()
            link_el = entry.find(f"{ns_atom}link")
            link = link_el.get("href", "") if link_el is not None else ""
            updated = (entry.findtext(f"{ns_atom}updated") or "").strip()
            summary = (entry.findtext(f"{ns_atom}summary") or "").strip()
            item_id = hashlib.md5(f"{feed['url']}:{title}".encode()).hexdigest()
            items.append(IntelItem(
                id=f"advisory:{item_id}",
                source=feed.get("source_org", "advisory"),
                domain="advisories",
                title=title[:300],
                url=link,
                body=summary[:500],
                ts=updated,
                tags=[feed.get("category", "advisory")],
                extra={
                    "feed_name": feed["name"],
                    "category": feed.get("category", ""),
                    "source_org": feed.get("source_org", ""),
                },
            ))

        return items[:15]  # cap per feed
