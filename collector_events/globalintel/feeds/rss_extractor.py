"""RSS / Atom feed aggregation.

Mirrors worldmonitor's ``src/config/feeds.ts`` + ``src/services/rss.ts``:
100+ curated feeds across 20+ categories with source credibility tiering
and propaganda-risk metadata.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from xml.etree import ElementTree

import aiohttp

from ..base import CHROME_UA, BaseExtractor, IntelItem

logger = logging.getLogger(__name__)


# ─── Feed catalogue ──────────────────────────────────────────────────

@dataclass(frozen=True)
class FeedDef:
    """One RSS/Atom feed definition."""

    name: str
    url: str
    category: str = ""
    lang: str = "en"
    source_type: str = "mainstream"   # wire | gov | intel | mainstream | market | tech
    propaganda_risk: str = "low"      # low | medium | high
    state_affiliation: str = ""       # e.g. "Russia", "China"


# fmt: off
# ── Core geopolitics/world feeds (subset — add more from feeds.ts) ──
FEEDS: list[FeedDef] = [
    # Wire services
    FeedDef("Reuters World", "https://news.google.com/rss/search?q=site:reuters.com+world&hl=en-US&gl=US&ceid=US:en", "politics", source_type="wire"),
    FeedDef("AP News", "https://news.google.com/rss/search?q=site:apnews.com+world&hl=en-US&gl=US&ceid=US:en", "politics", source_type="wire"),
    FeedDef("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml", "politics", source_type="wire"),

    # US
    FeedDef("CNN World", "https://rss.cnn.com/rss/edition_world.rss", "us"),
    FeedDef("NPR", "https://feeds.npr.org/1001/rss.xml", "us"),
    FeedDef("WSJ", "https://feeds.a.dj.com/rss/RSSWorldNews.xml", "us"),

    # Europe
    FeedDef("France 24 EN", "https://www.france24.com/en/rss", "europe"),
    FeedDef("EuroNews EN", "https://www.euronews.com/rss?format=xml", "europe"),
    FeedDef("Le Monde EN", "https://www.lemonde.fr/en/rss/une.xml", "europe"),
    FeedDef("DW News EN", "https://rss.dw.com/xml/rss-en-all", "europe"),
    FeedDef("BBC Russian", "https://feeds.bbci.co.uk/russian/rss.xml", "europe", lang="ru"),
    FeedDef("Meduza", "https://meduza.io/rss/all", "europe", lang="ru", source_type="intel"),
    FeedDef("Kyiv Independent", "https://news.google.com/rss/search?q=site:kyivindependent.com+when:3d&hl=en-US&gl=US&ceid=US:en", "europe"),
    FeedDef("TASS", "https://news.google.com/rss/search?q=site:tass.com+OR+TASS+Russia+when:1d&hl=en-US&gl=US&ceid=US:en", "europe", propaganda_risk="high", state_affiliation="Russia"),
    FeedDef("RT", "https://www.rt.com/rss/", "europe", propaganda_risk="high", state_affiliation="Russia"),

    # Middle East
    FeedDef("BBC Middle East", "https://feeds.bbci.co.uk/news/world/middle_east/rss.xml", "middleeast"),
    FeedDef("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml", "middleeast", propaganda_risk="medium", state_affiliation="Qatar"),
    FeedDef("Guardian ME", "https://www.theguardian.com/world/middleeast/rss", "middleeast"),

    # Asia
    FeedDef("BBC Asia", "https://feeds.bbci.co.uk/news/world/asia/rss.xml", "asia"),
    FeedDef("The Diplomat", "https://thediplomat.com/feed/", "asia"),
    FeedDef("Nikkei Asia", "https://news.google.com/rss/search?q=site:asia.nikkei.com+when:3d&hl=en-US&gl=US&ceid=US:en", "asia"),
    FeedDef("Xinhua", "https://news.google.com/rss/search?q=site:xinhuanet.com+when:1d&hl=en-US&gl=US&ceid=US:en", "asia", propaganda_risk="high", state_affiliation="China"),

    # Africa
    FeedDef("BBC Africa", "https://feeds.bbci.co.uk/news/world/africa/rss.xml", "africa"),
    FeedDef("News24 SA", "https://feeds.news24.com/articles/news24/TopStories/rss", "africa"),

    # Latin America
    FeedDef("BBC LatAm", "https://feeds.bbci.co.uk/news/world/latin_america/rss.xml", "latam"),
    FeedDef("Guardian Americas", "https://www.theguardian.com/world/americas/rss", "latam"),

    # Finance / Markets
    FeedDef("CNBC", "https://www.cnbc.com/id/100003114/device/rss/rss.html", "finance", source_type="market"),
    FeedDef("Yahoo Finance", "https://finance.yahoo.com/news/rssindex", "finance", source_type="market"),
    FeedDef("Financial Times", "https://www.ft.com/rss/home", "finance", source_type="market"),

    # Government / Institutions
    FeedDef("Federal Reserve", "https://www.federalreserve.gov/feeds/press_all.xml", "gov", source_type="gov"),
    FeedDef("SEC", "https://www.sec.gov/news/pressreleases.rss", "gov", source_type="gov"),
    FeedDef("UN News", "https://news.un.org/feed/subscribe/en/news/all/rss.xml", "gov", source_type="gov"),
    FeedDef("WHO", "https://www.who.int/rss-feeds/news-english.xml", "gov", source_type="gov"),

    # Think tanks & analysis
    FeedDef("Foreign Policy", "https://foreignpolicy.com/feed/", "thinktanks", source_type="intel"),
    FeedDef("Foreign Affairs", "https://www.foreignaffairs.com/rss.xml", "thinktanks", source_type="intel"),
    FeedDef("RAND", "https://www.rand.org/pubs/articles.xml", "thinktanks", source_type="intel"),
    FeedDef("CrisisWatch", "https://www.crisisgroup.org/rss", "crisis", source_type="intel"),
    FeedDef("IAEA", "https://www.iaea.org/feeds/topnews", "crisis", source_type="gov"),

    # Defence / OSINT
    FeedDef("War on the Rocks", "https://warontherocks.com/feed", "defense", source_type="intel"),
    FeedDef("Jamestown", "https://jamestown.org/feed/", "defense", source_type="intel"),

    # Energy
    FeedDef("Oil & Gas", "https://news.google.com/rss/search?q=(oil+price+OR+OPEC+OR+natural+gas+OR+LNG)+when:2d&hl=en-US&gl=US&ceid=US:en", "energy", source_type="market"),
]
# fmt: on


# ─── RSS parser ──────────────────────────────────────────────────────

def _parse_rss_xml(xml_text: str, feed: FeedDef) -> list[IntelItem]:
    """Parse RSS 2.0 or Atom XML into IntelItem list."""
    items: list[IntelItem] = []
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return items

    # Atom namespace
    atom_ns = "{http://www.w3.org/2005/Atom}"

    # Try RSS 2.0 <channel><item>
    for el in root.iter("item"):
        title = (el.findtext("title") or "").strip()
        link = (el.findtext("link") or "").strip()
        desc = (el.findtext("description") or "").strip()
        pub_date = (el.findtext("pubDate") or "").strip()
        guid = (el.findtext("guid") or link or "")
        item_id = hashlib.md5(guid.encode()).hexdigest()[:16]
        items.append(IntelItem(
            id=f"{feed.name.lower().replace(' ', '_')}:{item_id}",
            source=feed.name,
            domain="feeds",
            title=title[:500],
            url=link,
            body=desc[:1000],
            ts=pub_date,
            tags=[feed.category, feed.source_type],
            extra={
                "lang": feed.lang,
                "propaganda_risk": feed.propaganda_risk,
                "state_affiliation": feed.state_affiliation,
            },
        ))

    # Try Atom <entry>
    if not items:
        for el in root.iter(f"{atom_ns}entry"):
            title = (el.findtext(f"{atom_ns}title") or "").strip()
            link_el = el.find(f"{atom_ns}link")
            link = link_el.get("href", "") if link_el is not None else ""
            summary = (el.findtext(f"{atom_ns}summary") or "").strip()
            updated = (el.findtext(f"{atom_ns}updated") or "").strip()
            entry_id = (el.findtext(f"{atom_ns}id") or link or "")
            item_id = hashlib.md5(entry_id.encode()).hexdigest()[:16]
            items.append(IntelItem(
                id=f"{feed.name.lower().replace(' ', '_')}:{item_id}",
                source=feed.name,
                domain="feeds",
                title=title[:500],
                url=link,
                body=summary[:1000],
                ts=updated,
                tags=[feed.category, feed.source_type],
                extra={
                    "lang": feed.lang,
                    "propaganda_risk": feed.propaganda_risk,
                    "state_affiliation": feed.state_affiliation,
                },
            ))

    return items


# ─── Extractor ───────────────────────────────────────────────────────

class RSSFeedExtractor(BaseExtractor):
    """Fetches and parses a batch of RSS/Atom feeds concurrently.

    Mirrors worldmonitor pattern: ``src/services/rss.ts`` + ``src/config/feeds.ts``

    Source URLs: see ``FEEDS`` list above (100+ feeds available).
    Post-collection structure: list of ``IntelItem`` with:
        - Credibility tiering (wire > gov > intel > mainstream > market > tech)
        - Propaganda risk (high / medium / low)
        - State affiliation tracking
    """

    SOURCE = "rss_feeds"
    DOMAIN = "feeds"
    REDIS_KEY = "feeds:rss-digest:v1"
    TTL_SECONDS = 900  # 15min — matches worldmonitor feed digest cache

    def __init__(
        self,
        feeds: list[FeedDef] | None = None,
        max_concurrent: int = 20,
        per_feed_timeout: int = 8,
    ):
        super().__init__()
        self._feeds = feeds or FEEDS
        self._max_concurrent = max_concurrent
        self._per_feed_timeout = per_feed_timeout

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        import asyncio
        sem = asyncio.Semaphore(self._max_concurrent)
        all_items: list[IntelItem] = []

        async def _fetch_one(feed: FeedDef) -> list[IntelItem]:
            async with sem:
                try:
                    timeout = aiohttp.ClientTimeout(total=self._per_feed_timeout)
                    async with session.get(feed.url, timeout=timeout) as resp:
                        if resp.status != 200:
                            logger.debug("Feed %s returned %d", feed.name, resp.status)
                            return []
                        text = await resp.text()
                        return _parse_rss_xml(text, feed)
                except Exception as exc:
                    logger.debug("Feed %s error: %s", feed.name, exc)
                    return []

        tasks = [_fetch_one(f) for f in self._feeds]
        results = await asyncio.gather(*tasks)
        for batch in results:
            all_items.extend(batch)
        return all_items
