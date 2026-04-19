"""Travel advisories extractor (23 RSS/Atom feeds)."""

from __future__ import annotations

import asyncio
import hashlib
from forex_shared.logging.get_logger import get_logger
import re
from xml.etree import ElementTree

import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load
from ..processors.country_resolver import CountryResolver

logger = get_logger(__name__)

_ADVISORY_FEEDS: list[dict] = _load("advisories.json")


class AdvisoryExtractor(BaseExtractor):
    """Fetches travel/health advisories from 23 government feeds."""

    SOURCE = "advisories"
    DOMAIN = "advisories"
    REDIS_KEY = "intelligence:advisories:v1"
    TTL_SECONDS = 10800  # 3h

    def __init__(
        self,
        feeds: list[dict] | None = None,
        max_concurrent: int = 10,
    ):
        super().__init__()
        self._feeds = feeds or _ADVISORY_FEEDS
        self._max_concurrent = max_concurrent

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        sem = asyncio.Semaphore(self._max_concurrent)
        tasks = [self._fetch_feed(session, feed, sem) for feed in self._feeds]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        items: list[IntelItem] = []
        for res in results:
            if isinstance(res, list):
                items.extend(res)
            elif isinstance(res, Exception):
                logger.debug("Advisory feed error: %s", res)
        return items

    async def _fetch_feed(
        self,
        session: aiohttp.ClientSession,
        feed: dict,
        sem: asyncio.Semaphore,
    ) -> list[IntelItem]:
        url = feed.get("url", "")
        source_name = feed.get("name", url)
        domain_tag = feed.get("domain", "advisories")

        async with sem:
            try:
                timeout = aiohttp.ClientTimeout(total=10)
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status != 200:
                        return []
                    text = await resp.text()
            except Exception as exc:
                logger.debug("Feed %s failed: %s", source_name, exc)
                return []

        return self._parse_xml(text, source_name, domain_tag, url)

    def _parse_xml(
        self,
        text: str,
        source_name: str,
        domain_tag: str,
        feed_url: str,
    ) -> list[IntelItem]:
        items: list[IntelItem] = []
        try:
            root = ElementTree.fromstring(text)
        except ElementTree.ParseError:
            return []

        # RSS 2.0 <item> elements
        rss_items = root.findall(".//item")
        # Atom <entry> elements (with namespace)
        atom_ns = "http://www.w3.org/2005/Atom"
        atom_items = root.findall(f".//{{{atom_ns}}}entry") if not rss_items else []

        entries = rss_items or atom_items
        count = 0
        for entry in entries:
            if count >= 15:
                break

            if rss_items:
                title = (entry.findtext("title") or "").strip()
                link = (entry.findtext("link") or "").strip()
                desc = (entry.findtext("description") or "").strip()
                pub_date = (entry.findtext("pubDate") or "").strip()
            else:
                title = (entry.findtext(f"{{{atom_ns}}}title") or "").strip()
                link_el = entry.find(f"{{{atom_ns}}}link")
                link = (link_el.get("href", "") if link_el is not None else "").strip()
                desc = (entry.findtext(f"{{{atom_ns}}}summary") or
                        entry.findtext(f"{{{atom_ns}}}content") or "").strip()
                pub_date = (entry.findtext(f"{{{atom_ns}}}updated") or
                            entry.findtext(f"{{{atom_ns}}}published") or "").strip()

            if not title:
                continue

            # Strip HTML tags from body
            body = re.sub(r"<[^>]+>", "", desc)[:500]
            title = title[:300]

            item_id = hashlib.md5(f"{link}:{title}".encode()).hexdigest()

            items.append(IntelItem(
                id=f"adv:{item_id}",
                source=source_name,
                domain="advisories",
                title=title,
                url=link,
                body=body,
                ts=pub_date,
                tags=["advisory", domain_tag],
                country=CountryResolver().resolve(f"{source_name} {title} {body}"),
                extra={
                    "feed": source_name,
                    "feed_url": feed_url,
                },
            ))
            count += 1
        return items
