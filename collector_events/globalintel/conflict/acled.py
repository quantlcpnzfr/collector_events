"""ACLED armed conflict event extractor."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load
from ..utils import safe_float

logger = logging.getLogger(__name__)

_CFG = _load("conflict.json")["acled"]
_OAUTH_URL = _CFG["oauth_url"]
_API_URL = _CFG["api_url"]
_CONFLICT_TYPES = _CFG["conflict_types"]


class ACLEDExtractor(BaseExtractor):
    """Fetches conflict events from ACLED with OAuth2 authentication.

    Auth flow (mirrors worldmonitor acled-oauth.mjs):
        1. POST /oauth/token  {username, password, grant_type=password, client_id=acled}
        2. Falls back to static access_token if OAuth fails.
        3. Falls back to legacy email+key params if both fail.

    Severity: HIGH if fatalities > 0 or riot/clash sub-type, else MEDIUM.
    """

    SOURCE = "acled"
    DOMAIN = "conflict"
    REDIS_KEY = "conflict:acled:v1"
    TTL_SECONDS = 900

    def __init__(
        self,
        email: str = "",
        api_key: str = "",
        access_token: str = "",
        password: str = "",
        limit: int = 500,
    ):
        super().__init__()
        self._email = email
        self._api_key = api_key
        self._access_token = access_token
        self._password = password or api_key
        self._limit = limit

    async def _get_oauth_token(self, session: aiohttp.ClientSession) -> str | None:
        if not self._email or not self._password:
            return None
        try:
            payload = {
                "username": self._email,
                "password": self._password,
                "grant_type": "password",
                "client_id": "acled",
            }
            async with session.post(_OAUTH_URL, data=payload) as resp:
                if resp.status == 200:
                    body = await resp.json(content_type=None)
                    token = body.get("access_token")
                    if token:
                        return token
        except Exception as exc:
            logger.debug("ACLED OAuth failed: %s", exc)
        return None

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        token = await self._get_oauth_token(session)
        if not token:
            token = self._access_token

        since = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        params: dict = {
            "event_date": f"{since}|",
            "event_date_where": "BETWEEN",
            "event_type": "|".join(_CONFLICT_TYPES),
            "limit": str(self._limit),
        }
        if token:
            params["acled_access"] = token
        else:
            if self._email:
                params["email"] = self._email
            if self._api_key:
                params["key"] = self._api_key

        async with session.get(_API_URL, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

        events = data.get("data", [])
        items: list[IntelItem] = []
        for ev in events:
            fatalities = int(ev.get("fatalities", 0) or 0)
            sub_type = (ev.get("sub_event_type") or "").lower()
            severity = "HIGH" if fatalities > 0 or "riot" in sub_type or "clash" in sub_type else "MEDIUM"
            items.append(IntelItem(
                id=f"acled:{ev.get('data_id', '')}",
                source="acled",
                domain="conflict",
                title=f"{ev.get('event_type', '')}: {ev.get('notes', '')[:200]}",
                lat=safe_float(ev.get("latitude")),
                lon=safe_float(ev.get("longitude")),
                country=ev.get("country", ""),
                ts=ev.get("event_date", ""),
                severity=severity,
                tags=[ev.get("event_type", ""), ev.get("sub_event_type", "")],
                extra={
                    "event_type": ev.get("event_type", ""),
                    "sub_event_type": ev.get("sub_event_type", ""),
                    "fatalities": fatalities,
                    "actor1": ev.get("actor1", ""),
                    "actor2": ev.get("actor2", ""),
                    "admin1": ev.get("admin1", ""),
                    "source": ev.get("source", ""),
                },
            ))
        return items
