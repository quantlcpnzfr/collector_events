"""U.S. Bureau of Labor Statistics (BLS) extractor.

Upstream: https://api.bls.gov/publicAPI/v2/timeseries/data/
Auth: optional registrationKey (higher rate limits)

Mirrors worldmonitor GetBlsSeries handler.
Key series: CPI-U (CUSR0000SA0), Unemployment (LNS14000000),
            Nonfarm Payrolls (CES0000000001), PPI (WPSFD4).
"""

from __future__ import annotations

import logging

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)

BLS_API = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

BLS_SERIES = {
    "CUSR0000SA0": "CPI-U All Items (Inflation)",
    "LNS14000000": "Unemployment Rate",
    "CES0000000001": "Total Nonfarm Payrolls",
    "WPSFD4": "PPI Final Demand",
    "CES0500000003": "Average Hourly Earnings",
    "LNS11300000": "Labor Force Participation Rate",
}


class BlsSeriesExtractor(BaseExtractor):
    """Fetches key BLS labor statistics series.

    Post-collection: one IntelItem per series with recent observations.
    """

    SOURCE = "bls"
    DOMAIN = "economic"
    REDIS_KEY = "economic:bls:series:v1"
    TTL_SECONDS = 86400  # 24h

    def __init__(self, api_key: str = ""):
        super().__init__()
        self._api_key = api_key

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        from datetime import datetime, timezone
        current_year = datetime.now(timezone.utc).year
        payload = {
            "seriesid": list(BLS_SERIES.keys()),
            "startyear": str(current_year - 1),
            "endyear": str(current_year),
        }
        if self._api_key:
            payload["registrationkey"] = self._api_key

        async with session.post(BLS_API, json=payload) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

        items: list[IntelItem] = []
        for series in data.get("Results", {}).get("series", []):
            sid = series.get("seriesID", "")
            label = BLS_SERIES.get(sid, sid)
            observations = []
            for dp in series.get("data", [])[:12]:
                observations.append({
                    "year": dp.get("year"),
                    "period": dp.get("period"),
                    "value": dp.get("value"),
                })
            if observations:
                latest = observations[0]
                items.append(IntelItem(
                    id=f"bls:{sid}",
                    source=self.SOURCE,
                    domain=self.DOMAIN,
                    title=f"BLS: {label}",
                    tags=["labor", "bls", "macro"],
                    extra={
                        "series_id": sid,
                        "series_name": label,
                        "latest_value": latest.get("value"),
                        "latest_period": f"{latest.get('year')}-{latest.get('period')}",
                        "observations": observations,
                    },
                ))
        return items
