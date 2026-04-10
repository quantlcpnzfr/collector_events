"""Natural disaster & environment extractors.

Data sources (mirrors worldmonitor data-sources.mdx + seed scripts):

USGS Earthquake Hazards — US Geological Survey
    URL: https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson
    Auth: none
    Data: earthquakes magnitude 4.5+ in last 24h, GeoJSON format

NASA FIRMS — Fire Information for Resource Management System
    URL: https://firms.modaps.eosdis.nasa.gov/api/country/csv/{api_key}/VIIRS_SNPP_NRT/{country}/1
    Auth: API key (MAP_KEY)
    Data: active fire detections via VIIRS satellite

GDACS — Global Disaster Alerting Coordination System
    URL: https://www.gdacs.org/xml/rss.xml
    Auth: none
    Data: flood, cyclone, earthquake, drought, volcano alerts

NASA EONET — Earth Observatory Natural Event Tracker
    URL: https://eonet.gsfc.nasa.gov/api/v3/events
    Auth: none
    Data: wildfires, storms, volcanic eruptions, icebergs
"""

from __future__ import annotations

import logging

import aiohttp

from ..base import BaseExtractor, IntelItem

logger = logging.getLogger(__name__)

# ─── USGS earthquakes ────────────────────────────────────────────────

USGS_EARTHQUAKE_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson"


class USGSEarthquakeExtractor(BaseExtractor):
    """Fetches M4.5+ earthquakes from USGS.

    Post-collection structure per item:
        id="usgs:{event_id}", source="usgs", domain="environment",
        title=<place>, lat/lon, severity by magnitude,
        extra={magnitude, depth_km, tsunami, felt, alert}
    """

    SOURCE = "usgs"
    DOMAIN = "environment"
    REDIS_KEY = "environment:earthquakes:v1"
    TTL_SECONDS = 3600  # 1h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        async with session.get(USGS_EARTHQUAKE_URL) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

        items: list[IntelItem] = []
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [0, 0, 0])
            mag = props.get("mag", 0) or 0
            severity = "HIGH" if mag >= 6.0 else "MEDIUM" if mag >= 5.0 else "LOW"
            items.append(IntelItem(
                id=f"usgs:{feature.get('id', '')}",
                source="usgs",
                domain="environment",
                title=props.get("place", "Unknown"),
                url=props.get("url", ""),
                lat=coords[1] if len(coords) > 1 else None,
                lon=coords[0] if len(coords) > 0 else None,
                ts=str(props.get("time", "")),
                severity=severity,
                tags=["earthquake"],
                extra={
                    "magnitude": mag,
                    "depth_km": coords[2] if len(coords) > 2 else None,
                    "tsunami": props.get("tsunami", 0),
                    "felt": props.get("felt"),
                    "alert": props.get("alert"),
                    "mag_type": props.get("magType", ""),
                },
            ))
        return items


# ─── NASA FIRMS fire detections ──────────────────────────────────────

FIRMS_API_BASE = "https://firms.modaps.eosdis.nasa.gov/api/country/csv"


class NASAFireExtractor(BaseExtractor):
    """Fetches active fire detections from NASA FIRMS.

    Post-collection structure per item:
        id="fire:{lat}:{lon}:{acq_date}", source="nasa_firms", domain="environment",
        lat/lon, country, severity by confidence/FRP,
        extra={brightness, frp, confidence, satellite, daynight}
    """

    SOURCE = "nasa_firms"
    DOMAIN = "environment"
    REDIS_KEY = "environment:fires:v1"
    TTL_SECONDS = 7200  # 2h

    # High-impact countries
    COUNTRIES = ["USA", "AUS", "BRA", "CAN", "GRC", "PRT", "ESP", "TUR"]

    def __init__(self, api_key: str = "", countries: list[str] | None = None):
        super().__init__()
        self._api_key = api_key
        self._countries = countries or self.COUNTRIES

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        if not self._api_key:
            logger.warning("NASA FIRMS API key not configured")
            return []

        items: list[IntelItem] = []
        for country in self._countries:
            url = f"{FIRMS_API_BASE}/{self._api_key}/VIIRS_SNPP_NRT/{country}/1"
            try:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        continue
                    text = await resp.text()
                lines = text.strip().split("\n")
                if len(lines) < 2:
                    continue
                headers = lines[0].split(",")
                for line in lines[1:51]:  # limit per country
                    vals = line.split(",")
                    if len(vals) < len(headers):
                        continue
                    row = dict(zip(headers, vals))
                    lat = _safe_float(row.get("latitude"))
                    lon = _safe_float(row.get("longitude"))
                    frp = _safe_float(row.get("frp", "0"))
                    conf = row.get("confidence", "")
                    severity = "HIGH" if conf in ("h", "high") or (frp and frp > 100) else "MEDIUM"
                    items.append(IntelItem(
                        id=f"fire:{lat}:{lon}:{row.get('acq_date', '')}",
                        source="nasa_firms",
                        domain="environment",
                        title=f"Fire detection: {country}",
                        lat=lat,
                        lon=lon,
                        country=country,
                        ts=f"{row.get('acq_date', '')}T{row.get('acq_time', '0000')}",
                        severity=severity,
                        tags=["fire", "wildfire"],
                        extra={
                            "brightness": _safe_float(row.get("bright_ti4")),
                            "frp": frp,
                            "confidence": conf,
                            "satellite": row.get("satellite", ""),
                            "daynight": row.get("daynight", ""),
                        },
                    ))
            except Exception as exc:
                logger.warning("FIRMS %s failed: %s", country, exc)
        return items


# ─── GDACS disaster alerts ──────────────────────────────────────────

GDACS_RSS_URL = "https://www.gdacs.org/xml/rss.xml"


class GDACSExtractor(BaseExtractor):
    """Fetches disaster alerts from GDACS RSS feed.

    Post-collection structure per item:
        id="gdacs:{guid}", source="gdacs", domain="environment",
        title, lat/lon, severity (mapped from GDACS alert level),
        extra={event_type, alert_level, country, population}
    """

    SOURCE = "gdacs"
    DOMAIN = "environment"
    REDIS_KEY = "environment:disasters:v1"
    TTL_SECONDS = 7200  # 2h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        from xml.etree import ElementTree

        async with session.get(GDACS_RSS_URL) as resp:
            resp.raise_for_status()
            text = await resp.text()

        items: list[IntelItem] = []
        try:
            root = ElementTree.fromstring(text)
        except ElementTree.ParseError:
            return items

        gdacs_ns = "{http://www.gdacs.org}"
        geo_ns = "{http://www.georss.org/georss}"

        for item_el in root.iter("item"):
            title = (item_el.findtext("title") or "").strip()
            link = (item_el.findtext("link") or "").strip()
            guid = (item_el.findtext("guid") or link)
            pub_date = (item_el.findtext("pubDate") or "")
            alert_level = (item_el.findtext(f"{gdacs_ns}alertlevel") or "").strip()
            event_type = (item_el.findtext(f"{gdacs_ns}eventtype") or "").strip()
            country_text = (item_el.findtext(f"{gdacs_ns}country") or "").strip()
            point = (item_el.findtext(f"{geo_ns}point") or "").strip()
            population = (item_el.findtext(f"{gdacs_ns}population") or "").strip()

            lat, lon = None, None
            if point and " " in point:
                parts = point.split()
                lat = _safe_float(parts[0])
                lon = _safe_float(parts[1])

            severity_map = {"Red": "HIGH", "Orange": "MEDIUM", "Green": "LOW"}
            severity = severity_map.get(alert_level, "")

            items.append(IntelItem(
                id=f"gdacs:{guid}",
                source="gdacs",
                domain="environment",
                title=title[:300],
                url=link,
                lat=lat,
                lon=lon,
                country=country_text,
                ts=pub_date,
                severity=severity,
                tags=["disaster", event_type.lower()],
                extra={
                    "event_type": event_type,
                    "alert_level": alert_level,
                    "population": population,
                },
            ))
        return items


# ─── NASA EONET ──────────────────────────────────────────────────────

EONET_API = "https://eonet.gsfc.nasa.gov/api/v3/events"


class NASAEONETExtractor(BaseExtractor):
    """Fetches natural events from NASA EONET.

    Post-collection structure per item:
        id="eonet:{event_id}", source="eonet", domain="environment",
        title, lat/lon (from first geometry), tags=[category],
        extra={categories, sources}
    """

    SOURCE = "eonet"
    DOMAIN = "environment"
    REDIS_KEY = "environment:eonet:v1"
    TTL_SECONDS = 14400  # 4h

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        params = {"status": "open", "limit": "50"}
        async with session.get(EONET_API, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)

        items: list[IntelItem] = []
        for ev in data.get("events", []):
            categories = [c.get("title", "") for c in ev.get("categories", [])]
            sources = [s.get("url", "") for s in ev.get("sources", [])]
            geometries = ev.get("geometry", [])
            lat, lon = None, None
            if geometries:
                coords = geometries[-1].get("coordinates", [])
                if len(coords) >= 2:
                    lon, lat = coords[0], coords[1]
            items.append(IntelItem(
                id=f"eonet:{ev.get('id', '')}",
                source="eonet",
                domain="environment",
                title=ev.get("title", ""),
                lat=lat,
                lon=lon,
                ts=geometries[-1].get("date", "") if geometries else "",
                tags=[c.lower() for c in categories],
                extra={"categories": categories, "sources": sources},
            ))
        return items


def _safe_float(val: object) -> float | None:
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
