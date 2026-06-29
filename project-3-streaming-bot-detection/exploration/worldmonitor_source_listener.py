"""Print sample live signals from public sources used by World Monitor.

This deliberately consumes the original public provider APIs, not World Monitor's
private relay. It is a small discovery tool and a future source-adapter seam.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from dotenv import load_dotenv

USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson"
NASA_EONET_URL = "https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=20"
USER_AGENT = "project-3-streaming-bot-detection/0.1 (local data discovery)"


@dataclass(frozen=True)
class Signal:
    """Small common envelope for a provider-specific raw event."""

    id: str
    source: str
    observed_at: str
    title: str
    url: str | None
    latitude: float | None
    longitude: float | None
    details: dict[str, Any]


def fetch_json(url: str) -> dict[str, Any]:
    """Fetch JSON from a documented public API with a finite timeout."""
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urlopen(request, timeout=20) as response:  # noqa: S310 -- fixed HTTPS URLs above
        return json.load(response)


def iso_from_millis(timestamp: int | None) -> str:
    if timestamp is None:
        return "unknown"
    return datetime.fromtimestamp(timestamp / 1000, tz=UTC).isoformat()


def parse_usgs(payload: dict[str, Any]) -> list[Signal]:
    signals: list[Signal] = []
    for feature in payload.get("features", []):
        properties = feature.get("properties", {})
        coordinates = feature.get("geometry", {}).get("coordinates", [])
        signals.append(
            Signal(
                id=f"usgs:{feature['id']}",
                source="usgs_earthquakes",
                observed_at=iso_from_millis(properties.get("time")),
                title=properties.get("title", "Untitled earthquake"),
                url=properties.get("url"),
                latitude=coordinates[1] if len(coordinates) >= 2 else None,
                longitude=coordinates[0] if len(coordinates) >= 2 else None,
                details={
                    "magnitude": properties.get("mag"),
                    "place": properties.get("place"),
                    "tsunami": properties.get("tsunami"),
                    "alert": properties.get("alert"),
                },
            )
        )
    return signals


def parse_nasa_eonet(payload: dict[str, Any]) -> list[Signal]:
    signals: list[Signal] = []
    for event in payload.get("events", []):
        geometry = (event.get("geometry") or [{}])[-1]
        coordinates = geometry.get("coordinates", [])
        categories = [category.get("title") for category in event.get("categories", [])]
        signals.append(
            Signal(
                id=f"nasa_eonet:{event['id']}",
                source="nasa_eonet",
                observed_at=geometry.get("date", "unknown"),
                title=event.get("title", "Untitled natural event"),
                url=event.get("link"),
                latitude=coordinates[1] if geometry.get("type") == "Point" and len(coordinates) >= 2 else None,
                longitude=coordinates[0] if geometry.get("type") == "Point" and len(coordinates) >= 2 else None,
                details={"categories": categories, "closed": event.get("closed")},
            )
        )
    return signals


def fetch_signals(enabled_sources: set[str]) -> list[Signal]:
    """Collect current records from the enabled source adapters."""
    signals: list[Signal] = []
    if "usgs" in enabled_sources:
        signals.extend(parse_usgs(fetch_json(USGS_URL)))
    if "nasa_eonet" in enabled_sources:
        signals.extend(parse_nasa_eonet(fetch_json(NASA_EONET_URL)))
    return signals


def print_signal(signal: Signal) -> None:
    print(
        json.dumps(
            {
                "id": signal.id,
                "source": signal.source,
                "observed_at": signal.observed_at,
                "title": signal.title,
                "url": signal.url,
                "location": {"latitude": signal.latitude, "longitude": signal.longitude},
                "details": signal.details,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


def main() -> None:
    load_dotenv()
    poll_seconds = int(os.getenv("WORLD_MONITOR_POLL_SECONDS", "60"))
    enabled_sources = {
        source.strip().lower()
        for source in os.getenv("WORLD_MONITOR_SOURCES", "usgs,nasa_eonet").split(",")
        if source.strip()
    }
    unknown_sources = enabled_sources - {"usgs", "nasa_eonet"}
    if unknown_sources or not enabled_sources:
        raise ValueError("WORLD_MONITOR_SOURCES supports: usgs,nasa_eonet")
    if poll_seconds < 15:
        raise ValueError("WORLD_MONITOR_POLL_SECONDS must be at least 15.")

    seen_ids: set[str] = set()
    print(
        f"Polling public source adapters ({', '.join(sorted(enabled_sources))}) every {poll_seconds}s.",
        flush=True,
    )
    print("The first poll prints the current snapshot; later polls print only new IDs. Ctrl+C stops.", flush=True)

    while True:
        try:
            signals = fetch_signals(enabled_sources)
            for signal in sorted(signals, key=lambda item: item.observed_at):
                if signal.id not in seen_ids:
                    print_signal(signal)
            seen_ids.update(signal.id for signal in signals)
            if len(seen_ids) > 2_000:
                seen_ids = {signal.id for signal in signals}
        except (URLError, TimeoutError, json.JSONDecodeError) as error:
            print(f"Source poll failed: {error}", flush=True)

        time.sleep(poll_seconds)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, ValueError) as error:
        print(f"Listener stopped: {error}", flush=True)
