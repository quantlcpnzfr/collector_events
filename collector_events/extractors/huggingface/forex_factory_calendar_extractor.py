from __future__ import annotations

from typing import Any

from collector_events.extractors.huggingface.base_provider import BaseHuggingFaceDatasetProvider


class HuggingFaceForexFactoryCalendarExtractor(BaseHuggingFaceDatasetProvider):
    """
    Provider for:
    Ehsanrs2/Forex_Factory_Calendar

    The dataset exposes structured macroeconomic event records, so we normalize
    the fields into an event-oriented shape that fits collector pipelines.
    """

    DEFAULT_DATASET = "Ehsanrs2/Forex_Factory_Calendar"
    DEFAULT_CONFIG = "default"
    DEFAULT_SPLIT = "train"

    def __init__(
        self,
        dataset: str = DEFAULT_DATASET,
        config: str = DEFAULT_CONFIG,
        split: str = DEFAULT_SPLIT,
        token: str | None = None,
        timeout: int = 30,
    ) -> None:
        super().__init__(
            dataset=dataset,
            config=config,
            split=split,
            token=token,
            timeout=timeout,
        )

    @property
    def source_name(self) -> str:
        return "huggingface_forex_factory_calendar"

    def normalize_row(self, item: dict[str, Any]) -> dict[str, Any]:
        row = item.get("row", {})

        return {
            "row_idx": item.get("row_idx"),
            "dataset": self.dataset,
            "event_time": row.get("DateTime") or row.get("datetime"),
            "currency": row.get("Currency") or row.get("currency"),
            "impact": row.get("Impact") or row.get("impact"),
            "event": row.get("Event") or row.get("event"),
            "actual": row.get("Actual") or row.get("actual"),
            "forecast": row.get("Forecast") or row.get("forecast"),
            "previous": row.get("Previous") or row.get("previous"),
            "detail": row.get("Detail") or row.get("detail"),
        }

    def fetch_events(
        self,
        split: str | None = None,
        config: str | None = None,
        max_rows: int | None = 500,
        batch_size: int = 100,
    ) -> list[dict[str, Any]]:
        records = self.fetch_records(
            split=split,
            config=config,
            max_rows=max_rows,
            batch_size=batch_size,
        )

        events: list[dict[str, Any]] = []
        for record in records:
            currency = record.get("currency")
            title_parts = [part for part in (currency, record.get("event")) if part]
            title = " - ".join(title_parts) if title_parts else "Economic Calendar Event"
            text = record.get("detail") or record.get("event") or title

            events.append(
                {
                    "event_id": self.build_event_id(
                        "economic_calendar",
                        currency,
                        record.get("event_time"),
                        record.get("event"),
                        record.get("row_idx"),
                    ),
                    "provider": self.source_name,
                    "source": self.dataset,
                    "event_type": "economic_calendar",
                    "category": "economic_data",
                    "title": title,
                    "text": text,
                    "published_at": record.get("event_time"),
                    "currencies": [currency] if currency else [],
                    "impact": record.get("impact"),
                    "metadata": {
                        "actual": record.get("actual"),
                        "forecast": record.get("forecast"),
                        "previous": record.get("previous"),
                        "row_idx": record.get("row_idx"),
                    },
                }
            )

        return events
