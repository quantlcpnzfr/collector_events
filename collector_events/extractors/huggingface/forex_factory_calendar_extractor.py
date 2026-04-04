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
