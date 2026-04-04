from __future__ import annotations

from typing import Any

from collector_events.extractors.huggingface.base_provider import BaseHuggingFaceDatasetProvider


class HuggingFaceForexUsdJpyExtractor(BaseHuggingFaceDatasetProvider):
    """
    Provider for:
    huggingXG/forex_USDJPY

    The dataset exposes tick-level USDJPY quote data.
    """

    DEFAULT_DATASET = "huggingXG/forex_USDJPY"
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
        return "huggingface_forex_usdjpy"

    def normalize_row(self, item: dict[str, Any]) -> dict[str, Any]:
        row = item.get("row", {})

        return {
            "row_idx": item.get("row_idx"),
            "dataset": self.dataset,
            "pair": "USDJPY",
            "timestamp": row.get("timestamp"),
            "ask": row.get("ask"),
            "bid": row.get("bid"),
            "ask_volume": row.get("ask_volume"),
            "bid_volume": row.get("bid_volume"),
        }
