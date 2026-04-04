from __future__ import annotations

from typing import Any

from collector_events.extractors.huggingface.base_provider import BaseHuggingFaceDatasetProvider


class HuggingFaceStockMarketTweetsExtractor(BaseHuggingFaceDatasetProvider):
    """
    Provider for source 4:
    StephanAkkerman/stock-market-tweets-data
    """

    DEFAULT_DATASET = "StephanAkkerman/stock-market-tweets-data"
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
        return "huggingface_stock_market_tweets"

    def normalize_row(self, item: dict[str, Any]) -> dict[str, Any]:
        row = item.get("row", {})

        normalized = {
            "row_idx": item.get("row_idx"),
            "text": row.get("text"),
            "created_at": row.get("created_at"),
            "dataset": self.dataset,
        }

        for optional_field in ("username", "user", "symbol", "company", "source", "url"):
            if optional_field in row:
                normalized[optional_field] = row.get(optional_field)

        return normalized
