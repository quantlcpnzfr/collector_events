from __future__ import annotations

import logging
from typing import Any

from collector_events.extractors.huggingface.base_provider import BaseHuggingFaceDatasetProvider


class HuggingFaceSentimentExtractor(BaseHuggingFaceDatasetProvider):
    """
    Provider for source 2:
    zeroshot/twitter-financial-news-sentiment
    """

    DEFAULT_DATASET = "zeroshot/twitter-financial-news-sentiment"
    DEFAULT_CONFIG = "default"
    DEFAULT_SPLIT = "train"

    LABEL_MAP = {
        0: "Bearish",
        1: "Bullish",
        2: "Neutral",
    }

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
        return "huggingface_sentiment"

    def normalize_row(self, item: dict[str, Any]) -> dict[str, Any]:
        row = item.get("row", {})
        label = row.get("label")

        return {
            "row_idx": item.get("row_idx"),
            "text": row.get("text"),
            "label": label,
            "sentiment": self.LABEL_MAP.get(label, "Unknown"),
        }


if __name__ == "__main__":
    from collector_events.extractors.factory import ExtractorProviderFactory

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    extractor = ExtractorProviderFactory.create("source_2")

    print("Available splits:")
    for split_info in extractor.get_splits():
        print(
            f"- dataset={split_info.dataset} config={split_info.config} "
            f"split={split_info.split} rows={split_info.num_rows}"
        )

    print("\nFirst 5 training rows:")
    preview_df = extractor.fetch_dataframe(max_rows=5)
    print(preview_df.to_string(index=False))
