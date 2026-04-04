from __future__ import annotations

from typing import Any

from collector_events.extractors.huggingface.base_provider import BaseHuggingFaceDatasetProvider


class HuggingFaceTwitterFinancialNewsTopicExtractor(BaseHuggingFaceDatasetProvider):
    """
    Provider for:
    zeroshot/twitter-financial-news-topic
    """

    DEFAULT_DATASET = "zeroshot/twitter-financial-news-topic"
    DEFAULT_CONFIG = "default"
    DEFAULT_SPLIT = "train"

    TOPIC_MAP = {
        0: "Analyst Update",
        1: "Fed | Central Banks",
        2: "Company | Product News",
        3: "Treasuries | Corporate Debt",
        4: "Dividend",
        5: "Earnings",
        6: "Energy | Oil",
        7: "Financials",
        8: "Currencies",
        9: "General News | Opinion",
        10: "Gold | Metals | Materials",
        11: "IPO",
        12: "Legal | Regulation",
        13: "M&A | Investments",
        14: "Macro",
        15: "Markets",
        16: "Politics",
        17: "Personnel Change",
        18: "Stock Commentary",
        19: "Stock Movement",
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
        return "huggingface_financial_news_topic"

    def normalize_row(self, item: dict[str, Any]) -> dict[str, Any]:
        row = item.get("row", {})
        label = row.get("label")

        return {
            "row_idx": item.get("row_idx"),
            "dataset": self.dataset,
            "text": row.get("text"),
            "label": label,
            "topic": self.TOPIC_MAP.get(label, "Unknown"),
        }
