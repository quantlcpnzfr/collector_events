from __future__ import annotations

from typing import Any

from collector_events.extractors.huggingface.base_provider import BaseHuggingFaceDatasetProvider


class HuggingFaceBisCentralBankSpeechesExtractor(BaseHuggingFaceDatasetProvider):
    """
    Provider for:
    samchain/bis_central_bank_speeches

    The dataset stores long-form central bank speeches plus metadata such as bank,
    date and description.
    """

    DEFAULT_DATASET = "samchain/bis_central_bank_speeches"
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
        return "huggingface_bis_central_bank_speeches"

    def normalize_row(self, item: dict[str, Any]) -> dict[str, Any]:
        row = self.flatten_row(item)

        return {
            **row,
            "dataset": self.dataset,
            "central_bank": row.get("bank") or row.get("central_bank"),
            "description": row.get("description"),
            "speech_text": row.get("text") or row.get("speech_text"),
            "year": row.get("Year") or row.get("year"),
            "month": row.get("Month") or row.get("month"),
            "event_date": row.get("date"),
        }
