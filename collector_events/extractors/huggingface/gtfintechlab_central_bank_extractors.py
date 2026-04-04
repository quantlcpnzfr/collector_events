from __future__ import annotations

from typing import Any

from collector_events.extractors.huggingface.base_provider import BaseHuggingFaceDatasetProvider


class BaseGtfintechlabCentralBankExtractor(BaseHuggingFaceDatasetProvider):
    """
    Shared normalizer for gtfintechlab central bank sentence-classification datasets.
    """

    def normalize_row(self, item: dict[str, Any]) -> dict[str, Any]:
        row = self.flatten_row(item)

        return {
            **row,
            "dataset": self.dataset,
            "text": row.get("sentences") or row.get("sentence") or row.get("text"),
            "stance_label": row.get("stance_label"),
            "time_label": row.get("time_label"),
            "certain_label": row.get("certain_label"),
            "year": row.get("year") or row.get("Year"),
        }


class HuggingFaceEuropeanCentralBankExtractor(BaseGtfintechlabCentralBankExtractor):
    DEFAULT_DATASET = "gtfintechlab/european_central_bank"
    DEFAULT_CONFIG = "5768"
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
        return "huggingface_european_central_bank"


class HuggingFaceCentralBankOfBrazilExtractor(BaseGtfintechlabCentralBankExtractor):
    DEFAULT_DATASET = "gtfintechlab/central_bank_of_brazil"
    DEFAULT_CONFIG = "5768"
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
        return "huggingface_central_bank_of_brazil"
