from __future__ import annotations

from typing import Any

from collector_events.extractors.huggingface.base_provider import BaseHuggingFaceDatasetProvider


class BaseGtfintechlabCentralBankExtractor(BaseHuggingFaceDatasetProvider):
    """
    Shared normalizer for gtfintechlab central bank sentence-classification datasets.
    """

    central_bank_name = "Central Bank"
    currency_code = ""

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
            title = f"{self.central_bank_name} policy sentence"
            events.append(
                {
                    "event_id": self.build_event_id(
                        "central_bank_sentence",
                        self.central_bank_name,
                        record.get("year"),
                        record.get("row_idx"),
                    ),
                    "provider": self.source_name,
                    "source": self.dataset,
                    "event_type": "central_bank_communication",
                    "category": "monetary_policy",
                    "title": title,
                    "text": record.get("text"),
                    "published_at": record.get("year"),
                    "currencies": [self.currency_code] if self.currency_code else [],
                    "impact": None,
                    "metadata": {
                        "central_bank": self.central_bank_name,
                        "stance_label": record.get("stance_label"),
                        "time_label": record.get("time_label"),
                        "certain_label": record.get("certain_label"),
                        "year": record.get("year"),
                        "row_idx": record.get("row_idx"),
                    },
                }
            )

        return events


class HuggingFaceEuropeanCentralBankExtractor(BaseGtfintechlabCentralBankExtractor):
    DEFAULT_DATASET = "gtfintechlab/european_central_bank"
    DEFAULT_CONFIG = "5768"
    DEFAULT_SPLIT = "train"
    central_bank_name = "European Central Bank"
    currency_code = "EUR"

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
    central_bank_name = "Central Bank of Brazil"
    currency_code = "BRL"

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
