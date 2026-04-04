from __future__ import annotations

from typing import Any

from collector_events.extractors.huggingface.base_provider import BaseHuggingFaceDatasetProvider


class HuggingFaceCentralBankCommunicationsExtractor(BaseHuggingFaceDatasetProvider):
    """
    Provider for:
    aufklarer/central-bank-communications

    The dataset contains sentence- and document-level central bank communications,
    so normalization exposes monetary-policy specific metadata while keeping the
    original row payload intact for downstream analysis.
    """

    DEFAULT_DATASET = "aufklarer/central-bank-communications"
    DEFAULT_CONFIG = "sentences"
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
        return "huggingface_central_bank_communications"

    def normalize_row(self, item: dict[str, Any]) -> dict[str, Any]:
        row = self.flatten_row(item)

        return {
            **row,
            "dataset": self.dataset,
            "central_bank": row.get("central_bank") or row.get("bank"),
            "document_type": row.get("document_type") or row.get("type"),
            "event_date": row.get("date") or row.get("document_date"),
            "text": row.get("sentence") or row.get("text"),
            "text_original": row.get("sentence_original") or row.get("text_original"),
            "language": row.get("language"),
            "sentiment": row.get("sentiment"),
            "topic": row.get("topic"),
            "version": row.get("version"),
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
            central_bank = record.get("central_bank")
            document_type = record.get("document_type")
            text = record.get("text") or record.get("text_original")
            title_parts = [part for part in (central_bank, document_type) if part]
            title = " - ".join(title_parts) if title_parts else "Central Bank Communication"

            events.append(
                {
                    "event_id": self.build_event_id(
                        "central_bank_communication",
                        central_bank,
                        document_type,
                        record.get("event_date"),
                        record.get("row_idx"),
                    ),
                    "provider": self.source_name,
                    "source": self.dataset,
                    "event_type": "central_bank_communication",
                    "category": "monetary_policy",
                    "title": title,
                    "text": text,
                    "published_at": record.get("event_date"),
                    "currencies": self.infer_currencies_from_central_bank(central_bank),
                    "impact": None,
                    "metadata": {
                        "document_type": document_type,
                        "sentiment": record.get("sentiment"),
                        "topic": record.get("topic"),
                        "version": record.get("version"),
                        "language": record.get("language"),
                        "row_idx": record.get("row_idx"),
                    },
                }
            )

        return events
