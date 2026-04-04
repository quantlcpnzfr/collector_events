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
            title = record.get("description") or (
                f"{central_bank} speech" if central_bank else "Central Bank Speech"
            )

            events.append(
                {
                    "event_id": self.build_event_id(
                        "central_bank_speech",
                        central_bank,
                        record.get("event_date"),
                        record.get("row_idx"),
                    ),
                    "provider": self.source_name,
                    "source": self.dataset,
                    "event_type": "central_bank_speech",
                    "category": "monetary_policy",
                    "title": title,
                    "text": record.get("speech_text"),
                    "published_at": record.get("event_date") or record.get("year"),
                    "currencies": self.infer_currencies_from_central_bank(central_bank),
                    "impact": None,
                    "metadata": {
                        "central_bank": central_bank,
                        "description": record.get("description"),
                        "year": record.get("year"),
                        "month": record.get("month"),
                        "row_idx": record.get("row_idx"),
                    },
                }
            )

        return events
