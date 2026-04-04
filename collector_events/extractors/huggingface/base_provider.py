from __future__ import annotations

import json
import logging
import os
from abc import abstractmethod
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from collector_events.extractors.provider import DatasetSplitInfo, ExtractorProvider


logger = logging.getLogger(__name__)


class HuggingFaceDatasetError(RuntimeError):
    """Raised when the Hugging Face dataset API cannot satisfy a request."""


class BaseHuggingFaceDatasetProvider(ExtractorProvider):
    """
    Shared Hugging Face dataset-viewer client.

    Concrete subclasses only need to define dataset metadata and row normalization.
    """

    BASE_URL = "https://datasets-server.huggingface.co"

    def __init__(
        self,
        dataset: str,
        config: str = "default",
        split: str = "train",
        token: str | None = None,
        timeout: int = 30,
    ) -> None:
        self._dataset = dataset
        self.config = config
        self.split = split
        self.token = token if token is not None else os.getenv("HF_TOKEN")
        self.timeout = timeout

    @property
    def dataset(self) -> str:
        return self._dataset

    def _build_url(self, endpoint: str, **params: Any) -> str:
        filtered_params = {key: value for key, value in params.items() if value is not None}
        query_string = urlencode(filtered_params)
        return f"{self.BASE_URL}/{endpoint}?{query_string}"

    def _get_json(self, endpoint: str, **params: Any) -> dict[str, Any]:
        url = self._build_url(endpoint, **params)
        headers = {"User-Agent": "forex-system-hf-extractor/1.0"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        request = Request(url=url, headers=headers, method="GET")

        try:
            with urlopen(request, timeout=self.timeout) as response:
                payload = response.read().decode("utf-8")
                return json.loads(payload)
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise HuggingFaceDatasetError(
                f"Hugging Face API returned HTTP {exc.code} for {url}. Details: {detail}"
            ) from exc
        except URLError as exc:
            raise HuggingFaceDatasetError(
                f"Could not reach Hugging Face API at {url}. Reason: {exc.reason}"
            ) from exc
        except json.JSONDecodeError as exc:
            raise HuggingFaceDatasetError(f"Invalid JSON returned by Hugging Face API: {url}") from exc

    def get_splits(self) -> list[DatasetSplitInfo]:
        payload = self._get_json("splits", dataset=self.dataset)
        splits = payload.get("splits", [])
        results: list[DatasetSplitInfo] = []

        for item in splits:
            results.append(
                DatasetSplitInfo(
                    dataset=item.get("dataset", self.dataset),
                    config=item.get("config", self.config),
                    split=item.get("split", self.split),
                    num_rows=item.get("num_rows"),
                )
            )
        return results

    def get_size(self) -> dict[str, Any]:
        return self._get_json("size", dataset=self.dataset)

    def get_first_rows(
        self,
        config: str | None = None,
        split: str | None = None,
    ) -> list[dict[str, Any]]:
        payload = self._get_json(
            "first-rows",
            dataset=self.dataset,
            config=config or self.config,
            split=split or self.split,
        )
        return self._normalize_rows(payload.get("rows", []))

    def get_rows(
        self,
        offset: int = 0,
        length: int = 100,
        config: str | None = None,
        split: str | None = None,
    ) -> list[dict[str, Any]]:
        safe_length = min(max(length, 1), 100)
        payload = self._get_json(
            "rows",
            dataset=self.dataset,
            config=config or self.config,
            split=split or self.split,
            offset=offset,
            length=safe_length,
        )
        return self._normalize_rows(payload.get("rows", []))

    def fetch_dataframe(
        self,
        split: str | None = None,
        config: str | None = None,
        max_rows: int | None = 500,
        batch_size: int = 100,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            self.fetch_records(
                split=split,
                config=config,
                max_rows=max_rows,
                batch_size=batch_size,
            )
        )

    def fetch_records(
        self,
        split: str | None = None,
        config: str | None = None,
        max_rows: int | None = 500,
        batch_size: int = 100,
    ) -> list[dict[str, Any]]:
        active_split = split or self.split
        active_config = config or self.config
        rows: list[dict[str, Any]] = []
        offset = 0

        total_rows = self._get_split_row_count(config=active_config, split=active_split)
        target_rows = total_rows if max_rows is None else min(max_rows, total_rows or max_rows)

        while True:
            if target_rows is not None and offset >= target_rows:
                break

            current_batch_size = batch_size
            if target_rows is not None:
                current_batch_size = min(batch_size, target_rows - offset)

            batch = self.get_rows(
                offset=offset,
                length=current_batch_size,
                config=active_config,
                split=active_split,
            )
            if not batch:
                break

            rows.extend(batch)
            offset += len(batch)

            logger.info(
                "Fetched %s rows from dataset=%s config=%s split=%s",
                offset,
                self.dataset,
                active_config,
                active_split,
            )

            if len(batch) < current_batch_size:
                break

        return rows

    def _get_split_row_count(self, config: str, split: str) -> int | None:
        size_payload = self.get_size()
        split_entries = size_payload.get("size", {}).get("splits", [])

        for item in split_entries:
            if item.get("config") == config and item.get("split") == split:
                return item.get("num_rows")
        return None

    def _normalize_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []

        for item in rows:
            normalized.append(self.normalize_row(item))

        return normalized

    @abstractmethod
    def normalize_row(self, item: dict[str, Any]) -> dict[str, Any]:
        """Convert a raw Hugging Face row payload to the provider's normalized shape."""

    @staticmethod
    def flatten_row(item: dict[str, Any]) -> dict[str, Any]:
        """
        Generic fallback for tabular/text datasets with variable schemas.

        It keeps every original column and prefixes metadata consistently.
        """

        row = item.get("row", {})
        normalized = {"row_idx": item.get("row_idx")}
        normalized.update(row)
        return normalized

    @staticmethod
    def build_event_id(prefix: str, *parts: Any) -> str:
        cleaned_parts = [str(part).strip().replace(" ", "_") for part in parts if part not in (None, "")]
        suffix = "::".join(cleaned_parts)
        return f"{prefix}::{suffix}" if suffix else prefix

    @staticmethod
    def infer_currencies_from_central_bank(central_bank: str | None) -> list[str]:
        if not central_bank:
            return []

        normalized_name = central_bank.lower()
        bank_currency_map = {
            "federal reserve": "USD",
            "federal open market committee": "USD",
            "ecb": "EUR",
            "european central bank": "EUR",
            "bank of england": "GBP",
            "bank of japan": "JPY",
            "bank of canada": "CAD",
            "reserve bank of australia": "AUD",
            "reserve bank of new zealand": "NZD",
            "swiss national bank": "CHF",
            "norges bank": "NOK",
            "riksbank": "SEK",
            "central bank of brazil": "BRL",
            "banco central do brasil": "BRL",
            "people's bank of china": "CNY",
            "bank of korea": "KRW",
            "central bank of chile": "CLP",
            "central reserve bank of peru": "PEN",
            "central bank of the republic of turkey": "TRY",
            "central bank of china (taiwan)": "TWD",
            "bangko sentral ng pilipinas": "PHP",
            "central bank of the philippines": "PHP",
            "reserve bank of india": "INR",
            "south african reserve bank": "ZAR",
        }

        for bank_fragment, currency in bank_currency_map.items():
            if bank_fragment in normalized_name:
                return [currency]
        return []


class FlatHuggingFaceDatasetProvider(BaseHuggingFaceDatasetProvider):
    """
    Reusable provider for datasets whose row schema can be preserved as-is.

    This is useful for broad tabular or document datasets where we want to keep
    upstream fields intact and add only a minimal metadata envelope.
    """

    def normalize_row(self, item: dict[str, Any]) -> dict[str, Any]:
        normalized = self.flatten_row(item)
        normalized.setdefault("dataset", self.dataset)
        return normalized
