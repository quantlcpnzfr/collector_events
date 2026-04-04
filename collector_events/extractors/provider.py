from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class DatasetSplitInfo:
    dataset: str
    config: str
    split: str
    num_rows: int | None = None


class ExtractorProvider(ABC):
    """Common contract for dataset extractors regardless of upstream source."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Human-readable provider/source identifier."""

    @property
    @abstractmethod
    def dataset(self) -> str:
        """Upstream dataset identifier."""

    @abstractmethod
    def get_splits(self) -> list[DatasetSplitInfo]:
        """Return dataset split metadata."""

    @abstractmethod
    def get_size(self) -> dict[str, Any]:
        """Return raw size metadata from the upstream provider."""

    @abstractmethod
    def get_first_rows(
        self,
        config: str | None = None,
        split: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return the first rows in a normalized shape."""

    @abstractmethod
    def get_rows(
        self,
        offset: int = 0,
        length: int = 100,
        config: str | None = None,
        split: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return a normalized slice of rows."""

    @abstractmethod
    def fetch_dataframe(
        self,
        split: str | None = None,
        config: str | None = None,
        max_rows: int | None = 500,
        batch_size: int = 100,
    ) -> pd.DataFrame:
        """Download normalized rows into a pandas DataFrame."""
