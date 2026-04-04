from __future__ import annotations

from collector_events.extractors.huggingface.base_provider import FlatHuggingFaceDatasetProvider


class HuggingFaceOhlcv1mForexExtractor(FlatHuggingFaceDatasetProvider):
    """
    Provider for:
    mito0o852/OHLCV-1m-Forex

    This dataset is market-data oriented and benefits from preserving the raw
    upstream columns for downstream feature engineering.
    """

    DEFAULT_DATASET = "mito0o852/OHLCV-1m-Forex"
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
        return "huggingface_ohlcv_1m_forex"
