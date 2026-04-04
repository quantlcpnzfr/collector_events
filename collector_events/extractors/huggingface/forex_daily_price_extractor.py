from __future__ import annotations

from collector_events.extractors.huggingface.base_provider import FlatHuggingFaceDatasetProvider


class HuggingFaceForexDailyPriceExtractor(FlatHuggingFaceDatasetProvider):
    """
    Provider for:
    paperswithbacktest/Forex-Daily-Price

    Note: this dataset is gated on Hugging Face. The provider supports optional
    tokens through the shared base class when authenticated access is needed.
    """

    DEFAULT_DATASET = "paperswithbacktest/Forex-Daily-Price"
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
        return "huggingface_forex_daily_price"
