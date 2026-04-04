from __future__ import annotations

from collector_events.extractors.huggingface.base_provider import FlatHuggingFaceDatasetProvider


class HuggingFaceYahooFinanceExtractor(FlatHuggingFaceDatasetProvider):
    """
    Provider for:
    defeatbeta/yahoo-finance-data

    This dataset appears to aggregate several finance-related record types, so the
    normalizer intentionally preserves the source columns instead of forcing a narrow schema.
    """

    DEFAULT_DATASET = "defeatbeta/yahoo-finance-data"
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
        return "huggingface_yahoo_finance"
