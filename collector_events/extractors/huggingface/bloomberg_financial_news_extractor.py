from __future__ import annotations

from collector_events.extractors.huggingface.base_provider import FlatHuggingFaceDatasetProvider


class HuggingFaceBloombergFinancialNewsExtractor(FlatHuggingFaceDatasetProvider):
    """
    Provider for:
    danidanou/Bloomberg_Financial_News

    The dataset is a processed article corpus; keeping the source schema intact is
    the safest default for later summarization and classification stages.
    """

    DEFAULT_DATASET = "danidanou/Bloomberg_Financial_News"
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
        return "huggingface_bloomberg_financial_news"
