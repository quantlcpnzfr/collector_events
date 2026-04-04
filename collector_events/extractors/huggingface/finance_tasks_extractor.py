from __future__ import annotations

from collector_events.extractors.huggingface.base_provider import FlatHuggingFaceDatasetProvider


class HuggingFaceFinanceTasksExtractor(FlatHuggingFaceDatasetProvider):
    """
    Provider for:
    AdaptLLM/finance-tasks

    This dataset is benchmark/task-oriented rather than a live event feed, so the
    extractor preserves the original task fields and adds a consistent dataset marker.
    """

    DEFAULT_DATASET = "AdaptLLM/finance-tasks"
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
        return "huggingface_finance_tasks"
