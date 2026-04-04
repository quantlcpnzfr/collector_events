from __future__ import annotations

from typing import Any

from collector_events.extractors.provider import ExtractorProvider
from collector_events.extractors.huggingface.bloomberg_financial_news_extractor import (
    HuggingFaceBloombergFinancialNewsExtractor,
)
from collector_events.extractors.huggingface.finance_tasks_extractor import (
    HuggingFaceFinanceTasksExtractor,
)
from collector_events.extractors.huggingface.forex_daily_price_extractor import (
    HuggingFaceForexDailyPriceExtractor,
)
from collector_events.extractors.huggingface.forex_factory_calendar_extractor import (
    HuggingFaceForexFactoryCalendarExtractor,
)
from collector_events.extractors.huggingface.forex_usdjpy_extractor import (
    HuggingFaceForexUsdJpyExtractor,
)
from collector_events.extractors.huggingface.ohlcv_1m_forex_extractor import (
    HuggingFaceOhlcv1mForexExtractor,
)
from collector_events.extractors.huggingface.sentiment_extractor import (
    HuggingFaceSentimentExtractor,
)
from collector_events.extractors.huggingface.stock_market_tweets_extractor import (
    HuggingFaceStockMarketTweetsExtractor,
)
from collector_events.extractors.huggingface.twitter_financial_news_topic_extractor import (
    HuggingFaceTwitterFinancialNewsTopicExtractor,
)
from collector_events.extractors.huggingface.yahoo_finance_extractor import (
    HuggingFaceYahooFinanceExtractor,
)


class ExtractorProviderFactory:
    """
    Factory/registry for extractor providers.

    This keeps caller code decoupled from concrete implementation classes.
    """

    _PROVIDER_CLASSES: dict[str, type[ExtractorProvider]] = {
        "huggingface_forex_factory_calendar": HuggingFaceForexFactoryCalendarExtractor,
        "huggingface_sentiment": HuggingFaceSentimentExtractor,
        "huggingface_stock_market_tweets": HuggingFaceStockMarketTweetsExtractor,
        "huggingface_ohlcv_1m_forex": HuggingFaceOhlcv1mForexExtractor,
        "huggingface_forex_usdjpy": HuggingFaceForexUsdJpyExtractor,
        "huggingface_forex_daily_price": HuggingFaceForexDailyPriceExtractor,
        "huggingface_financial_news_topic": HuggingFaceTwitterFinancialNewsTopicExtractor,
        "huggingface_bloomberg_financial_news": HuggingFaceBloombergFinancialNewsExtractor,
        "huggingface_yahoo_finance": HuggingFaceYahooFinanceExtractor,
        "huggingface_finance_tasks": HuggingFaceFinanceTasksExtractor,
    }

    _ALIASES: dict[str, str] = {
        "forex_factory_calendar": "huggingface_forex_factory_calendar",
        "Ehsanrs2/Forex_Factory_Calendar": "huggingface_forex_factory_calendar",
        "source_2": "huggingface_sentiment",
        "zeroshot/twitter-financial-news-sentiment": "huggingface_sentiment",
        "stock_market_tweets": "huggingface_stock_market_tweets",
        "source_4": "huggingface_stock_market_tweets",
        "StephanAkkerman/stock-market-tweets-data": "huggingface_stock_market_tweets",
        "ohlcv_1m_forex": "huggingface_ohlcv_1m_forex",
        "mito0o852/OHLCV-1m-Forex": "huggingface_ohlcv_1m_forex",
        "forex_usdjpy": "huggingface_forex_usdjpy",
        "huggingXG/forex_USDJPY": "huggingface_forex_usdjpy",
        "forex_daily_price": "huggingface_forex_daily_price",
        "paperswithbacktest/Forex-Daily-Price": "huggingface_forex_daily_price",
        "financial_news_topic": "huggingface_financial_news_topic",
        "zeroshot/twitter-financial-news-topic": "huggingface_financial_news_topic",
        "bloomberg_financial_news": "huggingface_bloomberg_financial_news",
        "danidanou/Bloomberg_Financial_News": "huggingface_bloomberg_financial_news",
        "yahoo_finance": "huggingface_yahoo_finance",
        "defeatbeta/yahoo-finance-data": "huggingface_yahoo_finance",
        "finance_tasks": "huggingface_finance_tasks",
        "AdaptLLM/finance-tasks": "huggingface_finance_tasks",
    }

    @classmethod
    def resolve_provider_name(cls, provider_name: str) -> str:
        if provider_name in cls._PROVIDER_CLASSES:
            return provider_name

        try:
            return cls._ALIASES[provider_name]
        except KeyError as exc:
            available = ", ".join(sorted(cls.available_providers(include_aliases=True)))
            raise ValueError(
                f"Unknown extractor provider '{provider_name}'. Available providers: {available}"
            ) from exc

    @classmethod
    def create(cls, provider_name: str, **kwargs: Any) -> ExtractorProvider:
        canonical_name = cls.resolve_provider_name(provider_name)
        provider_class = cls._PROVIDER_CLASSES[canonical_name]
        return provider_class(**kwargs)

    @classmethod
    def available_providers(cls, include_aliases: bool = False) -> list[str]:
        if include_aliases:
            return sorted({*cls._PROVIDER_CLASSES, *cls._ALIASES})
        return sorted(cls._PROVIDER_CLASSES)
