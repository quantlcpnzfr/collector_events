from __future__ import annotations

from collector_events.extractors.huggingface.base_provider import (
    BaseHuggingFaceDatasetProvider,
    FlatHuggingFaceDatasetProvider,
    HuggingFaceDatasetError,
)
from collector_events.extractors.huggingface.bis_central_bank_speeches_extractor import (
    HuggingFaceBisCentralBankSpeechesExtractor,
)
from collector_events.extractors.huggingface.bloomberg_financial_news_extractor import (
    HuggingFaceBloombergFinancialNewsExtractor,
)
from collector_events.extractors.huggingface.central_bank_communications_extractor import (
    HuggingFaceCentralBankCommunicationsExtractor,
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
from collector_events.extractors.huggingface.gtfintechlab_central_bank_extractors import (
    HuggingFaceCentralBankOfBrazilExtractor,
    HuggingFaceEuropeanCentralBankExtractor,
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

__all__ = [
    "BaseHuggingFaceDatasetProvider",
    "FlatHuggingFaceDatasetProvider",
    "HuggingFaceDatasetError",
    "HuggingFaceBisCentralBankSpeechesExtractor",
    "HuggingFaceBloombergFinancialNewsExtractor",
    "HuggingFaceCentralBankCommunicationsExtractor",
    "HuggingFaceCentralBankOfBrazilExtractor",
    "HuggingFaceEuropeanCentralBankExtractor",
    "HuggingFaceFinanceTasksExtractor",
    "HuggingFaceForexDailyPriceExtractor",
    "HuggingFaceForexFactoryCalendarExtractor",
    "HuggingFaceForexUsdJpyExtractor",
    "HuggingFaceOhlcv1mForexExtractor",
    "HuggingFaceSentimentExtractor",
    "HuggingFaceStockMarketTweetsExtractor",
    "HuggingFaceTwitterFinancialNewsTopicExtractor",
    "HuggingFaceYahooFinanceExtractor",
]
