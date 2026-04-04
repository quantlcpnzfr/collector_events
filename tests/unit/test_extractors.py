from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from collector_events.extractors import create_extractor, resolve_extractor_name
from collector_events.extractors.factory import ExtractorProviderFactory
from collector_events.extractors.huggingface import (
    HuggingFaceSentimentExtractor,
)


def _assert_subset(testcase: unittest.TestCase, actual: dict, expected: dict) -> None:
    for key, value in expected.items():
        testcase.assertIn(key, actual)
        testcase.assertEqual(actual[key], value)


class ExtractorFactoryTests(unittest.TestCase):
    def test_resolve_provider_name_for_all_aliases(self) -> None:
        cases = {
            "huggingface_central_bank_communications": "huggingface_central_bank_communications",
            "aufklarer/central-bank-communications": "huggingface_central_bank_communications",
            "central_bank_communications": "huggingface_central_bank_communications",
            "huggingface_bis_central_bank_speeches": "huggingface_bis_central_bank_speeches",
            "samchain/bis_central_bank_speeches": "huggingface_bis_central_bank_speeches",
            "bis_central_bank_speeches": "huggingface_bis_central_bank_speeches",
            "huggingface_european_central_bank": "huggingface_european_central_bank",
            "gtfintechlab/european_central_bank": "huggingface_european_central_bank",
            "european_central_bank": "huggingface_european_central_bank",
            "huggingface_central_bank_of_brazil": "huggingface_central_bank_of_brazil",
            "gtfintechlab/central_bank_of_brazil": "huggingface_central_bank_of_brazil",
            "central_bank_of_brazil": "huggingface_central_bank_of_brazil",
            "huggingface_forex_factory_calendar": "huggingface_forex_factory_calendar",
            "Ehsanrs2/Forex_Factory_Calendar": "huggingface_forex_factory_calendar",
            "forex_factory_calendar": "huggingface_forex_factory_calendar",
            "huggingface_sentiment": "huggingface_sentiment",
            "zeroshot/twitter-financial-news-sentiment": "huggingface_sentiment",
            "source_2": "huggingface_sentiment",
            "huggingface_stock_market_tweets": "huggingface_stock_market_tweets",
            "StephanAkkerman/stock-market-tweets-data": "huggingface_stock_market_tweets",
            "source_4": "huggingface_stock_market_tweets",
            "huggingface_ohlcv_1m_forex": "huggingface_ohlcv_1m_forex",
            "mito0o852/OHLCV-1m-Forex": "huggingface_ohlcv_1m_forex",
            "ohlcv_1m_forex": "huggingface_ohlcv_1m_forex",
            "huggingface_forex_usdjpy": "huggingface_forex_usdjpy",
            "huggingXG/forex_USDJPY": "huggingface_forex_usdjpy",
            "forex_usdjpy": "huggingface_forex_usdjpy",
            "huggingface_forex_daily_price": "huggingface_forex_daily_price",
            "paperswithbacktest/Forex-Daily-Price": "huggingface_forex_daily_price",
            "forex_daily_price": "huggingface_forex_daily_price",
            "huggingface_financial_news_topic": "huggingface_financial_news_topic",
            "zeroshot/twitter-financial-news-topic": "huggingface_financial_news_topic",
            "financial_news_topic": "huggingface_financial_news_topic",
            "huggingface_bloomberg_financial_news": "huggingface_bloomberg_financial_news",
            "danidanou/Bloomberg_Financial_News": "huggingface_bloomberg_financial_news",
            "bloomberg_financial_news": "huggingface_bloomberg_financial_news",
            "huggingface_yahoo_finance": "huggingface_yahoo_finance",
            "defeatbeta/yahoo-finance-data": "huggingface_yahoo_finance",
            "yahoo_finance": "huggingface_yahoo_finance",
            "huggingface_finance_tasks": "huggingface_finance_tasks",
            "AdaptLLM/finance-tasks": "huggingface_finance_tasks",
            "finance_tasks": "huggingface_finance_tasks",
        }

        for requested_name, canonical_name in cases.items():
            with self.subTest(requested_name=requested_name):
                self.assertEqual(resolve_extractor_name(requested_name), canonical_name)
                provider = create_extractor(requested_name)
                self.assertEqual(provider.source_name, canonical_name)

    def test_available_providers_returns_only_canonical_names_by_default(self) -> None:
        providers = ExtractorProviderFactory.available_providers()
        self.assertIn("huggingface_forex_factory_calendar", providers)
        self.assertNotIn("Ehsanrs2/Forex_Factory_Calendar", providers)


class HuggingFaceTokenTests(unittest.TestCase):
    def test_hf_token_is_loaded_from_environment(self) -> None:
        with patch.dict(os.environ, {"HF_TOKEN": "env-token"}, clear=False):
            provider = HuggingFaceSentimentExtractor(token=None)
            self.assertEqual(provider.token, "env-token")

    def test_explicit_token_overrides_environment(self) -> None:
        with patch.dict(os.environ, {"HF_TOKEN": "env-token"}, clear=False):
            provider = HuggingFaceSentimentExtractor(token="explicit-token")
            self.assertEqual(provider.token, "explicit-token")


class ProviderNormalizationTests(unittest.TestCase):
    def test_normalize_row_for_all_providers(self) -> None:
        cases = [
            (
                "huggingface_central_bank_communications",
                {
                    "row_idx": 1,
                    "row": {
                        "central_bank": "Federal Reserve",
                        "document_type": "minutes",
                        "date": "2024-01-31",
                        "sentence": "Policy remains restrictive.",
                        "sentence_original": "Policy remains restrictive.",
                        "language": "en",
                        "sentiment": "hawkish",
                        "topic": "inflation",
                        "version": "v1",
                    },
                },
                {
                    "row_idx": 1,
                    "dataset": "aufklarer/central-bank-communications",
                    "central_bank": "Federal Reserve",
                    "document_type": "minutes",
                    "event_date": "2024-01-31",
                    "text": "Policy remains restrictive.",
                    "text_original": "Policy remains restrictive.",
                    "language": "en",
                    "sentiment": "hawkish",
                    "topic": "inflation",
                    "version": "v1",
                },
            ),
            (
                "huggingface_bis_central_bank_speeches",
                {
                    "row_idx": 2,
                    "row": {
                        "bank": "bank of greece",
                        "description": "Speech at Athens conference.",
                        "text": "Long-form central bank speech.",
                        "Year": 2003,
                        "Month": 2,
                    },
                },
                {
                    "row_idx": 2,
                    "dataset": "samchain/bis_central_bank_speeches",
                    "central_bank": "bank of greece",
                    "description": "Speech at Athens conference.",
                    "speech_text": "Long-form central bank speech.",
                    "year": 2003,
                    "month": 2,
                },
            ),
            (
                "huggingface_european_central_bank",
                {
                    "row_idx": 3,
                    "row": {
                        "sentences": "Inflation is expected to decline.",
                        "stance_label": "neutral",
                        "time_label": "forward looking",
                        "certain_label": "certain",
                        "year": 2015,
                    },
                },
                {
                    "row_idx": 3,
                    "dataset": "gtfintechlab/european_central_bank",
                    "text": "Inflation is expected to decline.",
                    "stance_label": "neutral",
                    "time_label": "forward looking",
                    "certain_label": "certain",
                    "year": 2015,
                },
            ),
            (
                "huggingface_central_bank_of_brazil",
                {
                    "row_idx": 4,
                    "row": {
                        "sentences": "Copom will maintain vigilance.",
                        "stance_label": "hawkish",
                        "time_label": "not forward looking",
                        "certain_label": "uncertain",
                        "year": 2008,
                    },
                },
                {
                    "row_idx": 4,
                    "dataset": "gtfintechlab/central_bank_of_brazil",
                    "text": "Copom will maintain vigilance.",
                    "stance_label": "hawkish",
                    "time_label": "not forward looking",
                    "certain_label": "uncertain",
                    "year": 2008,
                },
            ),
            (
                "huggingface_forex_factory_calendar",
                {
                    "row_idx": 5,
                    "row": {
                        "DateTime": "2025-04-07 08:30",
                        "Currency": "USD",
                        "Impact": "High",
                        "Event": "Non-Farm Payrolls",
                        "Actual": "250K",
                        "Forecast": "210K",
                        "Previous": "180K",
                        "Detail": "Strong labor market data.",
                    },
                },
                {
                    "row_idx": 5,
                    "dataset": "Ehsanrs2/Forex_Factory_Calendar",
                    "event_time": "2025-04-07 08:30",
                    "currency": "USD",
                    "impact": "High",
                    "event": "Non-Farm Payrolls",
                    "actual": "250K",
                    "forecast": "210K",
                    "previous": "180K",
                    "detail": "Strong labor market data.",
                },
            ),
            (
                "huggingface_sentiment",
                {"row_idx": 6, "row": {"text": "Fed pauses hikes", "label": 1}},
                {
                    "row_idx": 6,
                    "text": "Fed pauses hikes",
                    "label": 1,
                    "sentiment": "Bullish",
                },
            ),
            (
                "huggingface_stock_market_tweets",
                {
                    "row_idx": 7,
                    "row": {
                        "text": "Stocks rally on CPI surprise.",
                        "created_at": "2020-04-09 10:00:00",
                        "company": "SPX500",
                    },
                },
                {
                    "row_idx": 7,
                    "dataset": "StephanAkkerman/stock-market-tweets-data",
                    "text": "Stocks rally on CPI surprise.",
                    "created_at": "2020-04-09 10:00:00",
                    "company": "SPX500",
                },
            ),
            (
                "huggingface_ohlcv_1m_forex",
                {
                    "row_idx": 8,
                    "row": {
                        "timestamp": "2026-02-20 10:01:00",
                        "open": 1.0810,
                        "high": 1.0812,
                        "low": 1.0808,
                        "close": 1.0811,
                        "volume": 1450,
                    },
                },
                {
                    "row_idx": 8,
                    "dataset": "mito0o852/OHLCV-1m-Forex",
                    "timestamp": "2026-02-20 10:01:00",
                    "open": 1.0810,
                    "close": 1.0811,
                },
            ),
            (
                "huggingface_forex_usdjpy",
                {
                    "row_idx": 9,
                    "row": {
                        "timestamp": "2025-05-31 09:30:00.123",
                        "ask": 156.12,
                        "bid": 156.10,
                        "ask_volume": 100,
                        "bid_volume": 90,
                    },
                },
                {
                    "row_idx": 9,
                    "dataset": "huggingXG/forex_USDJPY",
                    "pair": "USDJPY",
                    "timestamp": "2025-05-31 09:30:00.123",
                    "ask": 156.12,
                    "bid": 156.10,
                    "ask_volume": 100,
                    "bid_volume": 90,
                },
            ),
            (
                "huggingface_forex_daily_price",
                {
                    "row_idx": 10,
                    "row": {
                        "instrument": "AUDUSD",
                        "date": "2026-03-07",
                        "close": 0.6591,
                    },
                },
                {
                    "row_idx": 10,
                    "dataset": "paperswithbacktest/Forex-Daily-Price",
                    "instrument": "AUDUSD",
                    "date": "2026-03-07",
                    "close": 0.6591,
                },
            ),
            (
                "huggingface_financial_news_topic",
                {"row_idx": 11, "row": {"text": "ECB signals pause", "label": 1}},
                {
                    "row_idx": 11,
                    "dataset": "zeroshot/twitter-financial-news-topic",
                    "text": "ECB signals pause",
                    "label": 1,
                    "topic": "Fed | Central Banks",
                },
            ),
            (
                "huggingface_bloomberg_financial_news",
                {
                    "row_idx": 12,
                    "row": {
                        "title": "Dollar rises as yields climb",
                        "text": "The dollar advanced against peers.",
                    },
                },
                {
                    "row_idx": 12,
                    "dataset": "danidanou/Bloomberg_Financial_News",
                    "title": "Dollar rises as yields climb",
                    "text": "The dollar advanced against peers.",
                },
            ),
            (
                "huggingface_yahoo_finance",
                {
                    "row_idx": 13,
                    "row": {
                        "headline": "Treasury yields edge higher",
                        "source": "Yahoo Finance",
                    },
                },
                {
                    "row_idx": 13,
                    "dataset": "defeatbeta/yahoo-finance-data",
                    "headline": "Treasury yields edge higher",
                    "source": "Yahoo Finance",
                },
            ),
            (
                "huggingface_finance_tasks",
                {
                    "row_idx": 14,
                    "row": {
                        "instruction": "Classify the market regime.",
                        "answer": "risk-on",
                    },
                },
                {
                    "row_idx": 14,
                    "dataset": "AdaptLLM/finance-tasks",
                    "instruction": "Classify the market regime.",
                    "answer": "risk-on",
                },
            ),
        ]

        for provider_name, item, expected_subset in cases:
            with self.subTest(provider_name=provider_name):
                provider = create_extractor(provider_name)
                normalized = provider.normalize_row(item)
                _assert_subset(self, normalized, expected_subset)


class ProviderFetchEventsTests(unittest.TestCase):
    def test_fetch_events_for_calendar_and_central_bank_sources(self) -> None:
        cases = [
            (
                "huggingface_forex_factory_calendar",
                [
                    {
                        "row_idx": 1,
                        "event_time": "2025-04-07 08:30",
                        "currency": "USD",
                        "impact": "High",
                        "event": "Non-Farm Payrolls",
                        "actual": "250K",
                        "forecast": "210K",
                        "previous": "180K",
                        "detail": "Strong labor market data.",
                    }
                ],
                {
                    "provider": "huggingface_forex_factory_calendar",
                    "source": "Ehsanrs2/Forex_Factory_Calendar",
                    "event_type": "economic_calendar",
                    "category": "economic_data",
                    "title": "USD - Non-Farm Payrolls",
                    "text": "Strong labor market data.",
                    "published_at": "2025-04-07 08:30",
                    "currencies": ["USD"],
                    "impact": "High",
                },
            ),
            (
                "huggingface_central_bank_communications",
                [
                    {
                        "row_idx": 2,
                        "central_bank": "Federal Reserve",
                        "document_type": "minutes",
                        "event_date": "2024-01-31",
                        "text": "Policy remains restrictive.",
                        "sentiment": "hawkish",
                        "topic": "inflation",
                        "version": "v1",
                        "language": "en",
                    }
                ],
                {
                    "provider": "huggingface_central_bank_communications",
                    "source": "aufklarer/central-bank-communications",
                    "event_type": "central_bank_communication",
                    "category": "monetary_policy",
                    "title": "Federal Reserve - minutes",
                    "text": "Policy remains restrictive.",
                    "published_at": "2024-01-31",
                    "currencies": ["USD"],
                    "impact": None,
                },
            ),
            (
                "huggingface_bis_central_bank_speeches",
                [
                    {
                        "row_idx": 3,
                        "central_bank": "European Central Bank",
                        "description": "Speech at Frankfurt forum",
                        "speech_text": "Inflation remains elevated.",
                        "event_date": "2024-02-20",
                        "year": 2024,
                        "month": 2,
                    }
                ],
                {
                    "provider": "huggingface_bis_central_bank_speeches",
                    "source": "samchain/bis_central_bank_speeches",
                    "event_type": "central_bank_speech",
                    "category": "monetary_policy",
                    "title": "Speech at Frankfurt forum",
                    "text": "Inflation remains elevated.",
                    "published_at": "2024-02-20",
                    "currencies": ["EUR"],
                    "impact": None,
                },
            ),
            (
                "huggingface_european_central_bank",
                [
                    {
                        "row_idx": 4,
                        "text": "Inflation is expected to decline.",
                        "stance_label": "neutral",
                        "time_label": "forward looking",
                        "certain_label": "certain",
                        "year": 2015,
                    }
                ],
                {
                    "provider": "huggingface_european_central_bank",
                    "source": "gtfintechlab/european_central_bank",
                    "event_type": "central_bank_communication",
                    "category": "monetary_policy",
                    "title": "European Central Bank policy sentence",
                    "text": "Inflation is expected to decline.",
                    "published_at": 2015,
                    "currencies": ["EUR"],
                    "impact": None,
                },
            ),
            (
                "huggingface_central_bank_of_brazil",
                [
                    {
                        "row_idx": 5,
                        "text": "Copom will maintain vigilance.",
                        "stance_label": "hawkish",
                        "time_label": "not forward looking",
                        "certain_label": "uncertain",
                        "year": 2008,
                    }
                ],
                {
                    "provider": "huggingface_central_bank_of_brazil",
                    "source": "gtfintechlab/central_bank_of_brazil",
                    "event_type": "central_bank_communication",
                    "category": "monetary_policy",
                    "title": "Central Bank of Brazil policy sentence",
                    "text": "Copom will maintain vigilance.",
                    "published_at": 2008,
                    "currencies": ["BRL"],
                    "impact": None,
                },
            ),
        ]

        for provider_name, records, expected_subset in cases:
            with self.subTest(provider_name=provider_name):
                provider = create_extractor(provider_name)
                with patch.object(provider, "fetch_records", return_value=records):
                    events = provider.fetch_events(max_rows=1)
                self.assertEqual(len(events), 1)
                _assert_subset(self, events[0], expected_subset)
                self.assertIn("event_id", events[0])
                self.assertIn("metadata", events[0])


if __name__ == "__main__":
    unittest.main()
