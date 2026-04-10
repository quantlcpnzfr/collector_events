"""Market data extractors."""

from .commodities import CommodityQuoteExtractor
from .cot import CotPositioningExtractor
from .country_index import CountryStockIndexExtractor
from .crypto import (
    AiTokenExtractor,
    CryptoQuoteExtractor,
    CryptoSectorExtractor,
    DefiTokenExtractor,
    OtherTokenExtractor,
    StablecoinExtractor,
)
from .etf import BtcEtfFlowExtractor
from .fear_greed import FearGreedExtractor
from .fx_rates import FXRateExtractor
from .gulf import GulfQuoteExtractor
from .prediction_markets import PredictionMarketExtractor
from .stocks import EarningsCalendarExtractor, SectorPerformanceExtractor, StockQuoteExtractor

__all__ = [
    "AiTokenExtractor",
    "BtcEtfFlowExtractor",
    "CommodityQuoteExtractor",
    "CotPositioningExtractor",
    "CountryStockIndexExtractor",
    "CryptoQuoteExtractor",
    "CryptoSectorExtractor",
    "DefiTokenExtractor",
    "EarningsCalendarExtractor",
    "FXRateExtractor",
    "FearGreedExtractor",
    "GulfQuoteExtractor",
    "OtherTokenExtractor",
    "PredictionMarketExtractor",
    "SectorPerformanceExtractor",
    "StablecoinExtractor",
    "StockQuoteExtractor",
]
