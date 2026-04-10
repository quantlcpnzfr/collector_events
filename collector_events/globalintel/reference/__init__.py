"""Shared reference data — Python equivalents of worldmonitor shared/*.json.

These constants provide the symbol catalogs, country mappings, and sector
definitions used by market/economic extractors.  Mirrors the single-source-
of-truth pattern from worldmonitor's ``shared/`` directory.
"""

from __future__ import annotations

# ─── Stock symbols (shared/stocks.json) ──────────────────────────────

STOCK_SYMBOLS: list[dict] = [
    # US Indices
    {"symbol": "^GSPC", "name": "S&P 500", "display": "S&P 500"},
    {"symbol": "^DJI", "name": "Dow Jones", "display": "Dow Jones"},
    {"symbol": "^IXIC", "name": "NASDAQ Composite", "display": "NASDAQ"},
    # US Mega-caps
    {"symbol": "AAPL", "name": "Apple", "display": "AAPL"},
    {"symbol": "MSFT", "name": "Microsoft", "display": "MSFT"},
    {"symbol": "GOOGL", "name": "Alphabet", "display": "GOOGL"},
    {"symbol": "AMZN", "name": "Amazon", "display": "AMZN"},
    {"symbol": "NVDA", "name": "NVIDIA", "display": "NVDA"},
    {"symbol": "META", "name": "Meta Platforms", "display": "META"},
    {"symbol": "TSLA", "name": "Tesla", "display": "TSLA"},
    {"symbol": "BRK-B", "name": "Berkshire Hathaway", "display": "BRK.B"},
    {"symbol": "JPM", "name": "JPMorgan Chase", "display": "JPM"},
    {"symbol": "V", "name": "Visa", "display": "V"},
    {"symbol": "JNJ", "name": "Johnson & Johnson", "display": "JNJ"},
    {"symbol": "UNH", "name": "UnitedHealth", "display": "UNH"},
    {"symbol": "XOM", "name": "Exxon Mobil", "display": "XOM"},
    {"symbol": "WMT", "name": "Walmart", "display": "WMT"},
    {"symbol": "MA", "name": "Mastercard", "display": "MA"},
    {"symbol": "PG", "name": "Procter & Gamble", "display": "PG"},
    {"symbol": "HD", "name": "Home Depot", "display": "HD"},
    {"symbol": "COST", "name": "Costco", "display": "COST"},
    {"symbol": "ABBV", "name": "AbbVie", "display": "ABBV"},
    {"symbol": "CRM", "name": "Salesforce", "display": "CRM"},
    {"symbol": "BAC", "name": "Bank of America", "display": "BAC"},
    {"symbol": "NFLX", "name": "Netflix", "display": "NFLX"},
    {"symbol": "AMD", "name": "AMD", "display": "AMD"},
    # Indian Indices + Stocks
    {"symbol": "^NSEI", "name": "Nifty 50", "display": "NIFTY"},
    {"symbol": "^BSESN", "name": "BSE Sensex", "display": "SENSEX"},
]

# Symbols that must use Yahoo Finance only (no Finnhub)
YAHOO_ONLY_SYMBOLS = [
    "^GSPC", "^DJI", "^IXIC", "^NSEI", "^BSESN",
]

# ─── Commodities (shared/commodities.json) ───────────────────────────

COMMODITIES: list[dict] = [
    # Volatility
    {"symbol": "^VIX", "name": "VIX", "display": "VIX"},
    # Precious metals
    {"symbol": "GC=F", "name": "Gold", "display": "Gold"},
    {"symbol": "SI=F", "name": "Silver", "display": "Silver"},
    {"symbol": "HG=F", "name": "Copper", "display": "Copper"},
    {"symbol": "PL=F", "name": "Platinum", "display": "Platinum"},
    {"symbol": "PA=F", "name": "Palladium", "display": "Palladium"},
    {"symbol": "ALI=F", "name": "Aluminum", "display": "Aluminum"},
    # Energy
    {"symbol": "CL=F", "name": "WTI Crude", "display": "WTI"},
    {"symbol": "BZ=F", "name": "Brent Crude", "display": "Brent"},
    {"symbol": "NG=F", "name": "Natural Gas", "display": "NatGas"},
    {"symbol": "TTF=F", "name": "EU Natural Gas (TTF)", "display": "TTF"},
    {"symbol": "RB=F", "name": "Gasoline", "display": "Gasoline"},
    {"symbol": "HO=F", "name": "Heating Oil", "display": "Heating Oil"},
    # Thematic ETFs
    {"symbol": "URA", "name": "Global X Uranium ETF", "display": "Uranium"},
    {"symbol": "LIT", "name": "Global X Lithium ETF", "display": "Lithium"},
    # Coal
    {"symbol": "MTF=F", "name": "Newcastle Coal", "display": "Coal"},
    # Agriculture
    {"symbol": "ZW=F", "name": "Wheat", "display": "Wheat"},
    {"symbol": "ZC=F", "name": "Corn", "display": "Corn"},
    {"symbol": "ZS=F", "name": "Soybeans", "display": "Soybeans"},
    {"symbol": "ZR=F", "name": "Rice", "display": "Rice"},
    {"symbol": "KC=F", "name": "Coffee", "display": "Coffee"},
    {"symbol": "SB=F", "name": "Sugar", "display": "Sugar"},
    {"symbol": "CC=F", "name": "Cocoa", "display": "Cocoa"},
    {"symbol": "CT=F", "name": "Cotton", "display": "Cotton"},
    # FX pairs
    {"symbol": "EURUSD=X", "name": "EUR/USD", "display": "EUR/USD"},
    {"symbol": "GBPUSD=X", "name": "GBP/USD", "display": "GBP/USD"},
    {"symbol": "JPY=X", "name": "USD/JPY", "display": "USD/JPY"},
    {"symbol": "CNY=X", "name": "USD/CNY", "display": "USD/CNY"},
    {"symbol": "INR=X", "name": "USD/INR", "display": "USD/INR"},
    {"symbol": "AUDUSD=X", "name": "AUD/USD", "display": "AUD/USD"},
    {"symbol": "CHF=X", "name": "USD/CHF", "display": "USD/CHF"},
    {"symbol": "CAD=X", "name": "USD/CAD", "display": "USD/CAD"},
    {"symbol": "TRY=X", "name": "USD/TRY", "display": "USD/TRY"},
]

# ─── Crypto — top 10 (shared/crypto.json) ────────────────────────────

CRYPTO_IDS: list[str] = [
    "bitcoin", "ethereum", "binancecoin", "solana", "ripple",
    "cardano", "dogecoin", "tron", "avalanche-2", "chainlink",
]
CRYPTO_META: dict[str, dict] = {
    "bitcoin": {"name": "Bitcoin", "symbol": "BTC"},
    "ethereum": {"name": "Ethereum", "symbol": "ETH"},
    "binancecoin": {"name": "BNB", "symbol": "BNB"},
    "solana": {"name": "Solana", "symbol": "SOL"},
    "ripple": {"name": "XRP", "symbol": "XRP"},
    "cardano": {"name": "Cardano", "symbol": "ADA"},
    "dogecoin": {"name": "Dogecoin", "symbol": "DOGE"},
    "tron": {"name": "TRON", "symbol": "TRX"},
    "avalanche-2": {"name": "Avalanche", "symbol": "AVAX"},
    "chainlink": {"name": "Chainlink", "symbol": "LINK"},
}
CRYPTO_COINPAPRIKA: dict[str, str] = {
    "bitcoin": "btc-bitcoin", "ethereum": "eth-ethereum",
    "binancecoin": "bnb-binance-coin", "solana": "sol-solana",
    "ripple": "xrp-xrp", "cardano": "ada-cardano",
    "dogecoin": "doge-dogecoin", "tron": "trx-tron",
    "avalanche-2": "avax-avalanche", "chainlink": "link-chainlink",
}

# ─── DeFi tokens (shared/defi-tokens.json) ───────────────────────────

DEFI_IDS: list[str] = [
    "aave", "uniswap", "jupiter-exchange-solana", "pendle",
    "maker", "lido-dao", "hyperliquid", "raydium", "aerodrome-finance", "curve-dao-token",
]
DEFI_META: dict[str, dict] = {
    "aave": {"name": "Aave", "symbol": "AAVE"},
    "uniswap": {"name": "Uniswap", "symbol": "UNI"},
    "jupiter-exchange-solana": {"name": "Jupiter", "symbol": "JUP"},
    "pendle": {"name": "Pendle", "symbol": "PENDLE"},
    "maker": {"name": "Maker", "symbol": "MKR"},
    "lido-dao": {"name": "Lido DAO", "symbol": "LDO"},
    "hyperliquid": {"name": "Hyperliquid", "symbol": "HYPE"},
    "raydium": {"name": "Raydium", "symbol": "RAY"},
    "aerodrome-finance": {"name": "Aerodrome", "symbol": "AERO"},
    "curve-dao-token": {"name": "Curve", "symbol": "CRV"},
}

# ─── AI tokens (shared/ai-tokens.json) ───────────────────────────────

AI_TOKEN_IDS: list[str] = [
    "bittensor", "render-token", "artificial-superintelligence-alliance",
    "akash-network", "ocean-protocol", "singularitynet",
    "grass", "virtual-protocol", "ai16z", "griffain",
]
AI_TOKEN_META: dict[str, dict] = {
    "bittensor": {"name": "Bittensor", "symbol": "TAO"},
    "render-token": {"name": "Render", "symbol": "RENDER"},
    "artificial-superintelligence-alliance": {"name": "Fetch.ai", "symbol": "FET"},
    "akash-network": {"name": "Akash", "symbol": "AKT"},
    "ocean-protocol": {"name": "Ocean Protocol", "symbol": "OCEAN"},
    "singularitynet": {"name": "SingularityNET", "symbol": "AGIX"},
    "grass": {"name": "Grass", "symbol": "GRASS"},
    "virtual-protocol": {"name": "Virtual Protocol", "symbol": "VIRTUAL"},
    "ai16z": {"name": "ai16z", "symbol": "AI16Z"},
    "griffain": {"name": "Griffain", "symbol": "GRIFFAIN"},
}

# ─── Other tokens (shared/other-tokens.json) ─────────────────────────

OTHER_TOKEN_IDS: list[str] = [
    "aptos", "sui", "sei-network", "injective-protocol",
    "celestia", "pyth-network", "jito-governance-token",
    "movement", "wormhole", "ondo-finance",
]
OTHER_TOKEN_META: dict[str, dict] = {
    "aptos": {"name": "Aptos", "symbol": "APT"},
    "sui": {"name": "Sui", "symbol": "SUI"},
    "sei-network": {"name": "Sei", "symbol": "SEI"},
    "injective-protocol": {"name": "Injective", "symbol": "INJ"},
    "celestia": {"name": "Celestia", "symbol": "TIA"},
    "pyth-network": {"name": "Pyth", "symbol": "PYTH"},
    "jito-governance-token": {"name": "Jito", "symbol": "JTO"},
    "movement": {"name": "Movement", "symbol": "MOVE"},
    "wormhole": {"name": "Wormhole", "symbol": "W"},
    "ondo-finance": {"name": "Ondo", "symbol": "ONDO"},
}

# ─── Stablecoins (shared/stablecoins.json) ───────────────────────────

STABLECOIN_IDS: list[str] = ["tether", "usd-coin", "dai", "first-digital-usd", "ethena-usde"]
STABLECOIN_COINPAPRIKA: dict[str, str] = {
    "tether": "usdt-tether", "usd-coin": "usdc-usd-coin", "dai": "dai-dai",
    "first-digital-usd": "fdusd-first-digital-usd", "ethena-usde": "usde-ethena-usde",
}

# ─── Sectors (shared/sectors.json) ───────────────────────────────────

SECTOR_ETFS: list[dict] = [
    {"symbol": "XLK", "name": "Technology"},
    {"symbol": "XLF", "name": "Financials"},
    {"symbol": "XLE", "name": "Energy"},
    {"symbol": "XLV", "name": "Health Care"},
    {"symbol": "XLY", "name": "Consumer Discretionary"},
    {"symbol": "XLI", "name": "Industrials"},
    {"symbol": "XLP", "name": "Consumer Staples"},
    {"symbol": "XLU", "name": "Utilities"},
    {"symbol": "XLB", "name": "Materials"},
    {"symbol": "XLRE", "name": "Real Estate"},
    {"symbol": "XLC", "name": "Communication Services"},
    {"symbol": "SMH", "name": "Semiconductors"},
]

# ─── BTC Spot ETFs (shared/etfs.json) ────────────────────────────────

BTC_SPOT_ETFS: list[dict] = [
    {"ticker": "IBIT", "issuer": "BlackRock"},
    {"ticker": "FBTC", "issuer": "Fidelity"},
    {"ticker": "ARKB", "issuer": "ARK / 21Shares"},
    {"ticker": "BITB", "issuer": "Bitwise"},
    {"ticker": "GBTC", "issuer": "Grayscale"},
    {"ticker": "HODL", "issuer": "VanEck"},
    {"ticker": "BRRR", "issuer": "Valkyrie"},
    {"ticker": "EZBC", "issuer": "Franklin Templeton"},
    {"ticker": "BTCO", "issuer": "Invesco / Galaxy"},
    {"ticker": "BTCW", "issuer": "WisdomTree"},
]

# ─── Gulf / GCC (shared/gulf.json) ───────────────────────────────────

GULF_SYMBOLS: list[dict] = [
    {"symbol": "TASI.SR", "name": "Tadawul All Share", "country": "Saudi Arabia", "flag": "🇸🇦", "type": "index"},
    {"symbol": "DFMGI.AE", "name": "Dubai Financial Market", "country": "UAE", "flag": "🇦🇪", "type": "index"},
    {"symbol": "UAE", "name": "iShares MSCI UAE ETF", "country": "UAE", "flag": "🇦🇪", "type": "index"},
    {"symbol": "QAT", "name": "iShares MSCI Qatar ETF", "country": "Qatar", "flag": "🇶🇦", "type": "index"},
    {"symbol": "GULF", "name": "WisdomTree Gulf Dividend", "country": "GCC", "flag": "🏴", "type": "index"},
    {"symbol": "MSM30.OM", "name": "Muscat MSM 30", "country": "Oman", "flag": "🇴🇲", "type": "index"},
    {"symbol": "SAR=X", "name": "USD/SAR", "country": "Saudi Arabia", "flag": "🇸🇦", "type": "currency"},
    {"symbol": "AED=X", "name": "USD/AED", "country": "UAE", "flag": "🇦🇪", "type": "currency"},
    {"symbol": "QAR=X", "name": "USD/QAR", "country": "Qatar", "flag": "🇶🇦", "type": "currency"},
    {"symbol": "KWD=X", "name": "USD/KWD", "country": "Kuwait", "flag": "🇰🇼", "type": "currency"},
    {"symbol": "BHD=X", "name": "USD/BHD", "country": "Bahrain", "flag": "🇧🇭", "type": "currency"},
    {"symbol": "OMR=X", "name": "USD/OMR", "country": "Oman", "flag": "🇴🇲", "type": "currency"},
    {"symbol": "CL=F", "name": "WTI Crude Oil", "country": "Global", "flag": "🛢️", "type": "oil"},
    {"symbol": "BZ=F", "name": "Brent Crude Oil", "country": "Global", "flag": "🛢️", "type": "oil"},
]

# ─── Crypto sectors (shared/crypto-sectors.json) ─────────────────────

CRYPTO_SECTORS: list[dict] = [
    {"id": "layer-1", "name": "Layer 1", "tokens": ["ethereum", "solana", "cardano", "avalanche-2", "aptos"]},
    {"id": "defi", "name": "DeFi", "tokens": ["aave", "uniswap", "maker", "curve-dao-token", "lido-dao"]},
    {"id": "layer-2", "name": "Layer 2", "tokens": ["matic-network", "arbitrum", "optimism", "starknet", "mantle"]},
    {"id": "ai", "name": "AI & Compute", "tokens": ["bittensor", "render-token", "artificial-superintelligence-alliance", "akash-network", "ocean-protocol"]},
    {"id": "memes", "name": "Memes", "tokens": ["dogecoin", "shiba-inu", "pepe", "bonk", "floki"]},
    {"id": "gaming", "name": "Gaming", "tokens": ["immutable-x", "the-sandbox", "gala", "axie-infinity", "illuvium"]},
    {"id": "privacy", "name": "Privacy", "tokens": ["monero", "zcash", "decred", "oasis-network", "secret"]},
    {"id": "infra", "name": "Infrastructure", "tokens": ["chainlink", "the-graph", "filecoin", "arweave", "helium"]},
]

# ─── Central banks (finance variant layer) ───────────────────────────

CENTRAL_BANKS: list[dict] = [
    {"code": "FED", "name": "Federal Reserve", "country": "US", "currency": "USD"},
    {"code": "ECB", "name": "European Central Bank", "country": "EU", "currency": "EUR"},
    {"code": "BOJ", "name": "Bank of Japan", "country": "JP", "currency": "JPY"},
    {"code": "BOE", "name": "Bank of England", "country": "GB", "currency": "GBP"},
    {"code": "PBOC", "name": "People's Bank of China", "country": "CN", "currency": "CNY"},
    {"code": "SNB", "name": "Swiss National Bank", "country": "CH", "currency": "CHF"},
    {"code": "RBA", "name": "Reserve Bank of Australia", "country": "AU", "currency": "AUD"},
    {"code": "BOC", "name": "Bank of Canada", "country": "CA", "currency": "CAD"},
    {"code": "RBI", "name": "Reserve Bank of India", "country": "IN", "currency": "INR"},
    {"code": "BOK", "name": "Bank of Korea", "country": "KR", "currency": "KRW"},
    {"code": "BCB", "name": "Banco Central do Brasil", "country": "BR", "currency": "BRL"},
    {"code": "SAMA", "name": "Saudi Central Bank", "country": "SA", "currency": "SAR"},
    {"code": "BIS", "name": "Bank for International Settlements", "country": "INT", "currency": ""},
]

# ─── BIS country codes for REER / policy rate queries ────────────────

BIS_COUNTRY_CODES = "US+GB+JP+XM+CH+SG+IN+AU+CN+CA+KR+BR"
