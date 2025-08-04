import msgspec
from datetime import datetime
from typing import List, Dict, Optional

# Define DOM (Depth of Market) level data
class DOMLevel(msgspec.Struct, gc=False, frozen=True):
    price: float
    size: float
    market_maker: str = ""  # Leave empty for anonymous market makers

# Define order book snapshot data
class OrderBookSnapshot(msgspec.Struct, gc=False, frozen=True):
    # Basic information
    symbol: str                  # e.g. "EUR.USD"
    # currency_pair: str           # e.g. "EUR/USD"
    # exchange: str                # e.g. "IDEALPRO"
    # timestamp: datetime          # Timestamp with timezone
    kind: str
    
    # Current quotes
    # bid: float
    # ask: float
    # bid_size: float
    # ask_size: float
    
    # Order book depth (first N levels)
    bids: List[DOMLevel]         # Bid side depth (sorted by price descending)
    asks: List[DOMLevel]         # Ask side depth (sorted by price ascending)
    
    # Optional: historical data (trimmed as needed)
    prev_bid: Optional[float] = None
    prev_ask: Optional[float] = None
    daily_high: Optional[float] = None
    daily_low: Optional[float] = None