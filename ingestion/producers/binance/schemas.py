"""
Data models for Binance tick messages.
"""
from dataclasses import dataclass


@dataclass
class TickMessage:
    """A single price tick from Binance."""
    symbol: str          # e.g. BTCUSDT
    price: float         # Current price
    quantity: float      # Trade quantity
    trade_id: int        # Binance trade ID
    timestamp_ms: int    # Event time in milliseconds (Binance)
    ingested_at_ms: int  # When we received it (our clock)
    side: str            # "buy" or "sell"

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "price": self.price,
            "quantity": self.quantity,
            "trade_id": self.trade_id,
            "timestamp_ms": self.timestamp_ms,
            "ingested_at_ms": self.ingested_at_ms,
            "side": self.side,
        }
