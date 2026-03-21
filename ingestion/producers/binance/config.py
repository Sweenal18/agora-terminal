"""
Configuration for the Binance WebSocket producer.
All values can be overridden via environment variables.
"""
import os

# Symbols to stream — all USDT pairs
SYMBOLS = [
    "btcusdt",
    "ethusdt",
    "solusdt",
]

# Binance WebSocket base URL (no API key needed)
BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_CRYPTO_TICKS", "crypto.ticks.raw")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
