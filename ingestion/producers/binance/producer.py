"""
Binance WebSocket Producer
--------------------------
Connects to Binance live trade stream for BTC, ETH, SOL.
Publishes each tick as a JSON message to Kafka topic: crypto.ticks.raw

Usage:
    python ingestion/producers/binance/producer.py

No API key required. Binance market data is free and open.
"""

import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone

import websocket
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from ingestion.producers.binance.config import (
    BINANCE_WS_BASE,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    LOG_LEVEL,
    SYMBOLS,
)
from ingestion.producers.binance.schemas import TickMessage

# --- Logging ------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s � %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("binance.producer")

# --- Kafka setup --------------------------------------------------------------

def create_kafka_topic(bootstrap_servers: str, topic: str) -> None:
    """Create the Kafka topic if it does not already exist."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        new_topic = NewTopic(topic, num_partitions=3, replication_factor=1)
        fs = admin.create_topics([new_topic])
        for t, f in fs.items():
            try:
                f.result()
                log.info(f"Created Kafka topic: {t}")
            except Exception as e:
                log.warning(f"Could not create topic {t}: {e}")
    else:
        log.info(f"Kafka topic already exists: {topic}")


def delivery_report(err, msg) -> None:
    """Called by Kafka producer for every message delivered or failed."""
    if err:
        log.error(f"Delivery failed for {msg.key()}: {err}")


# --- WebSocket handlers -------------------------------------------------------

class BinanceProducer:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "agora-binance-producer",
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 500,
        })
        self.running = True
        self.tick_count = 0
        self.last_log_time = time.time()

        # Graceful shutdown on Ctrl+C
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        log.info("Shutdown signal received � flushing Kafka producer...")
        self.running = False
        self.producer.flush(timeout=10)
        log.info(f"Shutdown complete. Total ticks published: {self.tick_count}")
        sys.exit(0)

    def on_message(self, ws, raw: str) -> None:
        """Handle incoming WebSocket message from Binance."""
        try:
            data = json.loads(raw)
            __stream = data.get("stream", "")
            payload = data.get("data", {})

            # Trade stream event
            if payload.get("e") != "trade":
                return

            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

            tick = TickMessage(
                symbol=payload["s"],
                price=float(payload["p"]),
                quantity=float(payload["q"]),
                trade_id=payload["t"],
                timestamp_ms=payload["T"],
                ingested_at_ms=now_ms,
                side="buy" if payload.get("m") is False else "sell",
            )

            self.producer.produce(
                topic=KAFKA_TOPIC,
                key=tick.symbol.encode(),
                value=json.dumps(tick.to_dict()).encode(),
                callback=delivery_report,
            )
            self.producer.poll(0)  # Non-blocking flush
            self.tick_count += 1

            # Log a summary every 10 seconds
            now = time.time()
            if now - self.last_log_time >= 10:
                log.info(f"Published {self.tick_count} ticks | Latest: {tick.symbol} @ ${tick.price:,.2f}")
                self.last_log_time = now

        except Exception as e:
            log.error(f"Error processing message: {e}", exc_info=True)

    def on_error(self, ws, error) -> None:
        log.error(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg) -> None:
        log.warning(f"WebSocket closed: {close_status_code} � {close_msg}")

    def on_open(self, ws) -> None:
        log.info(f"WebSocket connected. Streaming: {[s.upper() for s in SYMBOLS]}")

    def run(self) -> None:
        # Build combined stream URL � e.g. btcusdt@trade/ethusdt@trade/solusdt@trade
        streams = "/".join(f"{s}@trade" for s in SYMBOLS)
        url = f"{BINANCE_WS_BASE}?streams={streams}"

        log.info(f"Connecting to Binance stream: {url}")
        log.info(f"Publishing to Kafka topic: {KAFKA_TOPIC}")

        create_kafka_topic(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

        while self.running:
            try:
                ws = websocket.WebSocketApp(
                    url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                log.error(f"WebSocket crashed: {e}. Reconnecting in 5s...")
                time.sleep(5)


if __name__ == "__main__":
    log.info("Starting Agora Terminal � Binance Producer")
    BinanceProducer().run()
