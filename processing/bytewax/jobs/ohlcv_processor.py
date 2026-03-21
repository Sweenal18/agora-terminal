import json
import logging
import os
import socket
from datetime import timedelta, timezone, datetime

import bytewax.operators as op
import bytewax.operators.window as win
from bytewax.connectors.kafka import KafkaSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators.window import SystemClockConfig, TumblingWindow

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
log = logging.getLogger("bytewax.ohlcv")

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC_CRYPTO_TICKS", "crypto.ticks.raw")
QUESTDB_HOST     = os.getenv("QUESTDB_HOST", "questdb")
QUESTDB_ILP_PORT = int(os.getenv("QUESTDB_ILP_PORT", "9009"))
WINDOW_SECONDS   = int(os.getenv("OHLCV_WINDOW_SECONDS", "60"))
ALIGN_TO         = datetime(2026, 1, 1, tzinfo=timezone.utc)


class OHLCVCandle:
    def __init__(self):
        self.symbol      = None
        self.open        = None
        self.high        = None
        self.low         = None
        self.close       = None
        self.volume      = 0.0
        self.trade_count = 0

    def update(self, tick):
        price    = tick["price"]
        quantity = tick["quantity"]
        if self.open is None:
            self.symbol = tick["symbol"]
            self.open   = price
            self.high   = price
            self.low    = price
        else:
            self.high = max(self.high, price)
            self.low  = min(self.low, price)
        self.close       = price
        self.volume     += quantity
        self.trade_count += 1
        return self

    def to_ilp(self, window_close_ms):
        if self.open is None:
            return None
        ts_ns = window_close_ms * 1_000_000
        return (
            f"ohlcv_1m,symbol={self.symbol} "
            f"open={self.open},"
            f"high={self.high},"
            f"low={self.low},"
            f"close={self.close},"
            f"volume={self.volume},"
            f"trade_count={self.trade_count}i "
            f"{ts_ns}"
        )

    def __repr__(self):
        if self.open is None:
            return "OHLCVCandle(empty)"
        return (
            f"{self.symbol} O={self.open:.2f} H={self.high:.2f} "
            f"L={self.low:.2f} C={self.close:.2f} "
            f"V={self.volume:.4f} trades={self.trade_count}"
        )


class QuestDBWriter:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._socket = None
        self._connect()

    def _connect(self):
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect((self.host, self.port))
            log.info(f"Connected to QuestDB at {self.host}:{self.port}")
        except Exception as e:
            log.error(f"QuestDB connect failed: {e}")
            self._socket = None

    def write(self, ilp_line):
        if not self._socket:
            self._connect()
        try:
            self._socket.sendall((ilp_line + "\n").encode())
        except Exception as e:
            log.error(f"QuestDB write failed: {e}")
            self._socket = None


_writer = None

def get_writer():
    global _writer
    if _writer is None:
        _writer = QuestDBWriter(QUESTDB_HOST, QUESTDB_ILP_PORT)
    return _writer


def deserialize(msg):
    try:
        tick = json.loads(msg.value.decode("utf-8"))
        return tick["symbol"], tick
    except Exception as e:
        log.error(f"Deserialize error: {e}")
        return None


def empty_candle():
    return OHLCVCandle()


def fold_candle(candle, tick):
    return candle.update(tick)


def write_to_questdb(item):
    key, (window_meta, candle) = item
    if candle is None or candle.open is None:
        return item
    window_close_ms = int(window_meta.close_time.timestamp() * 1000)
    ilp = candle.to_ilp(window_close_ms)
    if ilp:
        get_writer().write(ilp)
        log.info(f"Candle -> {candle} @ {window_meta.close_time.strftime('%H:%M:%S')}")
    return item


flow = Dataflow("ohlcv")

stream = op.input("kafka_in", flow, KafkaSource(brokers=[KAFKA_BOOTSTRAP], topics=[KAFKA_TOPIC]))
parsed = op.map("deserialize", stream, deserialize)
filtered = op.filter("filter_none", parsed, lambda x: x is not None)

candles = win.fold_window(
    "ohlcv_1m",
    filtered,
    SystemClockConfig(),
    TumblingWindow(length=timedelta(seconds=WINDOW_SECONDS), align_to=ALIGN_TO),
    empty_candle,
    fold_candle,
)

output = op.map("write_questdb", candles, write_to_questdb)
op.output("stdout", output, StdOutSink())


if __name__ == "__main__":
    log.info(f"Starting OHLCV Processor - window={WINDOW_SECONDS}s")
    from bytewax.run import cli_main
    cli_main(flow)
