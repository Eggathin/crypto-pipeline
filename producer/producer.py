import json
import logging
import os
import signal
import time

import websocket
from confluent_kafka import Producer

FEED_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCTS = os.environ.get("PRODUCTS", "BTC-USD,ETH-USD").split(",")
TOPIC = os.environ.get("TOPIC", "crypto.trades.raw")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")

log = logging.getLogger("producer")
running = True


def shutdown(signum, frame):
    global running
    running = False
    log.info("received signal %s, stopping", signum)


def on_delivery(err, msg):
    if err is not None:
        log.error("delivery failed for key %s: %s", msg.key(), err)


def build_producer():
    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 50,
        "compression.type": "lz4",
        "enable.idempotence": True,
        "acks": "all",
    })


def stream(producer):
    ws = websocket.create_connection(FEED_URL, timeout=30)
    log.info("connected, subscribing to %s", PRODUCTS)
    ws.send(json.dumps({
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channels": ["matches"],
    }))

    last_trade_id = {}

    try:
        while running:
            msg = json.loads(ws.recv())

            if msg.get("type") != "match":
                log.info("control frame: %s", msg.get("type"))
                continue

            product = msg["product_id"]
            trade_id = msg["trade_id"]
            previous = last_trade_id.get(product)
            if previous is not None and trade_id != previous + 1:
                log.warning(
                    "trade_id gap on %s: %s -> %s", product, previous, trade_id
                )
            last_trade_id[product] = trade_id

            envelope = {
                "source": "coinbase",
                "ingested_at": time.time(),
                "payload": msg,
            }

            producer.produce(
                TOPIC,
                key=product.encode(),
                value=json.dumps(envelope).encode(),
                on_delivery=on_delivery,
            )
            producer.poll(0)
    finally:
        ws.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    producer = build_producer()
    backoff = 1

    while running:
        try:
            stream(producer)
            backoff = 1
        except Exception:
            log.exception("stream error, reconnecting in %ss", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
        finally:
            producer.flush(10)

    log.info("stopped")


if __name__ == "__main__":
    main()