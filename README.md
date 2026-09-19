# Crypto Market Data Project

## Key Information

Exchange: `wss://ws-feed.exchange.coinbase.com`
Channel: `matches`
Topic name: `crypto.trades.raw`
Partition count: `3`
Key choice: `product_id`

## Running the program

Ensure that the Docker Daemon is running. In the terminal:

`docker compose up -d`
`pip install -r requirements.txt`
`python producer.py`

In another terminal:

```{bash}
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic crypto.trades.raw \
  --from-beginning --property print.key=true --max-messages 10
```

Check that the partition distribution correctly reflects keying:

```{bash}
docker exec kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic crypto.trades.raw
```
