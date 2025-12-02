import json
import os

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

from .model import load_model, score

BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scores")


def main():
    model = load_model()

    consumer = KafkaConsumer(
        TRANSACTIONS_TOPIC,
        bootstrap_servers=BROKERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="fraud-detector",
    )

    producer = KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for msg in consumer:
        payload = msg.value
        df = pd.DataFrame([payload["data"]])
        scored = score(df, model)
        out = {
            "transaction_id": payload["transaction_id"],
            "score": float(scored.loc[0, "score"]),
            "fraud_flag": int(scored.loc[0, "fraud_flag"]),
        }
        producer.send(SCORING_TOPIC, value=out)


if __name__ == "__main__":
    main()
