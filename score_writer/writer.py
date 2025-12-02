import json
import os
import time

import psycopg2
from kafka import KafkaConsumer

BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scores")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "fraud"),
    "user": os.getenv("DB_USER", "fraud_user"),
    "password": os.getenv("DB_PASSWORD", "fraud_pass"),
}


def get_connection():
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError:
            time.sleep(2)


def main():
    conn = get_connection()
    cur = conn.cursor()

    consumer = KafkaConsumer(
        SCORING_TOPIC,
        bootstrap_servers=BROKERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="score-writer",
    )

    for msg in consumer:
        data = msg.value
        cur.execute(
            """
            INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
            VALUES (%s, %s, %s);
            """,
            (
                data["transaction_id"],
                float(data["score"]),
                int(data["fraud_flag"]),
            ),
        )


if __name__ == "__main__":
    main()
