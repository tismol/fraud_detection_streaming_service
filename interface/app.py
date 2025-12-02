import json
import os
import uuid
import csv

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st
from kafka import KafkaProducer

KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions"),
}

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "fraud"),
    "user": os.getenv("DB_USER", "fraud_user"),
    "password": os.getenv("DB_PASSWORD", "fraud_pass"),
}

COLS = [
    "transaction_time", "merch", "cat_id", "amount",
    "name_1", "name_2", "gender",
    "street", "one_city", "us_state", "post_code",
    "lat", "lon", "population_city", "jobs",
    "merchant_lat", "merchant_lon",
]

NUMERIC_COLS = [
    "amount", "post_code", "lat", "lon",
    "population_city", "merchant_lat", "merchant_lon",
]


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_file(uploaded_file):
    try:
        df = pd.read_csv(uploaded_file)
        mask = df["merch"].isna()

        for idx in df[mask].index:
            raw = df.at[idx, "transaction_time"]
            parsed = next(csv.reader([raw], delimiter=",", quotechar='"'))
            if len(parsed) == len(COLS):
                for col, val in zip(COLS, parsed):
                    if col in NUMERIC_COLS:
                        df.at[idx, col] = pd.to_numeric(val, errors="coerce")
                    else:
                        df.at[idx, col] = val

        return df
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {e}")
        return None


def send_to_kafka(df, topic, bootstrap_servers):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    df = df.copy()
    df["transaction_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    total_rows = len(df)
    progress_bar = st.progress(0.0)

    for idx, row in df.iterrows():
        producer.send(
            topic,
            value={
                "transaction_id": row["transaction_id"],
                "data": row.drop("transaction_id").to_dict(),
            },
        )
        progress_bar.progress((idx + 1) / total_rows)

    producer.flush()


def fetch_last_fraud():
    query = """
        SELECT transaction_id, score, fraud_flag, scored_at
        FROM transaction_scores
        WHERE fraud_flag = 1
        ORDER BY scored_at DESC
        LIMIT 10;
    """
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def fetch_last_scores(limit=100):
    query = """
        SELECT score
        FROM transaction_scores
        ORDER BY scored_at DESC
        LIMIT %s;
    """
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(query, (limit,))
        rows = cur.fetchall()
    return pd.DataFrame([r[0] for r in rows], columns=["score"])


def show_results_block():
    st.subheader("Результаты скоринга")

    if st.button("Обновить"):
        st.rerun()

    try:
        fraud_df = fetch_last_fraud()
        scores_df = fetch_last_scores()
    except Exception as e:
        st.error(f"Ошибка при обращении к базе данных: {e}")
        return

    st.markdown("**Последние транзакции с fraud_flag = 1:**")
    if fraud_df.empty:
        st.info("Записей с fraud_flag = 1 пока нет.")
    else:
        st.dataframe(fraud_df, use_container_width=True)

    st.markdown("**Распределение скоров последних транзакций:**")
    if scores_df.empty:
        st.info("Нет данных для построения гистограммы.")
    else:
        chart = (
            alt.Chart(scores_df)
            .mark_bar()
            .encode(
                x=alt.X("score", bin=alt.Bin(maxbins=30), title="score"),
                y=alt.Y("count()", title="Количество транзакций"),
            )
        )
        st.altair_chart(chart, use_container_width=True)


if "current_file" not in st.session_state:
    st.session_state.current_file = None
if "current_df" not in st.session_state:
    st.session_state.current_df = None
if "send_status" not in st.session_state:
    st.session_state.send_status = "Файл не отправлен"

st.title("Отправка данных в Kafka")

tab_send, tab_results = st.tabs(["Отправка", "Результаты"])

with tab_send:
    uploaded = st.file_uploader(
        "Загрузите CSV файл с транзакциями", type=["csv"]
    )

    if uploaded is not None:
        df = load_file(uploaded)
        if df is not None:
            st.session_state.current_file = uploaded.name
            st.session_state.current_df = df
            st.session_state.send_status = "Файл загружен"
            st.success(f"Файл {uploaded.name} загружен.")

    st.markdown("### Текущий файл")
    if st.session_state.current_file is None:
        st.info("Файл ещё не выбран.")
    else:
        st.markdown(f"**Файл:** `{st.session_state.current_file}`")
        st.markdown(f"**Статус:** `{st.session_state.send_status}`")

        if st.button("Отправить файл в Kafka"):
            if st.session_state.current_df is not None:
                with st.spinner("Отправка транзакций в Kafka..."):
                    send_to_kafka(
                        st.session_state.current_df,
                        KAFKA_CONFIG["topic"],
                        KAFKA_CONFIG["bootstrap_servers"],
                    )
                st.session_state.send_status = "Отправлен"
                st.success("Все транзакции отправлены в Kafka.")

with tab_results:
    show_results_block()
