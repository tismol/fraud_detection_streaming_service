# Потоковый скоринг фродовых транзакций

Пайплайн:

1. Пользователь загружает CSV с транзакциями через веб-интерфейс.
2. UI отправляет каждую строку в Kafka-топик `transactions` с уникальным `transaction_id`.
3. Сервис `fraud_detector`:
   - читает сообщения из `transactions`,
   - применяет препроцессинг и модель,
   - пишет результат в топик `scores` в виде `{transaction_id, score, fraud_flag}`.
4. Сервис `score_writer`:
   - читает `scores`,
   - сохраняет результаты в таблицу `transaction_scores` в PostgreSQL.
5. UI в отдельной вкладке:
   - показывает последние 10 транзакций с `fraud_flag = 1`,
   - строит гистограмму распределения `score` по последним 100 транзакциям.
---

## Структура проекта

```text
.
├── docker-compose.yml            # Описание всех сервисов и сети
├── README.md
├── db
│   └── init.sql                  # Инициализация схемы БД
├── fraud_detector
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── models
│   │   └── catboost_fraud.cbm    # Обученная модель
│   └── src
│       ├── __init__.py
│       ├── preprocessing.py      # Препроцессинг
│       ├── model.py              # Скоринг
│       └── service.py            # Чтение из Kafka, скоринг, запись в Kafka
├── score_writer
│   ├── Dockerfile
│   ├── requirements.txt
│   └── writer.py                 # Запись в БД
└── interface
    ├── Dockerfile
    ├── requirements.txt
    ├── app.py                    # UI: загрузка CSV, отправка в Kafka, просмотр результатов
    └── .streamlit
        └── config.toml
```
---

## Как запустить

1. Клонировать репозиторий:

   ```bash
   git clone https://github.com/tismol/fraud_detection_streaming_service.git
   cd fraud_detection_streaming_service
   ```

2. Запустить сборку:

   ```bash
   docker compose up -d --build
   ```

3. Открыть веб-интерфейс:

   * UI приложения: [http://localhost:8501](http://localhost:8501)
   * Kafka UI: [http://localhost:8080](http://localhost:8080)

---

## Как протестировать работу

### 1. Отправка транзакций

1. Перейти на [http://localhost:8501](http://localhost:8501).
2. Вкладка **«Отправка»**:

   * Загрузить CSV-файл с транзакциями.
   * Дождаться сообщения «Файл … загружен».
   * Нажать кнопку **«Отправить файл в Kafka»**.


### 2. Проверка скоринга

1. Вкладка **«Результаты»** в UI:

   * Кнопка **«Обновить»** перерисовывает таблицу и гистограмму.
   * Верхняя таблица: последние 10 записей, где `fraud_flag = 1`.
   * Ниже: гистограмма распределения `score` по последним 100 транзакциям.


---