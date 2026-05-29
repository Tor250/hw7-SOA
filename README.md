# hw5-SOA

## Запуск

Поднять весь стенд:

```bash
docker compose up --build
```

Поднять стенд в фоне:

```bash
docker compose up --build -d
```

Прогнать интеграционный тест:

```bash
docker compose --profile test up --build --abort-on-container-exit integration-test
```

Остановить и удалить контейнеры:

```bash
docker compose down -v
```

То же самое через `Makefile`:

```bash
make up-d      # поднять в фоне
make ps        # статус сервисов
make logs      # логи
make test      # интеграционный e2e тест
make agg       # пересчёт за последнюю доступную дату <= сегодня (UTC)
make export    # экспорт за последнюю агрегированную дату <= сегодня
make agg-latest
make export-latest
make grafana-sync
make reset-data
make down      # остановка и очистка
```

## Сервисы

- `producer` — HTTP API и генератор синтетических событий.
- `kafka-1..3` — Kafka KRaft cluster с репликацией.
- `schema-registry` — регистрация и версионирование Avro-схемы.
- `clickhouse` — raw storage и агрегаты.
- `analytics-service` — пересчёт метрик, идемпотентная синхронизация с PostgreSQL и экспорт в MinIO.
- `postgres` — готовые агрегаты для внешнего чтения.
- `minio` — S3-совместимое cold storage.
- `grafana` — готовый datasource и дашборд.

## Эндпоинты

- Producer API: `http://localhost:8000/events`
- Producer health: `http://localhost:8000/health`
- Analytics API: `http://localhost:8001/aggregation/run?date=YYYY-MM-DD`
- Export API: `http://localhost:8001/export/run?date=YYYY-MM-DD`
- Analytics health: `http://localhost:8001/health`
- Schema Registry: `http://localhost:8081`
- ClickHouse HTTP: `http://localhost:8123`
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- MinIO Console: `http://localhost:9001` (`minio` / `minio123`)
- MinIO API: `http://localhost:9002`
- PostgreSQL: `localhost:5433`

Ручной запуск агрегации и экспорта требует параметр даты:

```bash
curl -X POST "http://localhost:8001/aggregation/run?date=YYYY-MM-DD"
curl -X POST "http://localhost:8001/export/run?date=YYYY-MM-DD"
```

Через `Makefile`:

```bash
make agg                  # автоматически выберет последнюю дату <= сегодня (UTC)
make export               # автоматически выберет последнюю агрегированную дату <= сегодня
make agg DATE=YYYY-MM-DD
make export DATE=YYYY-MM-DD
make agg-latest
make export-latest
make grafana-sync         # принудительно перезагрузить дашборд в Grafana через API
make reset-data           # очистить raw+aggregates (ClickHouse/PostgreSQL)
```

Если дата указана без событий, `aggregation/run` вернёт пустой срез за эту дату и подсказку с доступными датами (`available_metric_dates`).
Если retention-панель в Grafana пустая после правок JSON, выполните `make grafana-sync` и обновите страницу.

## Что реализовано по пунктам

### 1. Kafka topic и схема

- Схема события описана в Avro: `src/common/schemas/movie_event.avsc`.
- `event_id` хранится как Avro logical type `uuid`, `event_type` и `device_type` заданы как `enum`.
- Schema Registry регистрирует subject `movie-events-value`, версия `1` создаётся автоматически init-контейнером `kafka-init`.
- Topic `movie-events` создаётся автоматически с `3` partition.
- Ключ партиционирования: `user_id`.
  Это сохраняет порядок пользовательской сессии, что особенно важно для последовательностей `VIEW_STARTED -> VIEW_PAUSED -> VIEW_RESUMED -> VIEW_FINISHED`, retention и DAU.

### 2. Продюсер

- `POST /events` принимает JSON, валидирует payload и публикует событие в Kafka.
- При необходимости `event_id` генерируется автоматически, затем сообщение валидируется и сериализуется по Avro-схеме.
- Используются `acks=all`, `enable.idempotence=true`, retry с exponential backoff и логирование публикаций.
- Включён генератор синтетических событий с реалистичными последовательностями просмотра и фоновым историческим seed для retention.

### 3. ClickHouse ingestion

- `analytics.movie_events_queue` — Kafka Engine table.
- `analytics.movie_events` — постоянное raw-хранилище на `MergeTree` с типами `UUID`, `Enum8`, `DateTime64`, `UInt32`.
- `analytics.movie_events_mv` — materialized view для автоматической перекладки из Kafka в MergeTree.
- Партиционирование raw-таблицы по месяцу, сортировка по `(event_date, user_id, event_timestamp, session_id, event_id)`.

### 4. Интеграционный тест pipeline

- Тест `tests/integration/test_pipeline.py` публикует события через HTTP producer.
- Проверяет появление события в ClickHouse.
- Затем проверяет ручной пересчёт агрегатов, upsert в PostgreSQL и экспорт в S3.
- Тест изолирован уникальной датой, чтобы не конфликтовать с потоковыми синтетическими событиями.

### 5. Aggregation Service и бизнес-метрики

- Отдельный контейнер `analytics-service`.
- Читает raw-события напрямую из ClickHouse.
- Автоматически применяет PostgreSQL migration при старте.
- По расписанию пересчитывает витрины в ClickHouse и идемпотентно пересинхронизирует данные в PostgreSQL.
- Есть ручной запуск пересчёта за дату: `POST /aggregation/run?date=YYYY-MM-DD`.
- Логирует начало и конец цикла, число обработанных raw-событий и длительность.

Вычисляются метрики:

- `dau`
- `avg_watch_time_seconds`
- `view_finish_conversion`
- `retention_d1`
- `retention_d7`
- `top_movies_daily`
- `device_distribution_daily`
- `retention_cohort_daily`

### 6. Grafana Dashboard

- Datasource для ClickHouse создаётся автоматически provisioning’ом.
- Загружается дашборд `Movie Analytics`.
- Есть обязательная cohort retention heatmap-панель (в виде cohort-таблицы с color background, Day 0..Day 7).
- Дополнительно есть панели `DAU`, `View Finish Conversion`, `Top Movies`, `Device Distribution`.

### 7. Экспорт в S3

- MinIO поднимается в `docker compose`.
- Экспорт идёт из PostgreSQL в JSON-файл.
- Ключ объекта: `s3://movie-analytics/daily/YYYY-MM-DD/aggregates.json`
- Повторный экспорт перезаписывает тот же объект.
- Есть ручной запуск: `POST /export/run?date=YYYY-MM-DD`

### 8. Kafka fault tolerance

- Поднят кластер из трёх Kafka broker/controller нод в KRaft.
- Topic создаётся с `replication.factor=2`, `min.insync.replicas=1`.
- Есть отдельный Schema Registry.
- Для всех long-running компонентов добавлены health checks, а one-shot init job'ы запускаются через `service_completed_successfully`.

## Структура

- `docker-compose.yml` — весь стенд.
- `src/producer` — producer API и генератор.
- `src/analytics` — aggregation/export service.
- `src/scripts` — автоматический bootstrap Kafka и ClickHouse.
- `infra/clickhouse/sql` — DDL ClickHouse.
- `infra/postgres/migrations` — PostgreSQL migrations.
- `infra/grafana` — provisioning и dashboard.
- `tests/integration` — интеграционные тесты.
