# hw7-SOA

## Что добавлено в ДЗ7

- CI/CD pipeline в `.github/workflows/ci.yml`: `build → unit tests → integration tests → E2E tests → load tests`.
- Метрики `/metrics` для `producer` и `analytics-service`.
- Prometheus, Grafana и Alertmanager в `docker-compose.yml`.
- Дашборды Grafana для каждого сервиса и отдельный инфраструктурный дашборд.
- Нагрузочный тест на `k6` и проверка SLI из Prometheus в CI.
- Интеграционные и E2E тесты с очисткой тестового состояния.

## Ветки

- `hw7-base` — базовая версия без доработок ДЗ7.
- `hw7-SOA` — текущая ветка с ДЗ7.

Сравнить изменения можно командой `git diff hw7-base..hw7-SOA`.

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
make test      # полный E2E тест
make unit      # unit tests
make integration
make e2e
make load
make collect-metrics
make check-sli
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
- `prometheus` — сбор сервисных и инфраструктурных метрик.
- `alertmanager` — обработка и показ алертов.

## Эндпоинты

- Producer API: `http://localhost:8000/events`
- Producer health: `http://localhost:8000/health`
- Analytics API: `http://localhost:8001/aggregation/run?date=YYYY-MM-DD`
- Export API: `http://localhost:8001/export/run?date=YYYY-MM-DD`
- Analytics health: `http://localhost:8001/health`
- Schema Registry: `http://localhost:8081`
- ClickHouse HTTP: `http://localhost:8123`
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- Prometheus: `http://localhost:9090`
- Alertmanager: `http://localhost:9093`
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
- После проверки тест удаляет тестовые события, агрегаты и объект в MinIO.

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

## ДЗ7: CI/CD, метрики и наблюдаемость

### CI pipeline

- Триггерится на `push` и `pull_request`.
- Шаги: `build`, `unit tests`, `integration tests`, `E2E tests`, `load tests`.
- Пайплайн падает при любой ошибке.
- Логи compose-сценариев сохраняются в артефакты GitHub Actions.

### Метрики

Оба сервиса экспортируют `/metrics` с:

- `http_requests_total{method,endpoint,status}`
- `http_request_errors_total{method,endpoint,error_type}`
- `http_request_duration_seconds_bucket{method,endpoint}`

Prometheus скрейпит:

- `producer`
- `analytics-service`
- `clickhouse-exporter`
- `postgres-exporter`
- `kafka-exporter`

### Grafana

Есть три дашборда:

- `Producer Service`
- `Analytics Service`
- `Infrastructure`

Видны:

- p50 / p95 / p99 latency
- error rate
- throughput
- Kafka / PostgreSQL / ClickHouse bottlenecks

### Нагрузочное тестирование

- Инструмент: `k6`
- Сценарий: `10 VU`, `30s`
- Проверки: `checks` на `GET /health` и `POST /events` должны быть > 99%, `p95 < 500ms` на `POST /events`
- Результат `k6` сохраняется в `artifacts/load/summary.json`

### Алерты

Правила лежат в `infra/prometheus/alerts.yml`:

- высокий error rate
- высокая latency
- недоступность сервиса
- Kafka consumer lag

Alertmanager поднимается вместе со стендом.

### SLI / SLO

Проверки выполняются скриптом `scripts/ci/check_prometheus_sli.py`:

- `producer_error_rate` — SLO `<= 1%`, порог отказа `> 1%`
- `producer_p95_latency_seconds` — SLO `<= 500ms`, порог отказа `> 500ms`
- `producer_availability` — SLO `>= 99%`, порог отказа `< 99%`
- `analytics_p95_latency_seconds` — SLO `<= 1s`, порог отказа `> 1s`
