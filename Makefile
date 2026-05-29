COMPOSE := docker compose
DATE ?=
LATEST_RAW_DATE_QUERY := SELECT if(count()=0, '', toString(max(event_date))) FROM analytics.movie_events WHERE event_date <= today()
LATEST_METRIC_DATE_QUERY := SELECT COALESCE(to_char(max(metric_date), 'YYYY-MM-DD'), '') FROM metric_values WHERE metric_date <= CURRENT_DATE

.PHONY: up up-d down restart ps logs build test test-run agg agg-latest export export-latest smoke retention-check grafana-sync reset-data

up:
	$(COMPOSE) up --build

up-d:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down -v --remove-orphans

restart:
	$(COMPOSE) restart

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=200

build:
	$(COMPOSE) build

test:
	$(COMPOSE) --profile test up --build --abort-on-container-exit integration-test

test-run:
	$(COMPOSE) --profile test run --rm integration-test

agg:
	@TARGET_DATE="$(DATE)"; \
	if [ -z "$$TARGET_DATE" ]; then \
		TARGET_DATE=$$($(COMPOSE) exec -T clickhouse clickhouse-client --user analytics --password analytics --query "$(LATEST_RAW_DATE_QUERY)"); \
	fi; \
	if [ -z "$$TARGET_DATE" ]; then \
		echo "No raw events in ClickHouse yet. Start producer/generator and try again."; \
		exit 1; \
	fi; \
	echo "Using DATE=$$TARGET_DATE"; \
	curl -fsS -X POST "http://localhost:8001/aggregation/run?date=$$TARGET_DATE"; \
	echo

agg-latest:
	$(MAKE) agg

export:
	@TARGET_DATE="$(DATE)"; \
	if [ -z "$$TARGET_DATE" ]; then \
		TARGET_DATE=$$($(COMPOSE) exec -T postgres psql -U analytics -d analytics -t -A -c "$(LATEST_METRIC_DATE_QUERY)"); \
	fi; \
	if [ -z "$$TARGET_DATE" ]; then \
		echo "No aggregated dates in PostgreSQL yet. Run 'make agg' first."; \
		exit 1; \
	fi; \
	echo "Using DATE=$$TARGET_DATE"; \
	curl -fsS -X POST "http://localhost:8001/export/run?date=$$TARGET_DATE"; \
	echo

export-latest:
	$(MAKE) export

smoke:
	$(MAKE) ps
	curl -fsS http://localhost:8000/health
	curl -fsS http://localhost:8001/health
	$(MAKE) agg
	$(MAKE) export

retention-check:
	$(COMPOSE) exec -T clickhouse clickhouse-client --user analytics --password analytics --query "SELECT cohort_date, day_number, retention_rate, retained_users, cohort_size FROM analytics.retention_cohort_daily ORDER BY cohort_date DESC, day_number LIMIT 30"

grafana-sync:
	curl -fsS -u admin:admin -X POST "http://localhost:3000/api/admin/provisioning/dashboards/reload"; \
	echo

reset-data:
	$(COMPOSE) exec -T clickhouse clickhouse-client --user analytics --password analytics --query "TRUNCATE TABLE analytics.movie_events"
	$(COMPOSE) exec -T clickhouse clickhouse-client --user analytics --password analytics --query "TRUNCATE TABLE analytics.daily_metrics"
	$(COMPOSE) exec -T clickhouse clickhouse-client --user analytics --password analytics --query "TRUNCATE TABLE analytics.top_movies_daily"
	$(COMPOSE) exec -T clickhouse clickhouse-client --user analytics --password analytics --query "TRUNCATE TABLE analytics.retention_cohort_daily"
	$(COMPOSE) exec -T clickhouse clickhouse-client --user analytics --password analytics --query "TRUNCATE TABLE analytics.device_distribution_daily"
	$(COMPOSE) exec -T postgres psql -U analytics -d analytics -c "TRUNCATE TABLE metric_values, top_movies, retention_cohort, device_distribution"
	@echo "All raw and aggregate tables were cleared. Wait for generator, then run 'make agg'."
