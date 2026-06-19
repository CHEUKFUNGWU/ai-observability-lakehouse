.PHONY: test lint pipeline infra-light infra-serving infra-dashboard infra-stop init-superset dashboard-stop seed-data flink-up flink-submit flink-jobs flink-savepoint flink-cancel flink-restore batch-backfill sync-doris health demo demo-streaming demo-serving clean

test:
	uv run pytest -v

lint:
	uv run ruff check .

pipeline:
	uv run python -m scripts.spark_paimon_backfill --input data/raw/mock_llm_requests/events.jsonl

infra-light:
	docker compose up -d postgres kafka flink-jobmanager flink-taskmanager
	scripts/prepare_flink_warehouse.sh
	scripts/create_kafka_topics.sh

infra-serving:
	docker compose up -d doris-fe doris-be doris-init

infra-dashboard:
	docker compose up -d superset-metadata superset-redis superset grafana

init-superset:
	scripts/init_superset.sh

dashboard-stop:
	docker compose stop superset superset-metadata superset-redis grafana

infra-stop:
	docker compose stop

seed-data:
	uv run python -m scripts.generate_mock_llm_logs --count 100 --seed 42
	uv run python -m scripts.generate_mock_agent_logs --count 100 --seed 42
	uv run python -m scripts.generate_mock_compliance_logs --count 100 --seed 42
	uv run python -m scripts.generate_mock_orchestration_logs --count 100 --seed 42
	uv run python -m scripts.generate_mock_platform_health_logs --sample-count 12 --seed 42
	scripts/load_llm_jsonl_to_postgres_source.sh data/raw/mock_llm_requests/events.jsonl

flink-up:
	$(MAKE) infra-light

flink-submit:
	scripts/run_flink_sql_sequence.sh \
	  flink/sql/00_catalogs.sql \
	  flink/sql/01_source_postgres_cdc.sql \
	  flink/sql/02_ods_kafka_tables.sql \
	  flink/sql/03_dwd_paimon_tables.sql \
	  flink/sql/04_dws_paimon_tables.sql \
	  flink/sql/10_ingest_ods_to_kafka.sql \
	  flink/sql/20_build_dwd_from_kafka_ods.sql \
	  flink/sql/30_build_dws_from_dwd.sql

flink-jobs:
	curl -s http://localhost:8081/jobs/overview

flink-savepoint:
	test -n "$(JOB_ID)"
	scripts/flink_savepoint.sh "$(JOB_ID)"

flink-cancel:
	test -n "$(JOB_ID)"
	scripts/flink_cancel_job.sh "$(JOB_ID)"

flink-restore:
	test -n "$(SAVEPOINT)"
	test -n "$(SQL_FILES)"
	scripts/run_flink_sql_from_savepoint.sh "$(SAVEPOINT)" $(SQL_FILES)

batch-backfill:
	uv run python -m scripts.spark_build_ads_cost_anomaly
	uv run python -m scripts.spark_build_ads_prompt_version_metrics
	uv run python -m scripts.spark_build_dim_model

sync-doris:
	docker compose exec -T doris-fe mysql -h 127.0.0.1 -P 9030 -u root < sql/create_doris_tables.sql
	docker compose exec -T doris-fe mysql -h 127.0.0.1 -P 9030 -u root < sql/doris_create_paimon_catalog.sql
	docker compose exec -T doris-fe mysql -h 127.0.0.1 -P 9030 -u root < sql/doris_sync_paimon_dws.sql
	uv run python -m scripts.load_dws_metrics_to_doris

health:
	scripts/check_pipeline_health.sh

demo:
	scripts/run_full_demo.sh

demo-streaming:
	scripts/run_streaming_demo.sh

demo-serving:
	scripts/run_serving_demo.sh

clean:
	rm -rf data/
