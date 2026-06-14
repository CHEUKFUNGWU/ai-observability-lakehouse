.PHONY: test pipeline flink-up demo clean lint

test:
	uv run pytest -v

lint:
	uv run ruff check .

pipeline:
	uv run python -m scripts.spark_paimon_backfill --input data/raw/mock_llm_requests/events.jsonl

flink-up:
	docker compose up -d postgres kafka flink-jobmanager flink-taskmanager

demo:
	scripts/run_full_demo.sh

clean:
	rm -rf data/
