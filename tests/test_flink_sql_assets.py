from pathlib import Path

from app.warehouse_contract import (
    render_agent_team_run_1d_flink_columns,
    render_agent_orchestration_flink_columns,
    render_agent_orchestration_flink_select,
    render_agent_orchestration_handoff_1d_flink_columns,
    render_agent_orchestration_where_clause,
    render_compliance_access_audit_flink_columns,
    render_compliance_access_audit_flink_select,
    render_compliance_access_audit_where_clause,
    render_cost_team_request_1d_flink_columns,
    render_compliance_data_retention_flink_columns,
    render_compliance_data_retention_flink_select,
    render_compliance_data_retention_where_clause,
    render_evaluation_feature_judgment_1d_flink_columns,
    render_evaluation_judgment_flink_columns,
    render_evaluation_judgment_flink_select,
    render_evaluation_judgment_where_clause,
    render_feedback_action_flink_columns,
    render_feedback_action_flink_select,
    render_feedback_action_where_clause,
    render_feedback_feature_action_1d_flink_columns,
    render_guardrail_check_flink_columns,
    render_guardrail_check_flink_select,
    render_guardrail_check_where_clause,
    render_guardrail_rule_check_1d_flink_columns,
    render_llm_feature_env_request_1d_flink_columns,
    render_llm_feature_request_1h_flink_columns,
    render_llm_request_flink_columns,
    render_llm_request_flink_select,
    render_llm_request_paimon_bootstrap,
    render_llm_request_where_clause,
    render_llm_region_request_1d_flink_columns,
    render_llm_session_request_1d_flink_columns,
    render_prompt_version_request_1d_flink_columns,
    render_retrieval_knowledge_base_request_1d_flink_columns,
    render_retrieval_request_flink_columns,
    render_retrieval_request_flink_select,
    render_retrieval_request_where_clause,
    render_platform_component_health_1d_flink_columns,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
FLINK_SQL_DIR = REPO_ROOT / "flink" / "sql"


EXPECTED_FLINK_SQL_FILES = [
    "00_catalogs.sql",
    "00_catalogs_standalone.sql",
    "01_source_postgres_cdc.sql",
    "02_ods_kafka_tables.sql",
    "03_dwd_paimon_tables.sql",
    "04_dws_paimon_tables.sql",
    "10_ingest_ods_to_kafka.sql",
    "20_build_dwd_from_kafka_ods.sql",
    "30_build_dws_from_dwd.sql",
    "91_verify_dwd_count.sql",
    "92_verify_dws_metrics.sql",
]


def read_asset(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_flink_sql_assets_exist_in_expected_order():
    actual_files = [path.name for path in sorted(FLINK_SQL_DIR.glob("*.sql"))]

    assert actual_files == EXPECTED_FLINK_SQL_FILES


def test_catalog_sql_defines_paimon_lake():
    sql = read_asset("flink/sql/00_catalogs.sql")

    assert "CREATE CATALOG paimon_lake" in sql
    assert "'type' = 'paimon'" in sql
    assert "'warehouse' = 'file:///workspace/data/paimon'" in sql
    assert "'type' = 'gravitino'" not in sql
    assert "CREATE DATABASE IF NOT EXISTS paimon_lake.ods" in sql
    assert "CREATE DATABASE IF NOT EXISTS paimon_lake.dwd" in sql
    assert "CREATE DATABASE IF NOT EXISTS paimon_lake.dws" in sql
    assert "CREATE DATABASE IF NOT EXISTS paimon_lake.dim" in sql
    assert "CREATE DATABASE IF NOT EXISTS paimon_lake.ads" in sql


def test_standalone_catalog_sql_uses_direct_paimon():
    sql = read_asset("flink/sql/00_catalogs_standalone.sql")

    assert "CREATE CATALOG paimon_lake" in sql
    assert "'type' = 'paimon'" in sql
    assert "'warehouse' = 'file:///workspace/data/paimon'" in sql
    assert "CREATE DATABASE IF NOT EXISTS paimon_lake.ods" in sql


def test_source_sql_uses_postgres_cdc_connector():
    sql = read_asset("flink/sql/01_source_postgres_cdc.sql")

    assert "CREATE TABLE IF NOT EXISTS src_llm_request_events" in sql
    assert "'connector' = 'postgres-cdc'" in sql
    assert "'database-name' = 'ai_observability'" in sql
    assert "'table-name' = 'llm_request_events'" in sql
    assert "'decoding.plugin.name' = 'pgoutput'" in sql
    assert "PRIMARY KEY (request_id) NOT ENFORCED" in sql


def test_paimon_layers_use_expected_tables():
    ods_sql = read_asset("flink/sql/02_ods_kafka_tables.sql")
    dwd_sql = read_asset("flink/sql/03_dwd_paimon_tables.sql")
    dws_sql = read_asset("flink/sql/04_dws_paimon_tables.sql")

    assert "ods_ai_observability_llm_request_events_di" in ods_sql
    assert "ods_ai_observability_retrieval_events_di" in ods_sql
    assert "ods_ai_observability_feedback_events_di" in ods_sql
    assert "ods_ai_observability_guardrail_events_di" in ods_sql
    assert "ods_ai_observability_evaluation_events_di" in ods_sql
    assert "ods_ai_observability_model_deployment_events_di" in ods_sql
    assert "ods_ai_observability_compliance_access_audit_events_di" in ods_sql
    assert "ods_ai_observability_compliance_data_retention_events_di" in ods_sql
    assert "ods_ai_observability_agent_orchestration_events_di" in ods_sql
    assert "ods_ai_observability_platform_health_metrics_di" in ods_sql
    assert "'connector' = 'upsert-kafka'" in ods_sql
    assert "PRIMARY KEY (request_id) NOT ENFORCED" in ods_sql
    assert "paimon_lake.dwd.dwd_ai_llm_request_di" in dwd_sql
    assert "paimon_lake.dwd.dwd_ai_retrieval_request_di" in dwd_sql
    assert render_retrieval_request_flink_columns() in dwd_sql
    assert "paimon_lake.dwd.dwd_ai_feedback_action_di" in dwd_sql
    assert render_feedback_action_flink_columns() in dwd_sql
    assert "paimon_lake.dwd.dwd_ai_guardrail_check_di" in dwd_sql
    assert render_guardrail_check_flink_columns() in dwd_sql
    assert "paimon_lake.dwd.dwd_ai_evaluation_judgment_di" in dwd_sql
    assert render_evaluation_judgment_flink_columns() in dwd_sql
    assert "paimon_lake.dwd.dwd_ai_model_deployment_di" in dwd_sql
    assert "paimon_lake.dwd.dwd_ai_compliance_access_audit_di" in dwd_sql
    assert render_compliance_access_audit_flink_columns() in dwd_sql
    assert "paimon_lake.dwd.dwd_ai_compliance_data_retention_di" in dwd_sql
    assert render_compliance_data_retention_flink_columns() in dwd_sql
    assert "paimon_lake.dwd.dwd_ai_agent_orchestration_di" in dwd_sql
    assert render_agent_orchestration_flink_columns() in dwd_sql
    assert "WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND" in dwd_sql
    assert render_llm_request_flink_columns() in dwd_sql
    assert "paimon_lake.dws.dws_ai_llm_feature_request_1d" in dws_sql
    assert "paimon_lake.dws.dws_ai_llm_feature_request_1h" in dws_sql
    assert "paimon_lake.dws.dws_ai_llm_session_request_1d" in dws_sql
    assert "paimon_lake.dws.dws_ai_retrieval_knowledge_base_request_1d" in dws_sql
    assert render_retrieval_knowledge_base_request_1d_flink_columns() in dws_sql
    assert "paimon_lake.dws.dws_ai_feedback_feature_action_1d" in dws_sql
    assert render_feedback_feature_action_1d_flink_columns() in dws_sql
    assert "paimon_lake.dws.dws_ai_guardrail_rule_check_1d" in dws_sql
    assert render_guardrail_rule_check_1d_flink_columns() in dws_sql
    assert "paimon_lake.dws.dws_ai_cost_team_request_1d" in dws_sql
    assert render_cost_team_request_1d_flink_columns() in dws_sql
    assert "paimon_lake.dws.dws_ai_evaluation_feature_judgment_1d" in dws_sql
    assert render_evaluation_feature_judgment_1d_flink_columns() in dws_sql
    assert "paimon_lake.dws.dws_ai_prompt_version_request_1d" in dws_sql
    assert render_prompt_version_request_1d_flink_columns() in dws_sql
    assert "paimon_lake.dws.dws_ai_llm_feature_env_request_1d" in dws_sql
    assert render_llm_feature_env_request_1d_flink_columns() in dws_sql
    assert "paimon_lake.dws.dws_ai_llm_region_request_1d" in dws_sql
    assert render_llm_region_request_1d_flink_columns() in dws_sql
    assert "paimon_lake.dws.dws_ai_agent_team_run_1d" in dws_sql
    assert render_agent_team_run_1d_flink_columns() in dws_sql
    assert "paimon_lake.dws.dws_ai_agent_orchestration_handoff_1d" in dws_sql
    assert render_agent_orchestration_handoff_1d_flink_columns() in dws_sql
    assert "paimon_lake.dws.dws_ai_platform_component_health_1d" in dws_sql
    assert render_platform_component_health_1d_flink_columns() in dws_sql
    assert render_llm_feature_request_1h_flink_columns() in dws_sql
    assert render_llm_session_request_1d_flink_columns() in dws_sql
    assert "PRIMARY KEY (request_id) NOT ENFORCED" in dwd_sql
    assert "PARTITIONED BY (`date`)" in dws_sql


def test_flink_prompt_version_dws_reuses_existing_prompt_table_and_score_numerators():
    dws_sql = read_asset("flink/sql/30_build_dws_from_dwd.sql")
    prompt_version_insert = dws_sql.split(
        "INSERT INTO paimon_lake.dws.dws_ai_prompt_version_request_1d", 1
    )[1].split("INSERT INTO", 1)[0]

    assert "INSERT INTO paimon_lake.dws.dws_ai_prompt_version_request_1d" in dws_sql
    assert "unique_request_prompt_keys" in dws_sql
    assert "evaluation_score_num_1d" in dws_sql
    assert "evaluation_score_den_1d" in dws_sql
    assert "metadata_conflict_cnt_1d" in dws_sql
    assert "CAST(0 AS BIGINT) AS p95_latency_ms" in prompt_version_insert
    assert "CAST(MAX(latency_ms) AS BIGINT) AS p95_latency_ms" not in prompt_version_insert


def test_flink_sql_layer_dependencies_are_explicit():
    ingest_sql = read_asset("flink/sql/10_ingest_ods_to_kafka.sql")
    dwd_sql = read_asset("flink/sql/20_build_dwd_from_kafka_ods.sql")
    dws_sql = read_asset("flink/sql/30_build_dws_from_dwd.sql")

    assert "INSERT INTO ods_ai_observability_llm_request_events_di" in ingest_sql
    assert "FROM src_llm_request_events" in ingest_sql
    assert "INSERT INTO paimon_lake.dwd.dwd_ai_llm_request_di" in dwd_sql
    assert "INSERT INTO paimon_lake.dwd.dwd_ai_retrieval_request_di" in dwd_sql
    assert "INSERT INTO paimon_lake.dwd.dwd_ai_feedback_action_di" in dwd_sql
    assert render_feedback_action_flink_select() in dwd_sql
    assert render_feedback_action_where_clause() in dwd_sql
    assert "INSERT INTO paimon_lake.dwd.dwd_ai_guardrail_check_di" in dwd_sql
    assert render_guardrail_check_flink_select() in dwd_sql
    assert render_guardrail_check_where_clause() in dwd_sql
    assert "INSERT INTO paimon_lake.dwd.dwd_ai_evaluation_judgment_di" in dwd_sql
    assert render_evaluation_judgment_flink_select() in dwd_sql
    assert render_evaluation_judgment_where_clause() in dwd_sql
    assert "INSERT INTO paimon_lake.dwd.dwd_ai_model_deployment_di" in dwd_sql
    assert "INSERT INTO paimon_lake.dwd.dwd_ai_compliance_access_audit_di" in dwd_sql
    assert render_compliance_access_audit_flink_select() in dwd_sql
    assert render_compliance_access_audit_where_clause() in dwd_sql
    assert "INSERT INTO paimon_lake.dwd.dwd_ai_compliance_data_retention_di" in dwd_sql
    assert render_compliance_data_retention_flink_select() in dwd_sql
    assert render_compliance_data_retention_where_clause() in dwd_sql
    assert "INSERT INTO paimon_lake.dwd.dwd_ai_agent_orchestration_di" in dwd_sql
    assert render_agent_orchestration_flink_select() in dwd_sql
    assert render_agent_orchestration_where_clause() in dwd_sql
    assert "FROM ods_ai_observability_llm_request_events_di" in dwd_sql
    assert render_llm_request_flink_select() in dwd_sql
    assert render_llm_request_where_clause() in dwd_sql
    assert "FROM ods_ai_observability_retrieval_events_di" in dwd_sql
    assert render_retrieval_request_flink_select() in dwd_sql
    assert render_retrieval_request_where_clause() in dwd_sql
    assert "FROM ods_ai_observability_feedback_events_di" in dwd_sql
    assert "FROM ods_ai_observability_guardrail_events_di" in dwd_sql
    assert "FROM ods_ai_observability_evaluation_events_di" in dwd_sql
    assert "FROM ods_ai_observability_model_deployment_events_di" in dwd_sql
    assert "FROM ods_ai_observability_compliance_access_audit_events_di" in dwd_sql
    assert "FROM ods_ai_observability_compliance_data_retention_events_di" in dwd_sql
    assert "FROM ods_ai_observability_agent_orchestration_events_di" in dwd_sql
    assert "CHAR_LENGTH(ip_address) = 64" in dwd_sql
    assert "request_id IS NOT NULL" in dwd_sql
    assert "created_at IS NOT NULL" in dwd_sql
    assert "prompt_tokens >= 0" in dwd_sql
    assert "completion_tokens >= 0" in dwd_sql
    assert "total_tokens = prompt_tokens + completion_tokens" in dwd_sql
    assert "latency_ms > 0" in dwd_sql
    assert "status IN ('success', 'error')" in dwd_sql
    assert "estimated_cost_usd >= 0" in dwd_sql
    assert "mode IN ('mock', 'live', 'replay', 'hermes')" in dwd_sql
    assert "INSERT INTO paimon_lake.dws.dws_ai_llm_feature_request_1d" in dws_sql
    assert "INSERT INTO paimon_lake.dws.dws_ai_llm_feature_request_1h" in dws_sql
    assert "INSERT INTO paimon_lake.dws.dws_ai_llm_session_request_1d" in dws_sql
    assert "INSERT INTO paimon_lake.dws.dws_ai_retrieval_knowledge_base_request_1d" in dws_sql
    assert "INSERT INTO paimon_lake.dws.dws_ai_feedback_feature_action_1d" in dws_sql
    assert "INSERT INTO paimon_lake.dws.dws_ai_guardrail_rule_check_1d" in dws_sql
    assert "INSERT INTO paimon_lake.dws.dws_ai_evaluation_feature_judgment_1d" in dws_sql
    assert "AVG(score) AS avg_score" in dws_sql
    assert "MIN(score) AS p10_score" in dws_sql
    assert "INSERT INTO paimon_lake.dws.dws_ai_llm_feature_env_request_1d" in dws_sql
    assert "INSERT INTO paimon_lake.dws.dws_ai_llm_region_request_1d" in dws_sql
    assert "INSERT INTO paimon_lake.dws.dws_ai_agent_orchestration_handoff_1d" in dws_sql
    assert "CAST(MAX(handoff_latency_ms) AS BIGINT) AS p95_handoff_latency_ms" in dws_sql
    assert "INSERT INTO paimon_lake.dws.dws_ai_platform_component_health_1d" in dws_sql
    assert "MAX(metric_value) > MAX(threshold) AS is_breach" in dws_sql
    assert "FROM paimon_lake.dwd.dwd_ai_llm_request_di" in dws_sql
    assert "FROM paimon_lake.dws.dws_ai_llm_feature_request_1h" in dws_sql
    assert "TUMBLE(" in dws_sql
    assert "INTERVAL '1' HOUR" in dws_sql
    assert "FROM paimon_lake.dwd.dwd_ai_feedback_action_di" in dws_sql
    assert "CAST(MAX(latency_ms) AS BIGINT) AS max_latency_ms" in dws_sql
    assert "CAST(0 AS BIGINT) AS p95_latency_ms" in dws_sql
    assert "PERCENTILE_CONT(" not in dws_sql


def test_flink_verify_sql_covers_dwd_and_dws_layers():
    dwd_sql = read_asset("flink/sql/91_verify_dwd_count.sql")
    dws_sql = read_asset("flink/sql/92_verify_dws_metrics.sql")

    assert "SET 'execution.runtime-mode' = 'batch'" in dwd_sql
    assert "COUNT(*) AS dwd_row_count" in dwd_sql
    assert "COUNT(*) AS dwd_retrieval_row_count" in dwd_sql
    assert "COUNT(*) AS dwd_feedback_row_count" in dwd_sql
    assert "COUNT(*) AS dwd_guardrail_row_count" in dwd_sql
    assert "COUNT(*) AS dwd_evaluation_row_count" in dwd_sql
    assert "COUNT(*) AS dwd_model_deployment_row_count" in dwd_sql
    assert "COUNT(*) AS dwd_compliance_access_audit_row_count" in dwd_sql
    assert "COUNT(*) AS dwd_compliance_data_retention_row_count" in dwd_sql
    assert "COUNT(*) AS dwd_agent_orchestration_row_count" in dwd_sql
    assert "FROM paimon_lake.dwd.dwd_ai_llm_request_di" in dwd_sql
    assert "SET 'execution.runtime-mode' = 'batch'" in dws_sql
    assert "COUNT(*) AS dws_metric_rows" in dws_sql
    assert "COUNT(*) AS dws_retrieval_metric_rows" in dws_sql
    assert "COUNT(*) AS dws_feedback_metric_rows" in dws_sql
    assert "COUNT(*) AS dws_guardrail_metric_rows" in dws_sql
    assert "COUNT(*) AS dws_cost_team_metric_rows" in dws_sql
    assert "COUNT(*) AS dws_evaluation_metric_rows" in dws_sql
    assert "COUNT(*) AS dws_prompt_version_metric_rows" in dws_sql
    assert "COUNT(*) AS dws_llm_feature_env_metric_rows" in dws_sql
    assert "COUNT(*) AS dws_llm_region_metric_rows" in dws_sql
    assert "COUNT(*) AS dws_agent_team_metric_rows" in dws_sql
    assert "COUNT(*) AS dws_llm_feature_hourly_metric_rows" in dws_sql
    assert "COUNT(*) AS dws_llm_session_metric_rows" in dws_sql
    assert "COUNT(*) AS dws_agent_orchestration_metric_rows" in dws_sql
    assert "COUNT(*) AS dws_platform_health_metric_rows" in dws_sql
    assert "SUM(request_count) AS total_request_count" in dws_sql
    assert "FROM paimon_lake.dws.dws_ai_llm_feature_request_1d" in dws_sql


def test_spark_paimon_table_bootstrap_matches_flink_keyed_tables():
    script = read_asset("scripts/spark_paimon_backfill.py")

    assert "'primary-key' = 'request_id'" in script
    assert "'primary-key' = 'date,app_name,feature_name,model_name'" in script
    assert "'bucket' = '-1'" in script
    assert "'bucket' = '4'" in script
    assert "render_llm_request_paimon_bootstrap" in script
    assert render_llm_request_paimon_bootstrap() not in script


def test_postgres_source_schema_matches_cdc_source_table():
    postgres_sql = read_asset("sql/source_postgres_schema.sql")

    assert "CREATE TABLE IF NOT EXISTS llm_request_events" in postgres_sql
    assert "request_id TEXT PRIMARY KEY" in postgres_sql
    assert "date DATE NOT NULL" in postgres_sql
    assert "idx_llm_request_events_date" in postgres_sql


def test_flink_dockerfile_installs_paimon_postgres_and_kafka_connectors():
    dockerfile = read_asset("docker/flink/Dockerfile")

    assert "FROM flink:${FLINK_VERSION}-scala_2.12-java17" in dockerfile
    assert "ARG PAIMON_VERSION=1.2.0" in dockerfile
    assert "ARG FLINK_CDC_VERSION=3.2.1" in dockerfile
    assert "ARG FLINK_SHADED_HADOOP_VERSION=2.8.3-10.0" in dockerfile
    assert "ARG FLINK_KAFKA_VERSION=3.3.0-1.20" in dockerfile
    assert "paimon-flink-1.20" in dockerfile
    assert "flink-sql-connector-postgres-cdc" in dockerfile
    assert "flink-sql-connector-kafka" in dockerfile
    assert "flink-shaded-hadoop-2-uber" in dockerfile
    assert "gravitino-flink-connector-runtime-1.18_2.12" not in dockerfile
    assert "/opt/flink/lib" in dockerfile


def test_compose_defines_stream_batch_runtime_services():
    compose = read_asset("docker-compose.yml")

    assert "kafka:" in compose
    assert "gravitino:" in compose
    assert "apache/gravitino:1.2.0" in compose
    assert "8090:8090" in compose
    assert "flink-jobmanager:" in compose
    assert "flink-taskmanager:" in compose
    assert "flink-sql-client:" in compose
    assert "dockerfile: docker/flink/Dockerfile" in compose
    assert "user: root" in compose
    assert "paimon_warehouse:" in compose
    assert "./flink:/workspace/flink:ro" in compose
    assert "- paimon_warehouse:/workspace/data/paimon:ro" in compose
    assert "execution.checkpointing.interval: 10s" in compose
    assert "execution.checkpointing.mode: EXACTLY_ONCE" in compose
    assert "execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION" in compose
    assert "restart-strategy.type: fixed-delay" in compose
    assert "restart-strategy.fixed-delay.attempts: 3" in compose
    assert "taskmanager.numberOfTaskSlots: 4" in compose
    assert "state.checkpoints.dir: file:///workspace/data/paimon/_checkpoints" in compose
    assert "state.savepoints.dir: file:///workspace/data/paimon/_savepoints" in compose
    assert "wal_level=logical" in compose
    assert "max_replication_slots=10" in compose
    assert "KAFKA_PROCESS_ROLES: broker,controller" in compose
    assert "CLUSTER_ID: 'ai-observability-kafka-cluster-001'" in compose


def test_flink_sql_runner_uses_dedicated_sql_client_service():
    script = read_asset("scripts/run_flink_sql_file.sh")

    assert "scripts/prepare_flink_warehouse.sh" in script
    assert "docker compose run -T --rm flink-sql-client" in script
    assert "/opt/flink/bin/sql-client.sh" in script
    assert "-f \"/workspace/${sql_file}\"" in script


def test_flink_sql_sequence_runner_keeps_catalog_in_one_session():
    script = read_asset("scripts/run_flink_sql_sequence.sh")

    assert "flink/sql/.generated_sequence.sql" in script
    assert "cat \"${sql_file}\"" in script
    assert "scripts/prepare_flink_warehouse.sh" in script
    assert "docker compose run -T --rm flink-sql-client" in script
    assert "/opt/flink/bin/sql-client.sh" in script
    assert "-f \"/workspace/${tmp_file}\"" in script
    assert "PIPESTATUS[0]" in script
    assert "\\[ERROR\\]" in script


def test_dws_session_duration_uses_a_flink_supported_timestampdiff_unit():
    sql = read_asset("flink/sql/30_build_dws_from_dwd.sql")

    assert "TIMESTAMPDIFF(MILLISECOND" not in sql
    assert "TIMESTAMPDIFF(SECOND, MIN(created_at), MAX(created_at)) * 1000" in sql


def test_flink_savepoint_restore_helpers_are_available():
    savepoint_script = read_asset("scripts/flink_savepoint.sh")
    cancel_script = read_asset("scripts/flink_cancel_job.sh")
    restore_script = read_asset("scripts/run_flink_sql_from_savepoint.sh")

    assert "/opt/flink/bin/flink savepoint" in savepoint_script
    assert "file:///workspace/data/paimon/_savepoints" in savepoint_script
    assert "/opt/flink/bin/flink cancel" in cancel_script
    assert "SET 'execution.savepoint.path'" in restore_script
    assert "docker compose run -T --rm flink-sql-client" in restore_script


def test_light_and_serving_demo_commands_are_split():
    makefile = read_asset("Makefile")
    streaming_demo = read_asset("scripts/run_streaming_demo.sh")
    serving_demo = read_asset("scripts/run_serving_demo.sh")
    health_script = read_asset("scripts/check_pipeline_health.sh")

    assert "infra-light:" in makefile
    assert "infra-serving:" in makefile
    assert "demo-streaming:" in makefile
    assert "demo-serving:" in makefile
    assert "scripts/run_streaming_demo.sh" in read_asset("scripts/run_full_demo.sh")
    assert "doris-fe" not in streaming_demo
    assert "make" not in streaming_demo
    assert "docker compose up -d doris-fe doris-be doris-init" in serving_demo
    assert "ods_ai_observability_llm_request_events_di" in health_script
    assert "ods_ai_observability_retrieval_events_di" in health_script
    assert "ods_ai_observability_feedback_events_di" in health_script
    assert "ods_ai_observability_guardrail_events_di" in health_script
    assert "ods_ai_observability_evaluation_events_di" in health_script
    assert "ods_ai_observability_model_deployment_events_di" in health_script
    assert "ods_ai_observability_compliance_access_audit_events_di" in health_script
    assert "ods_ai_observability_compliance_data_retention_events_di" in health_script
    assert "ods_ai_observability_agent_orchestration_events_di" in health_script
    assert "ods_ai_observability_platform_health_metrics_di" in health_script
    assert "insert-into_paimon_lake.dwd.dwd_ai_llm_request_di" in health_script
    assert "insert-into_paimon_lake.dwd.dwd_ai_retrieval_request_di" in health_script
    assert "insert-into_paimon_lake.dwd.dwd_ai_feedback_action_di" in health_script
    assert "insert-into_paimon_lake.dwd.dwd_ai_guardrail_check_di" in health_script
    assert "insert-into_paimon_lake.dwd.dwd_ai_evaluation_judgment_di" in health_script
    assert "insert-into_paimon_lake.dwd.dwd_ai_model_deployment_di" in health_script
    assert "insert-into_paimon_lake.dwd.dwd_ai_compliance_access_audit_di" in health_script
    assert "insert-into_paimon_lake.dwd.dwd_ai_compliance_data_retention_di" in health_script
    assert "insert-into_paimon_lake.dwd.dwd_ai_agent_orchestration_di" in health_script
    assert "insert-into_paimon_lake.dws.dws_ai_llm_feature_request_1d" in health_script
    assert "insert-into_paimon_lake.dws.dws_ai_retrieval_knowledge_base_request_1d" in health_script
    assert "insert-into_paimon_lake.dws.dws_ai_feedback_feature_action_1d" in health_script
    assert "insert-into_paimon_lake.dws.dws_ai_guardrail_rule_check_1d" in health_script
    assert "insert-into_paimon_lake.dws.dws_ai_evaluation_feature_judgment_1d" in health_script
    assert "insert-into_paimon_lake.dws.dws_ai_llm_feature_request_1h" in health_script
    assert "insert-into_paimon_lake.dws.dws_ai_llm_session_request_1d" in health_script
    assert "insert-into_paimon_lake.dws.dws_ai_agent_orchestration_handoff_1d" in health_script
    assert "insert-into_paimon_lake.dws.dws_ai_platform_component_health_1d" in health_script


def test_flink_warehouse_prepare_script_creates_checkpoint_dirs():
    script = read_asset("scripts/prepare_flink_warehouse.sh")

    assert "docker compose exec -T flink-jobmanager" in script
    assert "mkdir -p /workspace/data/paimon/_checkpoints" in script
    assert "/workspace/data/paimon/_savepoints" in script
    assert "chown -R flink:flink /workspace/data/paimon" in script
    assert "chmod -R u+rwX,g+rwX /workspace/data/paimon" in script


def test_postgres_source_loader_uses_copy_from_exporter():
    script = read_asset("scripts/load_llm_jsonl_to_postgres_source.sh")

    assert "scripts.export_llm_jsonl_to_postgres_copy" in script
    assert "docker compose exec -T postgres" in script
    assert "\\copy llm_request_events" in script
    assert "created_at,date" in script
