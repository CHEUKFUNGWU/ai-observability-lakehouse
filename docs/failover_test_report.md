# Flink Failover Test Report

## Procedure

1. Start Postgres, Kafka, Flink JobManager, and Flink TaskManager.
2. Create Kafka ODS topics.
3. Submit the Flink SQL sequence.
4. Insert 100 source rows.
5. Stop `ai-observability-flink-taskmanager`.
6. Insert 50 more rows while Kafka buffers the CDC stream.
7. Restart the TaskManager.
8. Verify DWD row counts after recovery.

## Execution

Run `scripts/test_flink_failover.sh`.

## Expected Result

The recovered pipeline should process all 150 rows without loss.
