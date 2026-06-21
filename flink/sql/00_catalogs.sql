-- Flink 1.20 uses the native Paimon catalog for data I/O.
-- Gravitino separately registers this warehouse for metadata management because
-- the Gravitino 1.2.0 Flink connector runtime supports Flink 1.18, not 1.20.

CREATE CATALOG paimon_lake WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///workspace/data/paimon'
);

CREATE DATABASE IF NOT EXISTS paimon_lake.ods;
CREATE DATABASE IF NOT EXISTS paimon_lake.dwd;
CREATE DATABASE IF NOT EXISTS paimon_lake.dws;
CREATE DATABASE IF NOT EXISTS paimon_lake.dim;
CREATE DATABASE IF NOT EXISTS paimon_lake.ads;

USE CATALOG default_catalog;
USE default_database;
