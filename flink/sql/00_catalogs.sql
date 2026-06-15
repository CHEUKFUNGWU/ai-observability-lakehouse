-- Flink SQL catalog bootstrap for the stream-batch lakehouse.

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
