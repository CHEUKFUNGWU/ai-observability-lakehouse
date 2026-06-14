CREATE CATALOG IF NOT EXISTS paimon_lake PROPERTIES (
    'type' = 'paimon',
    'warehouse' = 'file:///workspace/data/paimon'
);
