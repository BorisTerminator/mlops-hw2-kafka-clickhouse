-- Сравнение базовой и оптимизированной таблиц.

-- 1. Размер на диске (сжатый/несжатый) и число строк.
SELECT
    table,
    sum(rows)                                        AS rows,
    formatReadableSize(sum(data_compressed_bytes))   AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 1) AS ratio
FROM system.parts
WHERE active AND database = 'default' AND table IN ('transactions_raw', 'transactions')
GROUP BY table
ORDER BY table;

-- 2. Размер по колонкам: видно, что дали LowCardinality, Decimal32 и кодеки.
SELECT
    name,
    formatReadableSize(sumIf(data_compressed_bytes, table = 'transactions_raw')) AS raw,
    formatReadableSize(sumIf(data_compressed_bytes, table = 'transactions'))     AS optimized
FROM system.columns
WHERE database = 'default' AND table IN ('transactions_raw', 'transactions')
GROUP BY name
ORDER BY sumIf(data_compressed_bytes, table = 'transactions_raw') DESC;

-- 3. Время и объём чтения последних запросов к обеим таблицам
--    (выполнять после прогона запросов, см. README).
SYSTEM FLUSH LOGS;
SELECT
    if(query LIKE '%transactions_raw%', 'transactions_raw', 'transactions') AS table,
    count()                                AS runs,
    round(avg(query_duration_ms), 1)       AS avg_ms,
    any(read_rows)                         AS read_rows,
    formatReadableSize(any(read_bytes))    AS read_bytes,
    any(projections)                       AS used_projections
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_kind = 'Select'
  AND query LIKE '%argMax(cat_id, amount)%'
  AND query NOT LIKE '%system.query_log%'
GROUP BY table
ORDER BY table;
