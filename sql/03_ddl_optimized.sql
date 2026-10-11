-- П.4. Оптимизированная схема. Читает тот же топик своей группой потребителей,
-- поэтому обе таблицы получают полные данные и их можно честно сравнить.

CREATE TABLE IF NOT EXISTS default.kafka_transactions
(
    transaction_time String,
    merch            String,
    cat_id           String,
    amount           Decimal32(2),
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        UInt32,
    lat              Float32,
    lon              Float32,
    population_city  UInt32,
    jobs             String,
    merchant_lat     Float32,
    merchant_lon     Float32,
    target           UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_optimized',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS default.transactions
(
    -- время идёт почти по порядку: Delta хранит разницу соседних значений, ZSTD её сжимает
    transaction_time DateTime                CODEC(Delta, ZSTD(1)),
    -- строки с небольшим числом различных значений -> словарное кодирование
    merch            LowCardinality(String),
    cat_id           LowCardinality(String),
    -- деньги: точный тип на 4 байта вместо Float64 на 8
    amount           Decimal32(2)            CODEC(ZSTD(1)),
    name_1           LowCardinality(String),
    name_2           LowCardinality(String),
    gender           Enum8('F' = 1, 'M' = 2),
    street           LowCardinality(String),
    one_city         LowCardinality(String),
    us_state         LowCardinality(String),
    -- целые с подобранной разрядностью вместо Int64
    post_code        UInt32,
    -- координаты: Float32 даёт ~7 значащих цифр, для 4-6 знаков после точки хватает
    lat              Float32                 CODEC(ZSTD(1)),
    lon              Float32                 CODEC(ZSTD(1)),
    population_city  UInt32,
    jobs             LowCardinality(String),
    merchant_lat     Float32                 CODEC(ZSTD(1)),
    merchant_lon     Float32                 CODEC(ZSTD(1)),
    target           UInt8,

    -- Агрегатная проекция под запрос из п.3: ClickHouse при вставке заранее считает
    -- max/argMax по штатам, и запрос читает пару десятков строк вместо всей таблицы.
    PROJECTION p_state_top_category
    (
        SELECT
            us_state,
            argMax(cat_id, amount),
            max(amount)
        GROUP BY us_state
    )
)
ENGINE = MergeTree
-- Порядок сортировки подобран по размеру: штат -> город -> клиент -> время.
-- street в этих данных фактически идентифицирует клиента (965 значений), поэтому
-- его строки лежат подряд и атрибуты клиента (имя, координаты, индекс, население)
-- сжимаются почти в ноль, а время внутри клиента растёт и хорошо ложится на Delta.
-- Партиционирование по месяцу проверено и отброшено: на 786 тыс. строк оно лишь
-- дробит данные на 15 мелких кусков и увеличивает размер.
ORDER BY (us_state, one_city, street, transaction_time)
-- всегда Wide-формат кусков, чтобы system.columns показывал размер по колонкам
SETTINGS min_bytes_for_wide_part = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_transactions
TO default.transactions
AS
SELECT
    parseDateTimeBestEffort(transaction_time) AS transaction_time,
    merch, cat_id, amount, name_1, name_2,
    CAST(gender AS Enum8('F' = 1, 'M' = 2)) AS gender,
    street, one_city, us_state, post_code, lat, lon, population_city, jobs,
    merchant_lat, merchant_lon, target
FROM default.kafka_transactions;
