-- П.2. Базовая (неоптимизированная) схема: Kafka -> MATERIALIZED VIEW -> MergeTree.

-- 1. Таблица-«труба»: сама данные не хранит, а читает сообщения из топика.
--    Каждое чтение из неё сдвигает offset группы потребителей.
CREATE TABLE IF NOT EXISTS default.kafka_transactions_raw
(
    transaction_time String,
    merch            String,
    cat_id           String,
    amount           Float64,
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        Int64,
    lat              Float64,
    lon              Float64,
    population_city  Int64,
    jobs             String,
    merchant_lat     Float64,
    merchant_lon     Float64,
    target           Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_raw',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- 2. Таблица, где данные лежат на диске. Типы «как получилось», без сортировки.
CREATE TABLE IF NOT EXISTS default.transactions_raw
(
    transaction_time DateTime,
    merch            String,
    cat_id           String,
    amount           Float64,
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        Int64,
    lat              Float64,
    lon              Float64,
    population_city  Int64,
    jobs             String,
    merchant_lat     Float64,
    merchant_lon     Float64,
    target           Int64
)
ENGINE = MergeTree
ORDER BY tuple();

-- 3. Материализованное представление - «насос»: забирает пачки из Kafka-таблицы
--    и вставляет их в MergeTree. Время в CSV без секунд, поэтому parseDateTimeBestEffort.
CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_transactions_raw
TO default.transactions_raw
AS
SELECT
    parseDateTimeBestEffort(transaction_time) AS transaction_time,
    merch, cat_id, amount, name_1, name_2, gender, street, one_city, us_state,
    post_code, lat, lon, population_city, jobs, merchant_lat, merchant_lon, target
FROM default.kafka_transactions_raw;
