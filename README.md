# ДЗ №2: Kafka → ClickHouse

CSV с транзакциями загружается в топик Kafka, ClickHouse сам читает топик в таблицы,
SQL-запрос находит категорию самой крупной транзакции по каждому штату,
а оптимизированная схема хранит те же данные в 5 раз компактнее и отвечает на запрос, читая 152 строки вместо 786 тысяч.

```
data/train.csv ──producer.py──► Kafka, топик transactions
                                     │
                ┌────────────────────┴─────────────────────┐
                ▼ группа clickhouse_raw                    ▼ группа clickhouse_optimized
     kafka_transactions_raw (ENGINE=Kafka)        kafka_transactions (ENGINE=Kafka)
                │ mv_transactions_raw                      │ mv_transactions
                ▼                                          ▼
     transactions_raw (п.2, базовая)              transactions (п.4, оптимизированная)
```

Обе схемы читают топик разными группами потребителей, поэтому каждая получает все данные
и их можно сравнить на одинаковом наборе.

## Структура

| Файл | Что это | Пункт задания |
|---|---|---|
| [producer/producer.py](producer/producer.py) | Читает CSV и отправляет каждую строку в топик в формате JSON | п.1 |
| [producer/Dockerfile](producer/Dockerfile), [requirements.txt](requirements.txt) | Образ продюсера | п.1 |
| [sql/01_ddl.sql](sql/01_ddl.sql) | DDL базовой схемы: Kafka-таблица → MATERIALIZED VIEW → MergeTree | п.2 |
| [sql/02_query.sql](sql/02_query.sql) | Запрос «категория наибольшей транзакции по штату» | п.3 |
| [result/result.csv](result/result.csv) | Результат запроса | п.3 |
| [sql/03_ddl_optimized.sql](sql/03_ddl_optimized.sql) | Оптимизированная DDL | п.4 |
| [sql/04_compare.sql](sql/04_compare.sql) | Сравнение размера и скорости двух схем | п.4 |
| [docker-compose.yml](docker-compose.yml) | Kafka (KRaft, без Zookeeper), ClickHouse, продюсер | — |

## Запуск

Нужны Docker с Docker Compose v2 и свободные порты `9094`, `8123`, `9000`.
Команды одинаково работают в PowerShell и в bash.

### 1. Данные

Скачайте `train.csv` со страницы соревнования
https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394 (вкладка *Data*)
и положите его в папку `data/`:

```
data/train.csv
```

### 2. Поднять Kafka и ClickHouse

```bash
docker compose up -d --build --wait
```

При первом старте ClickHouse сам выполнит `sql/01_ddl.sql` и `sql/03_ddl_optimized.sql`
(они подключены в `/docker-entrypoint-initdb.d`). Проверка:

```bash
docker compose exec clickhouse clickhouse-client --query "SHOW TABLES"
```

Должно вывести шесть таблиц: `kafka_transactions`, `kafka_transactions_raw`, `mv_transactions`,
`mv_transactions_raw`, `transactions`, `transactions_raw`.

### 3. Загрузить CSV в Kafka (п.1)

```bash
docker compose run --rm producer
```

Ожидаемый вывод в конце: `Готово: 786431 строк в топик transactions за ~20 с`.
Продюсер вынесен в профиль `load`, поэтому `docker compose up` его не запускает
и данные случайно не зальются дважды.

Через несколько секунд ClickHouse дочитает топик. Проверка (обе цифры должны быть `786431`):

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT (SELECT count() FROM transactions_raw), (SELECT count() FROM transactions)"
```

### 4. Выполнить запрос и сохранить CSV (п.3)

```bash
docker compose exec clickhouse sh -c "clickhouse-client --queries-file /sql/02_query.sql --format CSVWithNames > /result/result.csv"
```

Файл появится в `result/result.csv` на хосте: 51 штат плюс заголовок.

### 5. Сравнить схемы (п.4)

Прогнать запрос по 10 раз на каждой таблице, затем посмотреть отчёт:

```bash
docker compose exec clickhouse sh -c 'for i in $(seq 10); do clickhouse-client --queries-file /sql/02_query.sql; sed s/default.transactions/default.transactions_raw/ /sql/02_query.sql | clickhouse-client; done > /dev/null'
docker compose exec clickhouse clickhouse-client --format PrettyCompactMonoBlock --queries-file /sql/04_compare.sql
```

### Остановить и начать заново

```bash
docker compose down -v
```

Флаг `-v` удаляет том с данными ClickHouse. Без него при следующем старте DDL-скрипты
не выполнятся повторно: ClickHouse запускает `initdb`-скрипты только на пустом томе.

### Продюсер с хоста (без Docker)

```bash
python -m venv .venv
.venv/Scripts/activate        # Windows; в Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
python producer/producer.py --csv data/train.csv --bootstrap localhost:9094
```

С хоста Kafka доступна на `localhost:9094`, из контейнеров на `kafka:9092`.
Это два разных listener'а в `docker-compose.yml`.

## Запрос (п.3)

```sql
SELECT
    us_state,
    argMax(cat_id, amount) AS top_category,
    max(amount)            AS max_amount
FROM default.transactions
GROUP BY us_state
ORDER BY us_state;
```

`argMax(cat_id, amount)` возвращает `cat_id` из той строки группы, где `amount` максимален.
Ничьих по максимальной сумме внутри штата в данных нет, поэтому ответ однозначный.
Результаты на базовой и оптимизированной таблицах совпадают.

## Оптимизация (п.4)

### Результат

| | Базовая `transactions_raw` | Оптимизированная `transactions` |
|---|---|---|
| Размер на диске (сжатый) | 55.98 MiB | **11.20 MiB** (в 5 раз меньше) |
| Время запроса п.3, среднее из 10 | 12.4 мс | **4.4 мс** |
| Прочитано строк | 786 431 | **152** |
| Прочитано байт | 28.89 MiB | **14.4 KiB** |

### Что изменено и почему

1. **Типы под данные.**
   - `LowCardinality(String)` для строк с небольшим числом различных значений
     (`cat_id` 14, `us_state` 51, `merch` 693, `street` 965 и т.д.): вместо строки хранится номер в словаре.
   - `Decimal32(2)` для `amount`: точный тип для денег на 4 байта вместо `Float64` на 8.
   - `Float32` для координат: 7 значащих цифр хватает для 4–6 знаков после точки.
   - `UInt32`/`UInt8` вместо `Int64` для индекса, населения и `target`.
   - `Enum8` для `gender`.
2. **Порядок сортировки `ORDER BY (us_state, one_city, street, transaction_time)`.**
   В этих данных `street` фактически идентифицирует клиента, поэтому строки одного клиента
   лежат подряд. Его имя, координаты, индекс и профессия повторяются и сжимаются почти в ноль:
   например, `lat` занимает 2.98 MiB в базовой таблице и 16.8 KiB в оптимизированной.
   Варианты сортировки сравнивались по размеру:

   | ORDER BY | Размер |
   |---|---|
   | `(us_state, cat_id, transaction_time)` | 20.2 MiB |
   | `(us_state, one_city, street, transaction_time)` | **10.7 MiB** |

3. **Кодеки.** `Delta + ZSTD` для времени (внутри клиента оно растёт, поэтому хранятся
   небольшие разницы), `ZSTD` для сумм и координат.
4. **Агрегатная PROJECTION под запрос.** При вставке ClickHouse заранее считает
   `argMax(cat_id, amount)` и `max(amount)` по штатам. Запрос автоматически читает
   проекцию (это видно в колонке `used_projections` отчёта) и обходится 152 строками.
5. **От партиционирования отказались.** Помесячное `PARTITION BY toYYYYMM(transaction_time)`
   на 786 тыс. строк дробит таблицу на 15 мелких кусков и увеличивает размер
   (22.3 MiB против 20.2 MiB). Партиции окупаются на объёмах на порядки больше.

## Возможные проблемы

- **Порт занят** (`bind: address already in use`): освободите `9094`, `8123` или `9000`
  либо поменяйте левую часть проброса в `docker-compose.yml`.
- **`SHOW TABLES` пуст**: том ClickHouse остался от прошлого запуска.
  Выполните `docker compose down -v` и начните с шага 2.
- **Счётчики строк больше 786431**: продюсер запускали несколько раз, и данные задублировались.
  Помогает `docker compose down -v` и повтор с шага 2.
