# Развернуть окружение

будет:
- развернуто окружение
- созданы топики
- загружены настройки для коннектора
- проверен его статус
- получен список коннекторов

```bash
docker-compose up -d \
&& echo "wait for 30 seconds when environment is ready:\n" \
&& sleep 30 \
&& echo "crete topics:" \
&& docker exec -it kafka-b-1 kafka-topics --bootstrap-server localhost:9092 \
  --create --topic pg.public.users \
  --partitions 3 \
  --replication-factor 2 \
&& docker exec -it kafka-b-1 kafka-topics --bootstrap-server localhost:9092 \
  --create --topic pg.public.orders \
  --partitions 3 \
  --replication-factor 2 \
&& echo "\n" \
&& echo "uploading debezium/debezium-config.json:" \
&& curl -X POST -H "Content-Type: application/json" \
  --data @debezium/debezium-config.json \
  http://localhost:8083/connectors \
&& echo "\n" \
&& sleep 2 \
&& echo "postgres-connector/status:" \
&& curl http://localhost:8083/connectors/postgres-connector/status \
&& echo "\n" \
&& echo "connectors list:" \
&& curl http://localhost:8083/connectors
```

###  Результат отправки конфигурации коннектора
```json
{
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "customers",
        "table.include.list": "public.users,public.orders",
        "plugin.name": "pgoutput",
        "snapshot.mode": "initial",
        "topic.prefix": "pg",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "message.key.columns": "public.users:id;public.orders:id",
        "name": "postgres-connector"
    },
    "tasks": [],
    "type": "source"
}
```

###  Результат статуса коннектора

```json
{
    "name": "postgres-connector",
    "connector": {
        "state": "RUNNING",
        "worker_id": "172.20.0.10:8083"
    },
    "tasks": [
        {
            "id": 0,
            "state": "RUNNING",
            "worker_id": "172.20.0.10:8083"
        }
    ],
    "type": "source"
}
```

Из чего следует, что коннектор `postgres-connector` успешно запущен и работает.

###  Результат списка коннекторов
```json
[
    "postgres-connector"
]
```

# Настройки Debezium Connector

## Файл unit_4/debezium/debezium-config.json

**Важно!**
Все чувствительные данные конечно в явном виде хранить не стоит тут, и не сохранять в git.

- `"name": "postgres-connector"`
  - Имя для коннектора
- `"connector.class": "io.debezium.connector.postgresql.PostgresConnector"`
  - Класс коннектора — указывает Debezium, что это коннектор для PostgreSQL
- `"database.hostname": "postgres"`
  - Имя хоста (или IP) PostgreSQL. В Docker — имя сервиса (postgres в файле `docker-compose.yml`)
- `"database.port": "5432"`
  - Порт для подключения к серверу БД PostgreSQL
- `"database.user": "postgres"`
  - Имя пользователя для подключения к серверу БД PostgreSQL
- `"database.password": "postgres"`
  - Пароль для подключения к БД PostgreSQL
- `"database.dbname": "customers"`
  - Имя базы данных для подключения к серверу БД PostgreSQL
- `"table.include.list": "public.users,public.orders"`
  - Список таблиц, которые нужно отслеживать.
- `"plugin.name": "pgoutput"`
  - Плагин декодирования WAL. pgoutput — стандартный для PostgreSQL 10+
- `"snapshot.mode": "initial"`
  - Как часто делать снимки (`initial` - делает снимок при первом запуске)
- `"topic.prefix": "pg"`
  - Префикс для имён топиков Kafka (`<prefix>.<schema>.<table>` -> `pg.public.users`).
- `"key.converter": "org.apache.kafka.connect.json.JsonConverter"`
  - Конвертер для ключей сообщений (JSON).
- `"value.converter": "org.apache.kafka.connect.json.JsonConverter"`
  - Конвертер для значений сообщений (JSON).
- `"key.converter.schemas.enable": "false"`
  - Включает/выключает добавление схемы в ключ сообщения
- `"value.converter.schemas.enable": "false"`
  - Включает/выключает добавление схемы в значение сообщения
- `"message.key.columns": "public.users:id;public.orders:id"`
  - Какие колонки использовать как ключ сообщения при отправке в топики Kafka.

# Настройки базы данных

- `listen_addresses = '*'`
  - разрешает PostgreSQL принимать соединения с любых IP-адресов
- `wal_level = 'logical'`
  - определяет уровень регистрации WAL (Write-Ahead Logging) в PostgreSQL. WAL — это механизм, с помощью которого PostgreSQL отслеживает все изменения в базе данных и записывает их в журнал. Значение `logical` указывает, что PostgreSQL будет регистрировать изменения на уровне отдельных строк, а не на уровне страниц. Важно отметить, что такая регистрация потребуется при подключении Debezium PostgreSQL Connector для чтения изменений из журнала WAL и их передачи в Kafka. Если указать `replica` или `minimal`, Debezium не сможет читать изменения
- `max_wal_senders = 1`
  - определяет максимальное количество процессов, которые могут одновременно читать журнал WAL. Значение 1 — только один процесс может одновременно осуществлять чтение из журнала WAL. Это ограничение предотвращает конфликты между процессами.
- `max_replication_slots = 1`
  - определяет максимальное количество слотов репликации, которые могут быть созданы в базе данных. Слот репликации — это механизм, который позволяет процессам читать журнал WAL и передавать изменения в другую базу данных или в Kafka. Значение 1 — может быть создан только один слот репликации.

# Назначение каждого компонента и их взаимосвязи

1. Kafka Cluster (Controller + Broker)
   
Kafka Controllers (kafka-c-1, kafka-c-2, kafka-c-3)

Назначение:
- Управляют кластером Kafka (KRaft режим)
- Хранят данные в топиках
- Кластер из 3 брокеров обеспечивает отказоустойчивость

Взаимодействие:

- Принимают данные от Kafka Connect через PLAINTEXT://kafka-b-*:9092
- Доступны извне через порты 19094, 29094, 39094 (маппинг на 127.0.0.1)

2. PostgreSQL (База данных)
  
Назначение:

- Источник данных для Debezium (source connector)
- Хранит таблицу users в БД customers
- При изменении данных (INSERT/UPDATE/DELETE) генерирует события CDC

Взаимодействие:

- Kafka Connect подключается к PostgreSQL для чтения WAL (Write-Ahead Log)
- Использует логический слот репликации debezium_slot
- Читает данные через порт 5432

3. Kafka Connect (Debezium)
   
Назначение:

- Source Connector: Читает изменения из PostgreSQL и отправляет в Kafka
- Менеджер коннекторов и задач
- Экспортирует JMX метрики на порт 9404

Взаимодействие:

- С PostgreSQL: Читает WAL через логическую репликацию
- С Kafka: Отправляет события в топики (например, pg.public.users)
- С Prometheus: Экспортирует метрики через JMX Exporter на 9404
- С Kafka UI: Статус коннекторов доступен через REST API 8083

4. Prometheus (Сборщик метрик)

Назначение:

- Scrape (сбор) метрик с Kafka Connect (порт 9404)
- Хранит временные ряды метрик
- Предоставляет данные для Grafana через API

Взаимодействие:

- Pull модель: Prometheus опрашивает Kafka Connect каждые 5 секунд
- С Grafana: Prometheus является источником данных


5. Grafana (Визуализация)

Назначение:

- Визуализирует метрики из Prometheus
- Отображает дашборд с Kafka Connect метриками
- Показывает: количество коннекторов, скорость записи, статистику

Взаимодействие:

- С Prometheus: Подключается через Datasource http://prometheus:9090
- С пользователем: Доступен через веб-интерфейс http://localhost:3000

6. Kafka UI (Веб-интерфейс)

Назначение:

- Визуализация Kafka кластера
- Просмотр топиков, сообщений, партиций
- Мониторинг консьюмер групп

Взаимодействие:

- Подключается к брокерам через kafka-b-1:9092,kafka-b-2:9092,kafka-b-3:9092
- Доступен через http://localhost:8080

## Взаимосвязи компонентов (поток данных сверху вниз):

1. Пользователь выполняет INSERT в PostgreSQL
2. PostgreSQL записывает данные в WAL
3. Debezium Connector (Kafka Connect) читает WAL
4. Debezium формирует событие CDC и отправляет в Kafka
5. Kafka сохраняет событие в топике (customers.public.users)
6. Prometheus собирает метрики с Kafka Connect (порт 9404)
7. Grafana отображает метрики на дашборде
8. Пользователь видит изменения метрик в Grafana

# Пошаговые инструкции по проверке работоспособности решения

## Шаг 1 -Развернуть окружение. 

## Шаг 2 - проверить результат развертывания

Доступны страницы:
- http://localhost:8080/ - Kafka UI
- http://localhost:9404/metrics - Метрики для сбора Prometheus, страница должна быть доступна, и вернуть данные.
- http://localhost:9090/ - Prometheus UI
  - http://localhost:9090/targets - можно увидеть Job-ы для сбора метрик
- http://localhost:3000 - Grafana (login: admin, password: admin)
  - смену пароля для проверки можно будет пропустить (а вообще, конечно лучше менять)
  - http://localhost:3000/d/kafka-connect-dashboard/kafka-connect-monitoring?orgId=1&from=now-5m&to=now&timezone=browser (настроить метрики для дашборда - как самостоятлеьно, так и с помощью ИИ делал)
    - есть дашборд для метрик:
      - Worker Connector Count
        - Показывает количество коннекторов, запущенных на данном worker'е Kafka Connect.
        - Единица измерения: штуки (количество)
        - Источник: `kafka_connect_worker_connector_count`
      - Worker Task Count
        - Показывает количество задач (`tasks`), запущенных на данном worker'е. Обычно каждый коннектор имеет 1 или более задач для параллельной обработки.
        - Единица измерения: штуки (количество)
        - Источник: `kafka_connect_worker_task_count`
      - Source Record Poll Rate (Records/sec)
        - Скорость, с которой source-коннектор извлекает (poll) записи из источника данных (БД, файловая система, API) до применения трансформаций. Показывает интенсивность чтения данных.
        - Единица измерения: записей в секунду
        - Источник: `kafka_connect_source_task_postgres_connector_0_source_record_poll_rate`
        - Как увидеть изменение:
          - Вставить новые данные в PostgreSQL:
            ```sql
            INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');
            INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');
            INSERT INTO users (name, email) VALUES ('Alice Johnson', 'alice@example.com');
            INSERT INTO users (name, email) VALUES ('Bob Brown', 'bob@example.com');
           ```
      - Source Record Write Rate (Records/sec)
        - Скорость, с которой source-коннектор записывает (write) данные в Kafka после применения трансформаций. Это количество сообщений, которые реально попадают в топики Kafka.
        - Единица измерения: записей в секунду
        - Источник: `kafka_connect_source_task_postgres_connector_0_source_record_write_rate`
        - Write Rate увеличится аналогично Poll Rate
      - Connector Startup Attempts
        - Общее количество попыток запуска коннекторов на данном worker'е. Увеличивается при каждом запуске коннектора, независимо от успеха.
        - Единица измерения: количество попыток (cumulative counter)
        - Источник: `kafka_connect_worker_connector_startup_attempts_total`
      - Connector Startup Success
        - Количество успешных запусков коннекторов. Если коннектор запустился без ошибок, этот счетчик увеличивается.
        - Единица измерения: количество успешных запусков (cumulative counter)
        - Источник: `kafka_connect_worker_connector_startup_success_tota`
        - Перезапустить успешно работающий коннектор:
          ```bash
            curl -X POST http://localhost:8083/connectors/postgres-connector/restart
          ```
          - Startup Success увеличится на 1

## Шаг 3 - Вставить новые данные в PostgreSQL:

```bash
docker exec -i postgres psql -U postgres -d customers << EOF
INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');
INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');
INSERT INTO users (name, email) VALUES ('Alice Johnson', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob Brown', 'bob@example.com');
INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product A', 2);
INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product B', 1);
INSERT INTO orders (user_id, product_name, quantity) VALUES (2, 'Product C', 5);
INSERT INTO orders (user_id, product_name, quantity) VALUES (3, 'Product D', 3);
INSERT INTO orders (user_id, product_name, quantity) VALUES (4, 'Product E', 4);
SELECT * FROM users;
SELECT * FROM orders;
EOF 
```

Результат будет вида:

```bash
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
INSERT 0 1
 id |     name      |       email       |         created_at         
----+---------------+-------------------+----------------------------
  1 | John Doe      | john@example.com  | 2026-08-08 15:06:59.88063
  2 | Jane Smith    | jane@example.com  | 2026-08-08 15:06:59.883732
  3 | Alice Johnson | alice@example.com | 2026-08-08 15:06:59.884607
  4 | Bob Brown     | bob@example.com   | 2026-08-08 15:06:59.885291
(4 rows)

 id | user_id | product_name | quantity |         order_date         
----+---------+--------------+----------+----------------------------
  1 |       1 | Product A    |        2 | 2026-08-08 15:06:59.886021
  2 |       1 | Product B    |        1 | 2026-08-08 15:06:59.88746
  3 |       2 | Product C    |        5 | 2026-08-08 15:06:59.888183
  4 |       3 | Product D    |        3 | 2026-08-08 15:06:59.88898
  5 |       4 | Product E    |        4 | 2026-08-08 15:06:59.88971
(5 rows)

```

В течение 5 секунд [увидим изменения в метриках](http://localhost:3000/d/kafka-connect-dashboard/kafka-connect-monitoring?orgId=1&from=now-5m&to=now&timezone=browser) (см описание метрик в шаге 2).
- Source Record Poll Total (Cumulative) = 9
- Source Record Write Total (Cumulative) = 9

так как вставили 9 записей

## Шаг 4 - в логах приложения Go увидим записи вида

Данные, которые получает Kafka от Debezium:

```bash
docker logs go-app
```
Увидим логи вида:
```bash
INFO: 2026/08/07 21:59:27 ConsumerUsers: in file: main.go:117: Message:
{
  "before": null,
  "after": {
    "created_at": 1786139395635838,
    "email": "john@example.com",
    "id": 1,
    "name": "John Doe"
  },
  "source": {
    "connector": "postgresql",
    "db": "customers",
    "lsn": 26683472,
    "name": "pg",
    "schema": "public",
    "sequence": "[null,\"26683472\"]",
    "snapshot": "false",
    "table": "users",
    "ts_ms": 1786128595637,
    "ts_ns": 1786128595637128000,
    "ts_us": 1786128595637128,
    "txId": 747,
    "version": "3.0.0.Final",
    "xmin": null
  },
  "transaction": null,
  "op": "c",
  "ts_ms": 1786128595933,
  "ts_us": 1786128595933573,
  "ts_ns": 1786128595933573878
}
```