# Развернуть окружение

```bash
docker-compose up -d
```

## Создать топики:

Потому что для брокеров я прописал `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`.

```bash
docker exec -it kafka-b-1 kafka-topics --bootstrap-server localhost:9092 \
  --create --topic pg.public.users \
  --partitions 3 \
  --replication-factor 2 \
&& docker exec -it kafka-b-1 kafka-topics --bootstrap-server localhost:9092 \
  --create --topic pg.public.orders \
  --partitions 3 \
  --replication-factor 2
```

## Отправка конфигурации коннектора

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @debezium/debezium-config.json \
  http://localhost:8083/connectors
```

###  Результат
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
        "database.server.name": "postgres",
        "table.include.list": "public.users,public.orders",
        "plugin.name": "pgoutput",
        "snapshot.mode": "initial",
        "topic.prefix": "pg",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "none",
        "transforms.unwrap.add.fields": "op,table,source.ts_ms",
        "message.key.columns": "public.users:id;public.orders:id",
        "name": "postgres-connector"
    },
    "tasks": [],
    "type": "source"
}
```

## Проверить статус коннектора

```bash
curl http://localhost:8083/connectors/postgres-connector/status
```

###  Результат

```json
{
    "name": "postgres-connector",
    "connector": {
        "state": "RUNNING",
        "worker_id": "172.20.0.9:8083"
    },
    "tasks": [
        {
            "id": 0,
            "state": "RUNNING",
            "worker_id": "172.20.0.9:8083"
        }
    ],
    "type": "source"
}
```

Из чего следует, что коннектор `postgres-connector` успешно запущен и работает.

## Посмотреть все коннекторы

```bash
curl http://localhost:8083/connectors
```

###  Результат
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
  - Префикс для имён топиков Kafka (<prefix>.<schema>.<table> → pg.public.users).
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

TODO

# Пошаговые инструкции по проверке работоспособности решения

TODO