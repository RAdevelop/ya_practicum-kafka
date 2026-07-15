схема работы:

- `user`:
  - есть список пользователей (заранее созданных в коде)
    - `id int64` - идентификатор пользователя
- `message` - сообщения между пользователями
  - `id int64` - идентификатор сообщения
    - лучше сделать строкой типа `UUID`, так как сообщения могут генерироваться разными сервисами (при условии, что между ними одна база пользователей)
  - `from_user_id int64` - от кого сообщение
  - `to_user_id int64` - кому сообщение
  - `text string` - текст сообщения
- `bad-words` - список нецензурных слов
  - `map[string]string` - карта слов, в которой ключом является само слово (приведенное к нижнему регистру), в значение маска `*` - столько символ, сколько букв в слове 
- goka emitter:
  - `blocker` 
    - шлет событие о блокировке/разблокировке в топик `blocked-users`
    - логирует событие
  - `message` 
    - шлет сообщения в топик `messages`
    - логирует сообщение
  - `bad-word`
    - добавляет в топик `bad-words` по ключу `bad-word` слова для цензуры
- goka processor:
  - `user`
    - `blocker`:
      - читает сообщения из топика `blocked-users`
      - обновляет персистентную таблицу `group-blocked-users-table` по состоянию блокировок
  - `censor`
    - читает сообщения из топика `messages`
    - проверяет наличие блокировки у между пользователями, если она есть, сообщение дальше не уходит
    - нецензурные слова в тексте сообщений маскирует, например, символом `*` по количеству букв
      - предварительно получив список запрещенных слов
    - отправляет сообщение дальше в топик `filtered-messages`
    - логирует сообщение после цензуры
  - `sender`
    - читает топик `filtered-messages`
    - выступает в качестве эмуляции получения сообщений пользователями
    - выводит в лог какой пользователь какое сообщение получит (получил)
- Топики:
  - `messages` - оригинальные сообщения
  - `bad-words` - запрещенные слова
  - `blocked-users` - в который будем писать кто кого заблокировал для себя
  - `filtered-messages` - сообщения, прошедшие через цензуру, и готовые для получения пользователями
- Постоянное хранилище:
  - `group-bad-word-table` - постоянное хранилище для запрещенных слов
  - `group-blocked-users-table` - постоянное хранилище для заблокированных пользователей

## Создание топиков

- messages:

```bash
docker exec -it kafka-b-1 kafka-topics --create --topic messages --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker exec -it kafka-b-1 kafka-topics --create --topic bad-words --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker exec -it kafka-b-1 kafka-topics --create --topic blocked-users --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker exec -it kafka-b-1 kafka-topics --create --topic filtered-messages --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker exec -it kafka-b-1 kafka-topics --create --topic group-bad-word-table --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=compact
docker exec -it kafka-b-1 kafka-topics --create --topic group-blocked-users-table --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=compact
```