схема работы:

- user:
  - есть список пользователей (заранее созданных в коде)
    - `id int64` - идентификатор пользователя
    - `accept_bad_word bool` - согласен принимать сообщения без цензуры (да|нет)
      - `accept_bad_word := true` - то пользователь X получает сообщения от любого другого пользователя Y
        - если у такого пользователя X кто-то был в списке заблокированных, то все они удаляются из этого списка. 
          - Адресованное сообщение пользователю X и все последующие сообщения до него доходят.
        - при этом, нецензурные слова в сообщении маскируются
      - `accept_bad_word := false` - то если пользователю X адресовано сообщение от любого другого пользователя Y, то пользователь Y добавляется в список заблокированных для пользователя X.  
        - Адресованное сообщение пользователю X и все последующие сообщения от пользователя Y до него НЕ доходят.
- message - сообщения между пользователями
  - `id int64` - идентификатор сообщения
    - лучше сделать строкой типа `UUID`, так как сообщения могут генерироваться разными сервисами (при условии, что между ними одна база пользователей)
  - `from_user_id int64` - от кого сообщение
  - `to_user_id int64` - кому сообщение
  - `text string` - текст сообщения
- `bad_words` - список нецензурных слов
  - `list map[string]string` - карта слов, в которой ключом является само слово (приведенное к нижнему регистру), в значение маска `*` - столько символ, сколько букв в слове 
- goka emitter:
  - `message` 
    - шлет сообщения в топик `messages`
    - логирует сообщение
  - `bad_word`
    - TODO ?! получает список слов из файла (`io.Reader`)
    - добавляет в топик `bad_words` по ключу `bad_word` слова для цензуры
- goka processor:
  - `user`
    - `blocker`:
      - читает сообщения из топика `messages`
      - проверяет, есть ли у пользователя X, как получателя сообщения, пользователь Y, как отправитель ему сообщения, в списке заблокированных
        - предварительно получив список заблокированных пользователей
        - если есть, то сообщение не отправляется дальше
        - если нет, то сообщение отправляется дальше в топик `messages_needs_checked_by_censor`
      - логирует, что сообщение отправлено дальше или нет
  - `censor`
    - читает сообщения из топика `messages_needs_checked_by_censor`
    - нецензурные слова в тексте сообщений маскирует, например, символом `*` по количеству букв
      - предварительно получив список запрещенных слов
    - отправляет сообщение дальше в топик `filtered_messages`
    - логирует сообщение после цензуры
  - `logger`
    - читает топик `filtered_messages`
    - выступает в качестве эмуляции получения сообщений пользователями
    - выводит в лог какой пользователь какое сообщение получит (получил)
- Топики:
  - `messages` - оригинальные сообщения
  - `bad_words` - запрещенные слова
  - `blocked_users` - в который будем писать кто кого заблокировал для себя
  - `messages_needs_checked_by_censor` - в который будем писать сообщения для дальнейшего цензурирования
  - `filtered_messages` - сообщения, прошедшие через цензуру, и готовые для получения пользователями
- Постоянное хранилище:
  - `group-censor-word-table` - постоянное хранилище для запрещенных слов
  - `group-blocked-users-table` - постоянное хранилище для заблокированных пользователей

## Создание топиков

- messages:

```bash
docker exec -it kafka-b-1 kafka-topics --create --topic messages --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```

- bad_words:

```bash
docker exec -it kafka-b-1 kafka-topics --create --topic bad_words --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```

- blocked_users:

```bash
docker exec -it kafka-b-1 kafka-topics --create --topic blocked_users --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```

- messages_needs_checked_by_censor:

```bash
docker exec -it kafka-b-1 kafka-topics --create --topic messages_needs_checked_by_censor --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```

- filtered_messages:

```bash
docker exec -it kafka-b-1 kafka-topics --create --topic filtered_messages --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```

- group-censor-word-table:

```bash
docker exec -it kafka-b-1 kafka-topics --create --topic group-censor-word-table --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```

- group-blocked-users-table:

```bash
docker exec -it kafka-b-1 kafka-topics --create --topic group-blocked-users-table --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```