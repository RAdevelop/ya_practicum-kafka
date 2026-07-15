

## Логика работы приложения

При старте web-приложения создаются
- View таблицы для:
  - запрещенных слов
  - состояние блокировок
- Эмиттеры для:
  - добавления новых запрещенных слов
  - обновления состояния блокировок у пользователей
  - отправки сообщений
- Процессоры для:
  - обновления состояния постоянного хранилища запрещенных слов
  - обновления состояния постоянного хранилища состояний блокировок у пользователей
  - процесса цензуры
  - процесса "демонстрации" какие в итоге сообщения дойдут и кому


## Cхема работы

- Пользователь (FromUserID) отправляет сообщение пользователю (ToUserID) с текстом сообщения
  - Сообщение отправляется в топик `messages`
- Процессор `censor` проверяет
  - есть ли для пользователя `ToUserID` список заблокированных пользователей
  - если есть, то сообщение дальше никак не обрабатывается, информация логируется
  - иначе проверяет, есть ли запрещенные слова в постоянном хранилище `group-bad-word-table`
  - если есть, то текст сообщения проходит цензуру
    - запрещенные слова в тексте сообщения маскируются (заменяются на "*" в том кол-ве, сколько символов у запрещенного слова)
  - дальше сообщение отправляется в топик `filtered-messages`
  - процессор `sender` это показывает в виде логов
- Процессор `bad_word` обновляет постоянное хранилище запрещенных слов
- Процессор `blocker` обновляет постоянное хранилище по состоянии блокировок у пользователя

## Web-api

Сделано минималистично.
- `GET /bad-words` - выводит список запрещенных слов из постоянного хранилища
- `GET /bad-word?word=[слово]` - добавляет новое запрещенное слово
- `GET /user-block/{user_id}` - показывает состояние блокировки пользователей для указанного
- `GET /user-block/{user_id}/{action}/{block_uid}` - для пользователя `{user_id}` можно `block|unblock` пользователя `{block_uid}`
  - пример:
    - `/user-block/1/block/2` - пользователь 1 заблокирует для себя пользователя 2, не будет получать от него сообщений
    - `/user-block/1/unblock/2` - пользователь 1 разблокирует для себя пользователя 2, будет получать от него сообщения
- `GET /message/{from_uid}/{to_uid}/?text=[какой-то текст]` - пользователь `from_uid` отправляет сообщение`to_uid` с текстом  


## Как проверять

### Развернуть кластер на основе unit-1

См [Кластер из 3-х нод (каждая нода выступает в роле брокера и контроллера)](../../unit_1/README.md#развертывание-кластера-в-docker-3c-3b)
- там выполните команду 
```bash
docker-compose -f docker-compose-3c-3b.yml up -d
```
- Или, если сейчас находитесь в паке данного проекта: `unit_2/goka-messages`, то:
```bash
 docker-compose -f ../../unit_1/docker-compose-3c-3b.yml up -d
```

Создать топики:
```bash
docker exec -it kafka-b-1 kafka-topics --create --topic messages --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker exec -it kafka-b-1 kafka-topics --create --topic bad-words --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker exec -it kafka-b-1 kafka-topics --create --topic blocked-users --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker exec -it kafka-b-1 kafka-topics --create --topic filtered-messages --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker exec -it kafka-b-1 kafka-topics --create --topic group-bad-word-table --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=compact
docker exec -it kafka-b-1 kafka-topics --create --topic group-blocked-users-table --bootstrap-server localhost:9092 --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=compact
```

Далее развернуть данное приложение:
```bash
docker-compose -f docker-compose.yml up -d
```

Открыть логи контейнера приложения для просмотра в реальном времени:
```bash
docker logs -f go-messages
```


- В браузере отправить сообщение: `http://localhost:8181/message/1/2/?text=%D0%BF%D0%BB%D0%BE%D1%85%D0%BE%20bad`
  - ожидаемый результат:
    - в браузере:
      - ```text
          {
          "message": "{\"id\":6788460617522188318,\"from_user_id\":1,\"to_user_id\":2,\"text\":\"плохо bad\"}",
          "status": "ok"
          }
          ```
    - в терминале:
      - ```text
        SUCCESS: 2026/07/15 09:32:53 [API]: in file: handlers.go:214: emit message: model.Message{ID:6788460617522188318, FromUserID:0x1, ToUserID:0x2, Text:"плохо bad"}
        ...
        SUCCESS: 2026/07/15 09:32:53 [MessageSendProcessor]: in file: sender.go:49: message: model.Message{ID:6788460617522188318, FromUserID:0x1, ToUserID:0x2, Text:"плохо bad"}
        ```
- В браузере добавить запрещенное слово `плохо`: `http://localhost:8181/bad-word?word=%D0%BF%D0%BB%D0%BE%D1%85%D0%BE`
  - ожидаемый результат:
    - в браузере:
    - ```text
          {
          "badWord": "плохо",
          "status": "ok"
          }
      ```
    - в терминале:
      - ```text
        SUCCESS: 2026/07/15 09:46:02 [API]: in file: handlers.go:100: EmitSync bad word: плохо
        SUCCESS: 2026/07/15 09:46:02 [BadWordProcessor]: in file: bad_word.go:73: badWords updated: store.BadWordsStore{Words:map[string]string{"плохо":"*****"}}
        ```
- В браузере снова отправить сообщение: `http://localhost:8181/message/1/2/?text=%D0%BF%D0%BB%D0%BE%D1%85%D0%BE%20bad`
  - ожидаемый результат:
    - в браузере:
      - ```text
          {
          "message": "{\"id\":8717473943191261799,\"from_user_id\":1,\"to_user_id\":2,\"text\":\"плохо bad\"}",
          "status": "ok"
          }
          ```
    - в терминале (слово `плохо` заменено на `*****`):
      - ```text
        SUCCESS: 2026/07/15 09:48:33 [CensorProcessor]: in file: censor.go:101: process messageID= 8717473943191261799, {"id":8717473943191261799,"from_user_id":1,"to_user_id":2,"text":"***** bad"}
        SUCCESS: 2026/07/15 09:48:33 [MessageSendProcessor]: in file: sender.go:48: send message ID= 8717473943191261799, FromUserID = 1, ToUserID = 2
        SUCCESS: 2026/07/15 09:48:33 [MessageSendProcessor]: in file: sender.go:49: message: model.Message{ID:8717473943191261799, FromUserID:0x1, ToUserID:0x2, Text:"***** bad"}
        ```
- В браузере пользователь 2 заблокирует у себя пользователя 1: `http://localhost:8181/user-block/2/block/1`
  - ожидаемый результат:
    - в браузере:
      - ```text
          {
            "event": "block:2:1",
            "status": "ok"
          }
          ```
    - в терминале:
      - ```text
        SUCCESS: 2026/07/15 09:50:53 [API]: in file: handlers.go:160: EmitSync event: block:2:1
        SUCCESS: 2026/07/15 09:50:53 [BlockUserProcessor]: in file: blocker.go:97: block users for ctx.Key()= 2, ctx.Value() = &{2 map[1:true]}
        SUCCESS: 2026/07/15 09:50:53 [BlockUserProcessor]: in file: blocker.go:98: block users for 2: map[1:true]
        ```
- В браузере снова отправить сообщение: `http://localhost:8181/message/1/2/?text=%D0%BF%D0%BB%D0%BE%D1%85%D0%BE%20bad`
  - ожидаемый результат:
    - в браузере:
      - ```text
          {
              "message": "{\"id\":6074040222336112825,\"from_user_id\":1,\"to_user_id\":2,\"text\":\"плохо bad\"}",
              "status": "ok"
          }
          ```
    - в терминале (слово `плохо` заменено на `*****`):
      - ```text
        SUCCESS: 2026/07/15 09:52:43 [API]: in file: handlers.go:214: emit message: model.Message{ID:6074040222336112825, FromUserID:0x1, ToUserID:0x2, Text:"плохо bad"}
        ERROR: 2026/07/15 09:52:43 [CensorProcessor]: in file: censor.go:76: can't send message FromUserID to ToUserID, cause FromUserID is blocked, message={"id":6074040222336112825,"from_user_id":1,"to_user_id":2,"text":"плохо bad"}
        INFO: 2026/07/15 09:52:43 [CensorProcessor]: in file: censor.go:69: blockedUser = &store.BlockedUsersStore{UserID:"2", BlockedUserIDs:map[string]bool{"1":true}}
        ```
        - видно, что сообщение дальше не ушло к `MessageSendProcessor` - от него нет логов
- В браузере пользователь 2 РАЗблокирует у себя пользователя 1: `http://localhost:8181/user-block/2/unblock/1`
  - ожидаемый результат:
    - в браузере:
      - ```text
          {
             "event": "unblock:2:1",
             "status": "ok"
          }
          ```
    - в терминале:
      - ```text
        SUCCESS: 2026/07/15 09:56:55 [API]: in file: handlers.go:160: EmitSync event: unblock:2:1
        SUCCESS: 2026/07/15 09:56:55 [BlockUserProcessor]: in file: blocker.go:97: unblock users for ctx.Key()= 2, ctx.Value() = &{2 map[]}
        SUCCESS: 2026/07/15 09:56:55 [BlockUserProcessor]: in file: blocker.go:98: unblock users for 2: map[]
        ```
- В браузере снова отправить сообщение: `http://localhost:8181/message/1/2/?text=%D0%BF%D0%BB%D0%BE%D1%85%D0%BE%20bad`
  - ожидаемый результат:
    - в браузере:
      - ```text
          {
              "message": "{\"id\":1317930504628117587,\"from_user_id\":1,\"to_user_id\":2,\"text\":\"плохо bad\"}",
              "status": "ok"
          }
          ```
    - в терминале (слово `плохо` заменено на `*****`):
      - ```text
        SUCCESS: 2026/07/15 09:58:46 [API]: in file: handlers.go:214: emit message: model.Message{ID:1317930504628117587, FromUserID:0x1, ToUserID:0x2, Text:"плохо bad"}
        INFO: 2026/07/15 09:58:46 [CensorProcessor]: in file: censor.go:69: blockedUser = &store.BlockedUsersStore{UserID:"2", BlockedUserIDs:map[string]bool{}}
        INFO: 2026/07/15 09:58:46 [CensorProcessor]: in file: censor.go:88: mapBadWord = store.BadWordsStore{Words:map[string]string{"плохо":"*****"}}
        SUCCESS: 2026/07/15 09:58:46 [CensorProcessor]: in file: censor.go:101: process messageID= 1317930504628117587, {"id":1317930504628117587,"from_user_id":1,"to_user_id":2,"text":"***** bad"}
        SUCCESS: 2026/07/15 09:58:46 [MessageSendProcessor]: in file: sender.go:48: send message ID= 1317930504628117587, FromUserID = 1, ToUserID = 2
        SUCCESS: 2026/07/15 09:58:46 [MessageSendProcessor]: in file: sender.go:49: message: model.Message{ID:1317930504628117587, FromUserID:0x1, ToUserID:0x2, Text:"***** bad"}
        ```
        - видно, что сообщение дальше ушло к `MessageSendProcessor` - есть логи от него, и сообщение все так же после цензуры замаскировано

  