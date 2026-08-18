# Развернуть окружение

```bash
make rebuild #Чтобы собрать только окружение, без создания топиков и выдачи прав
make rebuild-all #Чтобы сразу создать топики и выдать права

make ssl: ## Если нужно просто сгенерировать SSL сертификаты (они попадут в паку mount_dir)
```

В результате:
- развернуто окружение
  - взаимодействие идет полностью по `SSL`, никаких `PLAINTEXT` протоколов
- сгенерированы сертификаты для:
  - контроллеров: `kafka-c-1, kafka-c-2, kafka-c-3`
  - брокеров: `kafka-b-1, kafka-b-2, kafka-b-3`
  - пользователей: `admin, kafka-ui, producer, consumer`
  - см папку: `mount_dir`
    - она не добавлена в git, так как ее содержимое генерируется при развертывании
    - скрипт генерации: `scripts/cert.sh`
    - пароль 
      - для сертификатов один общий для простоты примера (`kafka123`)
      - но можно быстро поправить так, чтобы он указывался при развертывании окружения и выставлялся в `ENV` переменную 
- созданы топики: `topic-1, topic-2`
  - скрипт: `scripts/topic.sh`
- выданы необходимые права на топики: `topic-1, topic-2`
  - скрипт: `scripts/acl.sh`

В docker-compose.yml файле они объявлены как `super.user` - иначе не поднимался нормально кластер, да и брокеры с контроллерами должны коммуницировать корректно по `SSL`. 
```yml
KAFKA_SUPER_USERS: "User:CN=admin,L=Moscow,OU=Practice,O=Yandex,C=RU;User:CN=kafka-b-1,L=Moscow,OU=Practice,O=Yandex,C=RU;User:CN=kafka-b-2,L=Moscow,OU=Practice,O=Yandex,C=RU;User:CN=kafka-b-3,L=Moscow,OU=Practice,O=Yandex,C=RU;User:CN=kafka-c-1,L=Moscow,OU=Practice,O=Yandex,C=RU;User:CN=kafka-c-2,L=Moscow,OU=Practice,O=Yandex,C=RU;User:CN=kafka-c-3,L=Moscow,OU=Practice,O=Yandex,C=RU;"
```

Не удалось задать такие правила (`mappign.rules`), чтобы можно было указывать в сокращенном виде: `User:admin`.


# Создаем топики и раздаем права

> **📌 Примечание:**
> 
> Топики и выдача прав уже к этому моменту уже выполнено, если собирали командой `make rebuild-all`. 
>
> Ниже примеры команд с пояснениями, если собирали командой:
> `make rebuild`


Команды будут выполняться от имени `admin` пользователя (к тому же, он у нас `super.user`). Это реализуется с помощью указания пути к файлу:
```bash
--command-config /etc/kafka/secrets/admin/admin-client.properties
```
- без указания этого файла команды не будут выполняться, точнее будут завершаться с ошибками.
- это один из признаков работы кластера по `SSL`

## Создадим топики `topic-1` и `topic-2`

```bash
docker exec -it kafka-b-1 kafka-topics \
  --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
  --command-config /etc/kafka/secrets/admin/admin-client.properties \
  --create \
  --topic topic-1 \
  --partitions 3 \
  --replication-factor 3 \
&& docker exec -it kafka-b-1 kafka-topics \
  --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
  --command-config /etc/kafka/secrets/admin/admin-client.properties \
  --create \
  --topic topic-2 \
  --partitions 3 \
  --replication-factor 3
```


> **📌 Важно!**
>
> Если открыть [Kafka-UI](http://localhost:8080/ui/clusters/kafka-kraft/all-topics) со списком топиков, то их там не увидим.

## Дадим Kafka-UI права на топики

- на просмотр списка топиков
  - `--operation Describe` - Просмотр информации. Дает право видеть топики и их детали.
  - `--topic "*"` - Все топики. Разрешает просмотр любых топиков.
```bash
# Дадим Kafka-UI права на все топики
docker exec -it kafka-b-1 kafka-acls \
--command-config /etc/kafka/secrets/admin/admin-client.properties \
--bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
--add \
--allow-principal "User:CN=kafka-ui,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Describe \
--topic "*"
```

- Если открыть [Kafka-UI](http://localhost:8080/ui/clusters/kafka-kraft/all-topics) со списком топиков, то уже увидим их.


## Дадим права на `topic-1` продюсерам и консьюмерам

```bash
#### topic-1: Доступен как для продюсеров, так и для консьюмеров.

## Дадим `producer` права на запись в топик:
docker exec -it kafka-b-1 kafka-acls \
--command-config /etc/kafka/secrets/admin/admin-client.properties \
--bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
--add \
--allow-principal "User:CN=producer,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Write \
--operation Describe \
--topic "topic-1"

## Дадим `consumer` права на чтение из топика:
docker exec -it kafka-b-1 kafka-acls \
--command-config /etc/kafka/secrets/admin/admin-client.properties \
--bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
--add \
--allow-principal "User:CN=consumer,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Read \
--operation Describe \
--topic "topic-1"
```

## Дадим права на `topic-2` продюсерам и консьюмерам

- Продюсеры могут отправлять сообщения.
- Консьюмеры не имеют доступа к чтению данных.

```bash
#### topic-2:
#### - Продюсеры могут отправлять сообщения.
#### - Консьюмеры не имеют доступа к чтению данных.
## Дадим `producer` права на запись в топик:
docker exec -it kafka-b-1 kafka-acls \
--command-config /etc/kafka/secrets/admin/admin-client.properties \
--bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
--add \
--allow-principal "User:CN=producer,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Write \
--operation Describe \
--topic "topic-2"

## Дадим `consumer` права на топик (только для метаданных):
docker exec -it kafka-b-1 kafka-acls \
--command-config /etc/kafka/secrets/admin/admin-client.properties \
--bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
--add \
--allow-principal "User:CN=consumer,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Describe \
--topic "topic-2"
```

## Посмотрим Список прав - какие кому выдали
```bash
docker exec -it kafka-b-1 kafka-acls \
--command-config /etc/kafka/secrets/admin/admin-client.properties \
--bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
--list \
--topic "topic-1" \
--topic "topic-2"
```

В результате увидим:
- для топика `topic-1`
  - `consumer` имеет права на чтение:
    - `operation=READ, permissionType=ALLOW`
    - `operation=DESCRIBE, permissionType=ALLOW`
  - `producer` имеет права на запись:
    - `operation=WRITE, permissionType=ALLOW`
    - `operation=DESCRIBE, permissionType=ALLOW`
- для топика `topic-2`
  - `consumer` имеет права просмотр метаданных:
    - `operation=DESCRIBE, permissionType=ALLOW`
  - `producer` имеет права на запись:
    - `operation=WRITE, permissionType=ALLOW`
    - `operation=DESCRIBE, permissionType=ALLOW`

```text
Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=topic-1, patternType=LITERAL)`:
	(principal=User:CN=consumer,L=Moscow,OU=Practice,O=Yandex,C=RU, host=*, operation=READ, permissionType=ALLOW)
	(principal=User:CN=consumer,L=Moscow,OU=Practice,O=Yandex,C=RU, host=*, operation=DESCRIBE, permissionType=ALLOW)
	(principal=User:CN=producer,L=Moscow,OU=Practice,O=Yandex,C=RU, host=*, operation=WRITE, permissionType=ALLOW)
	(principal=User:CN=producer,L=Moscow,OU=Practice,O=Yandex,C=RU, host=*, operation=DESCRIBE, permissionType=ALLOW)

Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=topic-2, patternType=LITERAL)`:
	(principal=User:CN=consumer,L=Moscow,OU=Practice,O=Yandex,C=RU, host=*, operation=DESCRIBE, permissionType=ALLOW)
	(principal=User:CN=producer,L=Moscow,OU=Practice,O=Yandex,C=RU, host=*, operation=WRITE, permissionType=ALLOW)
	(principal=User:CN=producer,L=Moscow,OU=Practice,O=Yandex,C=RU, host=*, operation=DESCRIBE, permissionType=ALLOW)
```

Для всех топиков, чтобы увидеть выданные права для `kafka-ui`:
```bash
docker exec -it kafka-b-1 kafka-acls \
  --command-config /etc/kafka/secrets/admin/admin-client.properties \
  --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
  --list \
  --topic "*"
```

В результате увидим:
- для топиков `kafka-ui` имеет права просмотр метаданных:
    - `operation=DESCRIBE, permissionType=ALLOW`

```text
Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=*, patternType=LITERAL)`:
	(principal=User:CN=kafka-ui,L=Moscow,OU=Practice,O=Yandex,C=RU, host=*, operation=DESCRIBE, permissionType=ALLOW)
```

## Еще пример команд ACL

- на изменение кластера для `kafka-ui`
    ```bash
    docker exec -it kafka-b-1 kafka-acls \
    --command-config /etc/kafka/secrets/admin/admin-client.properties \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
      --add \
      --allow-principal "User:CN=kafka-ui,L=Moscow,OU=Practice,O=Yandex,C=RU" \
      --operation ALTER --cluster
    ```

- на все для кластера для `kafka-ui`
    ```bash
    docker exec -it kafka-b-1 kafka-acls \
    --command-config /etc/kafka/secrets/admin/admin-client.properties \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --add \
    --allow-principal "User:CN=kafka-ui,L=Moscow,OU=Practice,O=Yandex,C=RU" \
    --operation All \
    --cluster
    ```

- на право просматривать информацию о ресурсе, без возможности изменять его. для User:kafka-ui
  ```bash
      docker exec -it kafka-b-1 kafka-acls \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --command-config /etc/kafka/secrets/admin/admin-client.properties \
    --add \
    --allow-principal "User:CN=kafka-ui,L=Moscow,OU=Practice,O=Yandex,C=RU" \
    --operation Describe \
    --cluster 
  ```
