# Развернуть окружение

```bash
make rebuild
```

будет:
- развернуто окружение
  - взаимодействие идет полностью по `SSL`, никаких `PLAINTEXT` протоколов
- сгенерированы сертификаты для:
  - контроллеров: `kafka-c-1, kafka-c-2, kafka-c-3`
  - брокеров: `kafka-b-1, kafka-b-2, kafka-b-3`
  - пользователей: `admin, kafka-ui`
  - см папку: `mount_dir`
    - она не добавлена в git, так как ее содержимое генерируется при развертывании
    - скрипт генерации: `scripts/cert.sh`
    - пароль 
      - для сертификатов один общий для простоты примера (`kafka123`)
      - но можно быстро поправить так, чтобы он указывался при развертывании окружения и выставлялся в `ENV` переменную 

В docker-compose.yml файле они объявлены как `super.user` - иначе не поднимался нормально кластер, да и брокеры с контроллерами должны коммуницировать корректно по `SSL`. 
```yml
KAFKA_SUPER_USERS: "User:CN=admin,L=Moscow,OU=Practice,O=Yandex,C=RU;User:CN=kafka-b-1,L=Moscow,OU=Practice,O=Yandex,C=RU;User:CN=kafka-b-2,L=Moscow,OU=Practice,O=Yandex,C=RU;User:CN=kafka-b-3,L=Moscow,OU=Practice,O=Yandex,C=RU;User:CN=kafka-c-1,L=Moscow,OU=Practice,O=Yandex,C=RU;User:CN=kafka-c-2,L=Moscow,OU=Practice,O=Yandex,C=RU;User:CN=kafka-c-3,L=Moscow,OU=Practice,O=Yandex,C=RU;"
```

Не удалось задать такие правила (`mappign.rules`), чтобы можно было указывать в сокращенном виде: `User:admin`.


# Создаем топики и раздаем права

Команды будут выполняться от имени `admin` пользователя (к тому же, он у нас `super.user`). Это реализуется с помощью указания пути к файлу:
```bash
--command-config /etc/kafka/secrets/admin/admin-client.properties
```
- без указания этого файла команды не будут выполняться, точнее будут завершаться с ошибками.
- это один из признаков работы кластера по `SSL`

## Топики

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

**Важно!**
- Если открыть [Kafka-UI](http://localhost:8080/ui/clusters/kafka-kraft/all-topics) со списком топиков, то их там не увидим.

## Дадим Kafka-UI права на топики

- на просмотр списка топиков
  - `--operation Describe` - Просмотр информации. Дает право видеть топики и их детали.
  - `--topic "*"` - Все топики. Разрешает просмотр любых топиков.
```bash
docker exec -it kafka-b-1 kafka-acls \
--command-config /etc/kafka/secrets/admin/admin-client.properties \
--bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
--add \
--allow-principal "User:CN=kafka-ui,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Describe \
--topic "*"
```

- Если открыть [Kafka-UI](http://localhost:8080/ui/clusters/kafka-kraft/all-topics) со списком топиков, то уже увидим их.


## Дадим права на `topic-1` продюсерам и консьюмеров

## Дадим права на `topic-2` продюсерам и консьюмеров

- Продюсеры могут отправлять сообщения.
- Консьюмеры не имеют доступа к чтению данных.

TODO bash команды

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
- список прав какие у кого
```bash
docker exec -it kafka-b-1 kafka-acls \
  --command-config /etc/kafka/secrets/admin/admin-client.properties \
  --bootstrap-server kafka-b-1:9093 \
  --list \
  --cluster 
```