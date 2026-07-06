# Развертывание кластера Kafka

## Описание задачи

Разверните локальный Kafka-кластер из трёх серверов.
- Для установки и запуска Kafka-кластера используйте Docker. За основу возьмите докер-файлы из предыдущих уроков или соберите свой докер-файл так, как считаете нужным. Можно настроить через Zookeeper или Kraft.
- Убедитесь, что кластер настроен для реализации отказоустойчивости. В следующей теме вам нужно будет включить репликацию топиков в развёрнутом кластере.

## Развертывание кластера Kafka (в режиме KRaft)

1 нода: брокер (b) + контроллер (c) - поэтому будем так же шарить порты наружу
- `cp-kafka` c версии 8 - по умолчанию работает в режиме KRaft
- По портам будем придерживаться следующего:
- `9092` - для внутренней коммуникаций брокеров и "внутренних" клиентов (например, kafka-ui будет находиться в той же Докер-сети)
- `9093` - для внутренней коммуникаций контроллеров (обмен метаданными)
- `9094` - для внешней коммуникаций брокеров (например, для подключения к кластеру из вне)

### Кластер из одной ноды (выступает в роле брокера и контроллера)
 - docker-compose-node-1.yml
```yml
services:
  kafka-cb-1:
    image: confluentinc/cp-kafka:8.3.0
    container_name: kafka-cb-1
    hostname: kafka-cb-1
    environment:
      # Уникальный идентификатор кластера. Значение одинаково для всех контроллеров и брокеров, его мы получим после первого запуска.
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
      # Уникальный номер ноды в рамках кластера
      KAFKA_NODE_ID: 1
      # Список всех контроллеров кластера с их идентификаторами, адресами и портами.
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-cb-1:9093"
      # KAFKA_LISTENERS - Задаёт адреса и порты, на которых Kafka принимает соединения (внутренние и внешние).
      # Здесь мы говорим Kafka, где искать входящие запросы. В конфигурационном файле вы указываете три слушателя:
      # PLAINTEXT://:9092, CONTROLLER://:9093 и EXTERNAL://:9094.
      #Первые два предназначены для внутренних коммуникаций внутри Docker, а последний ― для внешних соединений с хост-машиной.
      KAFKA_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094"
      # Адреса и порты, которые Kafka «рекламирует» для подключения клиентов. Клиенты Kafka должны знать, как подключиться к брокеру.
      # Если брокер Kafka работает внутри докер-сети, этот параметр может указывать на адрес в этой сети.
      # Если нужно подключаться к брокеру локально, здесь указывают соответствующий порт и протокол.
      # В вашем конфигурационном файле вы объявляете два слушателя PLAINTEXT://kafka-0:9092 и EXTERNAL://127.0.0.1:9094.
      # Первый слушатель обслуживает внутренние коммуникации в Docker, второй ― внешние коммуникации с хост-машиной.
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-cb-1:9092,EXTERNAL://127.0.0.1:9094"
      # Определяет, какой протокол безопасности используется для каждого типа соединений.
      # Это помогает обеспечить безопасность данных при передаче между брокером и клиентами.
      # В вашем конфигурационном файле вы указываете, что все слушатели (CONTROLLER, EXTERNAL, PLAINTEXT)
      # будут использовать протокол PLAINTEXT, который не предполагает шифрования данных.
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"
      # Указывает слушателя для БРОКЕРА, который используется для его связи с другими узлами.
      # Настройку можно не указывать, PLAINTEXT - имя по умолчанию. Но можно переименовать в INTERNAL (по вкусу).
      # Тогда надо будет внести изменения в строки всех настроек заменить "PLAINTEXT:" на "INTERNAL:"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      # Указывает слушателя для КОНТРОЛЛЕРА, который используется для его связи с другими узлами.
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      # Устанавливает, какие функциональные роли будет выполнять конкретный узел Kafka
      # #Переменная указывает, какие роли будет выполнять узел в кластере — брокер, контроллер или оба сразу.
      KAFKA_PROCESS_ROLES: "controller,broker"
      # Указывает количество реплик для топика смещений (__consumer_offsets), которая хранит информацию о смещениях потребителей.
      # Значение 1 означает, что не будет резервных копий смещения.
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      # Отключаем автоматическое создание топиков (опционально)
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    # Монтируем расположение данных и секретов в Docker Volumes, иначе имя смонтированных Volumes будет сгенерировано, и сложно будет ориентироваться
    volumes:
      - kafka-cb-1-data:/var/lib/kafka/data
      - kafka-cb-1-secrets:/etc/kafka/secrets
    networks:
      - kafka-network
    ports:
      - "9094:9094"
  kafka-ui:
    # Подключим интерфейс для взаимодействия с Kafka
    image: provectuslabs/kafka-ui:v0.7.2
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS: "kafka-cb-1:9092"
      KAFKA_CLUSTERS_0_NAME: "kafka-kraft"
      DYNAMIC_CONFIG_ENABLED: 'true'
    networks:
      - kafka-network
volumes:
  kafka-cb-1-data:
  kafka-cb-1-secrets:
  
networks:
  kafka-network:
    driver: bridge
```

Для подключения UI к кластеру Кафки указали:
- `KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS: "kafka-cb-1:9092"`
- порт `9092` - потому что сервис UI так же находится внутри той же сети (в Докере), что и кластер Кафки. Поэтому UI может коммуницировать с кластером по "внутреннему каналу связи" (`PLAINTEXT://kafka-cb-1:9092`)

#### Развертывание кластера в Docker
- выполните команду `docker-compose -f docker-compose-node-1.yml up -d`
  - дождитесь завершения скачивания образов и создания контейнеров
- в результате увидите о том, что контейнера созданы и запущены:
```bash
✔ Network unit_1_kafka-network     Created
✔ Volume unit_1_kafka-cb-1-data    Created
✔ Volume unit_1_kafka-cb-1-secrets Created
✔ Container kafka-cb-1             Started
✔ Container unit_1-kafka-ui-1      Started
```
- для полного пересоздания контейнеров стоит не забыть удалить сеть (network) и тома (volumes) на тот случай, чтобы уже записанные в тома данные не повлияли на пересборку:
```bash 
docker stop kafka-cb-1 unit_1-kafka-ui-1 \
&& docker rm kafka-cb-1 unit_1-kafka-ui-1 \
&& docker network rm unit_1_kafka-network \
&& docker volume rm unit_1_kafka-cb-1-data unit_1_kafka-cb-1-secrets \
&& docker-compose -f docker-compose-node-1.yml up -d
```


#### Проверьте состояние Kafka с помощью UI и команд
- Теперь по адресу http://localhost:8080 у нас доступен интерфейс для управления Kafka - перейти по ссылке, увидеть:
    - `kafka-kraft` - как имя Кластера в колонке "Cluster name" (и он имеет статус Online)
    - В разделе [Brokers](http://localhost:8080/ui/clusters/kafka-kraft/brokers):
      - `Broker Count` - 1 брокер
      - `Active Controller` - 1 контроллер
- `docker exec -it kafka-cb-1 sh` - выполнить команду в терминале вашего ПК, чтобы зайти в контейнер нашей ноды кластера
- `kafka-topics --list --bootstrap-server kafka-cb-1:9092` - далее выполнить эту команду, находясь в командной оболочке контейнера - в результате будет выведена пустой список, так как топики ещё не созданы

### Кластер из 3-х нод (каждая нода выступает в роле брокера и контроллера)
- docker-compose-node-3.yml
```yml
services:
    kafka-cb-1:
        image: confluentinc/cp-kafka:8.3.0
        container_name: kafka-cb-1
        hostname: kafka-cb-1
        environment:
            CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
            KAFKA_NODE_ID: 1
            KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-cb-1:9093,2@kafka-cb-2:9093,3@kafka-cb-3:9093"
            KAFKA_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094"
            KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-cb-1:9092,EXTERNAL://127.0.0.1:9094"
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"
            KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
            KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
            KAFKA_PROCESS_ROLES: "controller,broker"
            KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
            KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
        volumes:
            - kafka-cb-1-data:/var/lib/kafka/data
            - kafka-cb-1-secrets:/etc/kafka/secrets
        networks:
            - kafka-network
        ports:
            - "19094:9094"
    kafka-cb-2:
        image: confluentinc/cp-kafka:8.3.0
        container_name: kafka-cb-2
        hostname: kafka-cb-2
        environment:
            CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
            KAFKA_NODE_ID: 2
            KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-cb-1:9093,2@kafka-cb-2:9093,3@kafka-cb-3:9093"
            KAFKA_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094"
            KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-cb-2:9092,EXTERNAL://127.0.0.1:9094"
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"
            KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
            KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
            KAFKA_PROCESS_ROLES: "controller,broker"
            KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
            KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
        volumes:
            - kafka-cb-2-data:/var/lib/kafka/data
            - kafka-cb-2-secrets:/etc/kafka/secrets
        networks:
            - kafka-network
        ports:
            - "29094:9094"
    kafka-cb-3:
        image: confluentinc/cp-kafka:8.3.0
        container_name: kafka-cb-3
        hostname: kafka-cb-3
        environment:
            CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
            KAFKA_NODE_ID: 3
            KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-cb-1:9093,2@kafka-cb-2:9093,3@kafka-cb-3:9093"
            KAFKA_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094"
            KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-cb-3:9092,EXTERNAL://127.0.0.1:9094"
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"
            KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
            KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
            KAFKA_PROCESS_ROLES: "controller,broker"
            KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
            KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
        volumes:
            - kafka-cb-3-data:/var/lib/kafka/data
            - kafka-cb-3-secrets:/etc/kafka/secrets
        networks:
            - kafka-network
        ports:
            - "39094:9094"
    kafka-ui:
        image: provectuslabs/kafka-ui:v0.7.2
        ports:
            - "8080:8080"
        environment:
            KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS: "kafka-cb-1:9092,kafka-cb-2:9092,kafka-cb-3:9092"
            KAFKA_CLUSTERS_0_NAME: "kafka-kraft"
            DYNAMIC_CONFIG_ENABLED: 'true'
        networks:
            - kafka-network
        depends_on:
            - kafka-cb-1
            - kafka-cb-2
            - kafka-cb-3
volumes:
    kafka-cb-1-data:
    kafka-cb-1-secrets:
    kafka-cb-2-data:
    kafka-cb-2-secrets:
    kafka-cb-3-data:
    kafka-cb-3-secrets:

networks:
    kafka-network:
        driver: bridge
```
#### Отличия от кластера с одной нодой
- создали три ноды, чтобы был кворум 2n+1 - в нашем случае (n = 3) 3 ноды
- для каждой ноды указали свой уникальный `KAFKA_NODE_ID`
- для каждой ноды указали список контроллеров для организации кворума `KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-cb-1:9093,2@kafka-cb-2:9093,3@kafka-cb-3:9093"`
- Для сервиса с UI указали весь список нод: `KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS: "kafka-cb-1:9092,kafka-cb-2:9092,kafka-cb-3:9092"`
- наружу из контейнеров мы пошарили порты:
  - для удобства добавили `1` у номера порта, который будет снаружи смотреть на порт`9094` 
  - таким образом, для подключения снаружи надо будет использовать порты: `19094, 29094, 39094` 
```
  - "19094:9094"
  - "29094:9094"
  - "39094:9094"
```
Для подключения UI к кластеру Кафки перечислили все три ноды:
- `KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS: "kafka-cb-1:9092,kafka-cb-3:9092,kafka-cb-3:9092"`
- порт `9092` - потому что сервис UI так же находится внутри той же сети (в Докере), что и кластер Кафки. Поэтому UI может коммуницировать с кластером по "внутреннему каналу связи" (`PLAINTEXT://kafka-cb-N:9092`)

#### Развертывание кластера в Docker
- выполните команду `docker-compose -f docker-compose-node-3.yml up -d`
    - дождитесь завершения скачивания образов и создания контейнеров
- в результате увидите о том, что контейнера созданы и запущены:
```bash
✔ Network unit_1_kafka-network     Created
✔ Volume unit_1_kafka-cb-2-data    Created
✔ Volume unit_1_kafka-cb-2-secrets Created
✔ Volume unit_1_kafka-cb-3-data    Created
✔ Volume unit_1_kafka-cb-3-secrets Created
✔ Volume unit_1_kafka-cb-1-data    Created
✔ Volume unit_1_kafka-cb-1-secrets Created
✔ Container unit_1-kafka-ui-1      Started
✔ Container kafka-cb-2             Started
✔ Container kafka-cb-3             Started
✔ Container kafka-cb-1             Started
```
- для полного пересоздания контейнеров стоит не забыть удалить сеть (network) и тома (volumes) на тот случай, чтобы уже записанные в тома данные не повлияли на пересборку:
```bash 
docker stop kafka-cb-1 kafka-cb-2 kafka-cb-3 unit_1-kafka-ui-1 \
&& docker rm kafka-cb-1 kafka-cb-2 kafka-cb-3 unit_1-kafka-ui-1 \
&& docker network rm unit_1_kafka-network \
&& docker volume rm unit_1_kafka-cb-1-data unit_1_kafka-cb-2-data unit_1_kafka-cb-3-data unit_1_kafka-cb-1-secrets unit_1_kafka-cb-2-secrets unit_1_kafka-cb-3-secrets \
&& docker-compose -f docker-compose-node-3.yml up -d
```

#### Проверьте состояние Kafka с помощью UI и команд
- Теперь по адресу http://localhost:8080 у нас доступен интерфейс для управления Kafka - перейти по ссылке, увидеть:
    - `kafka-kraft` - как имя Кластера в колонке "Cluster name" (и он имеет статус Online)
    - В разделе [Brokers](http://localhost:8080/ui/clusters/kafka-kraft/brokers):
        - `Broker Count` - 3 брокера
        - `Active Controller` - 1 активный контроллер
- Для каждой ноды выполните:
  - `docker exec -it kafka-cb-N sh` - (N: 1, 2, 3) выполнить команду в терминале вашего ПК, чтобы зайти в контейнер нашей ноды кластера
  - `kafka-topics --list --bootstrap-server kafka-cb-N:9092` - (N: 1, 2, 3) далее выполнить эту команду, находясь в командной оболочке контейнера - в результате будет выведена пустой список, так как топики ещё не созданы

## Кластер из 6-и нод (3 брокера и 3 контроллера)
- docker-compose-3c-3b.yml
```yml
services:
    kafka-c-1:
        image: confluentinc/cp-kafka:8.3.0
        # Для роли контроллера "KAFKA_ADVERTISED_LISTENERS" должна отсутствовать, но в поставке cp-kafka, она все равно создается. Поэтому вот так удаляем ее.
        entrypoint: [ "/bin/bash", "-c", "unset KAFKA_ADVERTISED_LISTENERS; exec /etc/confluent/docker/run" ]
        container_name: kafka-c-1
        hostname: kafka-c-1
        environment:
            CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
            KAFKA_NODE_ID: 1
            KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-c-1:9093,2@kafka-c-2:9093,3@kafka-c-3:9093"
            KAFKA_LISTENERS: "CONTROLLER://:9093"
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT"
            KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
            KAFKA_PROCESS_ROLES: "controller"
        volumes:
            - kafka-c-1-data:/var/lib/kafka/data
            - kafka-c-1-secrets:/etc/kafka/secrets
        networks:
            - kafka-network
    kafka-c-2:
        image: confluentinc/cp-kafka:8.3.0
        # Для роли контроллера "KAFKA_ADVERTISED_LISTENERS" должна отсутствовать, но в поставке cp-kafka, она все равно создается. Поэтому вот так удаляем ее.
        entrypoint: [ "/bin/bash", "-c", "unset KAFKA_ADVERTISED_LISTENERS; exec /etc/confluent/docker/run" ]
        container_name: kafka-c-2
        hostname: kafka-c-2
        environment:
            CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
            KAFKA_NODE_ID: 2
            KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-c-1:9093,2@kafka-c-2:9093,3@kafka-c-3:9093"
            KAFKA_LISTENERS: "CONTROLLER://:9093"
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT"
            KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
            KAFKA_PROCESS_ROLES: "controller"
        volumes:
            - kafka-c-2-data:/var/lib/kafka/data
            - kafka-c-2-secrets:/etc/kafka/secrets
        networks:
            - kafka-network
    kafka-c-3:
        image: confluentinc/cp-kafka:8.3.0
        # Для роли контроллера "KAFKA_ADVERTISED_LISTENERS" должна отсутствовать, но в поставке cp-kafka, она все равно создается. Поэтому вот так удаляем ее.
        entrypoint: [ "/bin/bash", "-c", "unset KAFKA_ADVERTISED_LISTENERS; exec /etc/confluent/docker/run" ]
        container_name: kafka-c-3
        hostname: kafka-c-3
        environment:
            CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
            KAFKA_NODE_ID: 3
            KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-c-1:9093,2@kafka-c-2:9093,3@kafka-c-3:9093"
            KAFKA_LISTENERS: "CONTROLLER://:9093"
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT"
            KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
            KAFKA_PROCESS_ROLES: "controller"
        volumes:
            - kafka-c-3-data:/var/lib/kafka/data
            - kafka-c-3-secrets:/etc/kafka/secrets
        networks:
            - kafka-network
    kafka-b-1:
        image: confluentinc/cp-kafka:8.3.0
        container_name: kafka-b-1
        hostname: kafka-b-1
        environment:
            CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
            KAFKA_NODE_ID: 4
            KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-c-1:9093,2@kafka-c-2:9093,3@kafka-c-3:9093"
            KAFKA_LISTENERS: "PLAINTEXT://:9092,EXTERNAL://:9094"
            KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-b-1:9092,EXTERNAL://127.0.0.1:9094"
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"
            KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
            KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
            KAFKA_PROCESS_ROLES: "broker"
            KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
            KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
        volumes:
            - kafka-b-1-data:/var/lib/kafka/data
            - kafka-b-1-secrets:/etc/kafka/secrets
        networks:
            - kafka-network
        ports:
            - "19094:9094"
    kafka-b-2:
        image: confluentinc/cp-kafka:8.3.0
        container_name: kafka-b-2
        hostname: kafka-b-2
        environment:
            CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
            KAFKA_NODE_ID: 5
            KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-c-1:9093,2@kafka-c-2:9093,3@kafka-c-3:9093"
            KAFKA_LISTENERS: "PLAINTEXT://:9092,EXTERNAL://:9094"
            KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-b-2:9092,EXTERNAL://127.0.0.1:9094"
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"
            KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
            KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
            KAFKA_PROCESS_ROLES: "broker"
            KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
            KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
        volumes:
            - kafka-b-2-data:/var/lib/kafka/data
            - kafka-b-2-secrets:/etc/kafka/secrets
        networks:
            - kafka-network
        ports:
            - "29094:9094"
    kafka-b-3:
        image: confluentinc/cp-kafka:8.3.0
        container_name: kafka-b-3
        hostname: kafka-b-3
        environment:
            CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
            KAFKA_NODE_ID: 6
            KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-c-1:9093,2@kafka-c-2:9093,3@kafka-c-3:9093"
            KAFKA_LISTENERS: "PLAINTEXT://:9092,EXTERNAL://:9094"
            KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-b-3:9092,EXTERNAL://127.0.0.1:9094"
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"
            KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
            KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
            KAFKA_PROCESS_ROLES: "broker"
            KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
            KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
        volumes:
            - kafka-b-3-data:/var/lib/kafka/data
            - kafka-b-3-secrets:/etc/kafka/secrets
        networks:
            - kafka-network
        ports:
            - "39094:9094"
    kafka-ui:
        image: provectuslabs/kafka-ui:v0.7.2
        ports:
            - "8080:8080"
        environment:
            KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS: "kafka-b-1:9092,kafka-b-2:9092,kafka-b-3:9092"
            KAFKA_CLUSTERS_0_NAME: "kafka-kraft"
            DYNAMIC_CONFIG_ENABLED: 'true'
        networks:
            - kafka-network
        depends_on:
            - kafka-c-1
            - kafka-c-2
            - kafka-c-3
            - kafka-b-1
            - kafka-b-2
            - kafka-b-3
volumes:
    kafka-c-1-data:
    kafka-c-1-secrets:
    kafka-c-2-data:
    kafka-c-2-secrets:
    kafka-c-3-data:
    kafka-c-3-secrets:
    kafka-b-1-data:
    kafka-b-1-secrets:
    kafka-b-2-data:
    kafka-b-2-secrets:
    kafka-b-3-data:
    kafka-b-3-secrets:

networks:
    kafka-network:
        driver: bridge
```
#### Отличия от кластера с одной нодой
- Для каждого контроллера пришлось удалять переменную окружения ``, так как она для контроллеров не нужна, и вызывает ошибку при старте контейнера для контроллеров.
  - При этом, в yml файле ее не прописывал для них. А при старте контейнера была ошибка о ее наличии, и что она не должна быть пустой. Если ее прописать в yml, то была ошибка о ее наличии.
  - В итоге увидел в инспектировании контейнера, что она там есть - и пустая. 
  - Документации на сайте ничего об этом явно не говорила (либо я плохо искал).
  - **Тут уже воспользовался ИИ, чтобы найти вот такое решение:**
  - `entrypoint: [ "/bin/bash", "-c", "unset KAFKA_ADVERTISED_LISTENERS; exec /etc/confluent/docker/run" ]`
- создали 6 нод, чтобы был кворум 2n+1 - в нашем случае (n = 3) 3 ноды для контроллеров и 3 для брокеров
- для каждой ноды указали свой уникальный `KAFKA_NODE_ID`
- для каждой ноды указали список контроллеров для организации кворума `KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-c-1:9093,2@kafka-c-2:9093,3@kafka-c-3:9093"`
- Для сервиса с UI указали весь список нод: `KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS: "kafka-b-1:9092,kafka-b-2:9092,kafka-b-3:9092"`
- наружу из контейнеров мы пошарили порты:
    - для удобства добавили `1` у номера порта, который будет снаружи смотреть на порт`9094`
    - таким образом, для подключения снаружи надо будет использовать порты: `19094, 29094, 39094`
```
  - "19094:9094"
  - "29094:9094"
  - "39094:9094"
```
Для подключения UI к кластеру Кафки перечислили все три ноды:
- `KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS: "kafka-b-1:9092,kafka-b-2:9092,kafka-b-3:9092"`
- порт `9092` - потому что сервис UI так же находится внутри той же сети (в Докере), что и кластер Кафки. Поэтому UI может коммуницировать с кластером по "внутреннему каналу связи" (`PLAINTEXT://kafka-cb-N:9092`)

#### Развертывание кластера в Docker 3c-3b
- выполните команду `docker-compose -f docker-compose-3c-3b.yml up -d`
    - дождитесь завершения скачивания образов и создания контейнеров
- в результате увидите о том, что контейнера созданы и запущены:
```bash
✔ Network unit_1_kafka-network
✔ Volume unit_1_kafka-c-1-data
✔ Volume unit_1_kafka-b-1-data
✔ Volume unit_1_kafka-c-1-secrets
✔ Volume unit_1_kafka-c-2-data
✔ Volume unit_1_kafka-c-2-secrets
✔ Volume unit_1_kafka-c-3-data
✔ Volume unit_1_kafka-c-3-secrets
✔ Volume unit_1_kafka-b-2-data
✔ Volume unit_1_kafka-b-1-secrets
✔ Volume unit_1_kafka-b-2-secrets
✔ Volume unit_1_kafka-b-3-data
✔ Volume unit_1_kafka-b-3-secrets
✔ Container kafka-b-3
✔ Container kafka-b-1
✔ Container kafka-c-1
✔ Container kafka-c-2
✔ Container kafka-b-2
✔ Container kafka-c-3
✔ Container unit_1-kafka-ui-1
```
- для полного пересоздания контейнеров стоит не забыть удалить сеть (network) и тома (volumes) на тот случай, чтобы уже записанные в тома данные не повлияли на пересборку:
```bash 
docker stop kafka-c-1 kafka-c-2 kafka-c-3 kafka-b-1 kafka-b-2 kafka-b-3 unit_1-kafka-ui-1 \
&& docker rm kafka-c-1 kafka-c-2 kafka-c-3 kafka-b-1 kafka-b-2 kafka-b-3 unit_1-kafka-ui-1 \
&& docker network rm unit_1_kafka-network \
&& docker volume rm unit_1_kafka-c-1-data unit_1_kafka-c-1-secrets unit_1_kafka-c-2-data unit_1_kafka-c-2-secrets unit_1_kafka-c-3-data unit_1_kafka-c-3-secrets unit_1_kafka-b-1-data unit_1_kafka-b-1-secrets unit_1_kafka-b-2-data unit_1_kafka-b-2-secrets unit_1_kafka-b-3-data unit_1_kafka-b-3-secrets \
&& docker-compose -f docker-compose-3c-3b.yml up -d
```

#### Проверьте состояние Kafka с помощью UI и команд
- Теперь по адресу http://localhost:8080 у нас доступен интерфейс для управления Kafka - перейти по ссылке, увидеть:
    - `kafka-kraft` - как имя Кластера в колонке "Cluster name" (и он имеет статус Online)
    - В разделе [Brokers](http://localhost:8080/ui/clusters/kafka-kraft/brokers):
        - `Broker Count` - 3 брокера
        - `Active Controller` - до 6 активный контроллер (вероятно kafka-ui так мониторит)
- Для каждой ноды выполните:
    - `docker exec -it kafka-b-N sh` - (N: 1, 2, 3) выполнить команду в терминале вашего ПК, чтобы зайти в контейнер нашей ноды кластера
    - `kafka-topics --list --bootstrap-server kafka-b-N:9092` - (N: 1, 2, 3) далее выполнить эту команду, находясь в командной оболочке контейнера - в результате будет выведена пустой список, так как топики ещё не созданы