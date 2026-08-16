# Развернуть окружение

будет:
- развернуто окружение
- созданы топики
- загружены настройки для коннектора
- проверен его статус
- получен список коннекторов

```bash
clear \
&& echo "ssl create\n" \
&& sh ./scripts/certs.sh \
&& docker-compose down \
&& docker-compose down -v \
&& docker-compose up -d
```

## Пересобрать, если требуется

```bash
docker stop kafka-b-3 kafka-b-2 kafka-c-1 kafka-c-3 kafka-b-1 kafka-c-2 unit_5-kafka-ui-1 \
&& docker rm kafka-b-3 kafka-b-2 kafka-c-1 kafka-c-3 kafka-b-1 kafka-c-2 unit_5-kafka-ui-1 \
&& docker network rm unit_5_kafka-network \
&& docker volume rm unit_5_kafka-c-2-data unit_5_kafka-b-2-data unit_5_kafka-c-3-data unit_5_kafka-c-1-data unit_5_kafka-b-1-data unit_5_kafka-b-3-data \
&& echo "ssl create\n" \
&& sh ./scripts/generate_ssl.sh \
&& sh ./scripts/admin_client.sh \
&& docker-compose up -d \
&& sleep 20 \
&& docker-compose stop
```