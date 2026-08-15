#!/bin/bash

set -e

# Ждем старта Kafka
echo "wait for 30 seconds when environment is ready:\n"
sleep 30

echo "topic create:\n"
docker exec -it kafka-b-1 kafka-topics --create \
    --topic topic-1 \
    --partitions 3 \
    --replication-factor 2 \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --command-config /etc/kafka/ssl/admin-client.conf

docker exec -it kafka-b-1 kafka-topics --create \
    --topic topic-2 \
    --partitions 3 \
    --replication-factor 2 \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --command-config /etc/kafka/ssl/admin-client.conf

# Настраиваем ACL для topic-1 (все доступно)
docker exec -it kafka-b-1 kafka-acls --add \
    --allow-principal "User:CN=kafka-client" \
    --operation Read --operation Write --operation Describe \
    --topic topic-1 \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --command-config /etc/kafka/ssl/admin-client.conf

# Настраиваем ACL для topic-2 (только продюсеры)
docker exec -it kafka-b-1 kafka-acls --add \
    --allow-principal "User:CN=kafka-client" \
    --operation Write --operation Describe \
    --topic topic-2 \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --command-config /etc/kafka/ssl/admin-client.conf

# Запрещаем чтение для topic-2
docker exec -it kafka-b-1 kafka-acls --add \
    --deny-principal "User:CN=kafka-client" \
    --operation Read \
    --topic topic-2 \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --command-config /etc/kafka/ssl/admin-client.conf

echo "=== Kafka setup completed ==="