#!/bin/bash
# setup-kafka-full.sh

set -e

echo "=== Setting up Kafka topics and ACLs ==="

# Ждем, пока кластер полностью запустится
echo "Waiting for Kafka cluster to be ready..."
sleep 30

# Создаем топики
echo "Creating topics..."
docker exec -it kafka-b-1 kafka-topics \
    --create \
    --topic topic-1 \
    --partitions 3 \
    --replication-factor 2 \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --command-config /etc/kafka/secrets/creds/admin-client.conf || true

docker exec -it kafka-b-1 kafka-topics \
    --create \
    --topic topic-2 \
    --partitions 3 \
    --replication-factor 2 \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --command-config /etc/kafka/secrets/creds/admin-client.conf || true

# Настраиваем ACL для topic-1
echo "Setting up ACL for topic-1..."
docker exec -it kafka-b-1 kafka-acls \
    --add \
    --allow-principal User:producer-user \
    --operation Write \
    --operation Describe \
    --topic topic-1 \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --command-config /etc/kafka/secrets/creds/admin-client.conf

docker exec -it kafka-b-1 kafka-acls \
    --add \
    --allow-principal User:consumer-user \
    --operation Read \
    --operation Describe \
    --topic topic-1 \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --command-config /etc/kafka/secrets/creds/admin-client.conf

# Настраиваем ACL для topic-2
echo "Setting up ACL for topic-2..."
docker exec -it kafka-b-1 kafka-acls \
    --add \
    --allow-principal User:producer-user \
    --operation Write \
    --operation Describe \
    --topic topic-2 \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --command-config /etc/kafka/secrets/creds/admin-client.conf

docker exec -it kafka-b-1 kafka-acls \
    --add \
    --deny-principal User:consumer-user \
    --operation Read \
    --topic topic-2 \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --command-config /etc/kafka/secrets/creds/admin-client.conf

# Проверяем ACL
echo "Verifying ACLs..."
docker exec -it kafka-b-1 kafka-acls \
    --list \
    --topic topic-1 \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --command-config /etc/kafka/secrets/creds/admin-client.conf

docker exec -it kafka-b-1 kafka-acls \
    --list \
    --topic topic-2 \
    --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
    --command-config /etc/kafka/secrets/creds/admin-client.conf

echo "=== Setup completed successfully! ==="