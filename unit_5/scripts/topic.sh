#!/bin/bash
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo "${YELLOW}Create topic-1${NC}"
docker exec -it kafka-b-1 kafka-topics \
  --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
  --command-config /etc/kafka/secrets/admin/admin-client.properties \
  --create \
  --topic topic-1 \
  --partitions 3 \
  --replication-factor 3

echo "${YELLOW}Create topic-2${NC}"
docker exec -it kafka-b-1 kafka-topics \
  --bootstrap-server kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093 \
  --command-config /etc/kafka/secrets/admin/admin-client.properties \
  --create \
  --topic topic-2 \
  --partitions 3 \
  --replication-factor 3