#!/bin/bash

YELLOW='\033[0;33m'
NC='\033[0m' # No Color

CACERT="./mount_dir/kafka-connect/creds/truststore.pem"
CERT="./mount_dir/kafka-connect/creds/keystore.pem"
KEY="./mount_dir/kafka-connect/creds/keystore.key"

################################ kafka-connect

######## FileStreamSinkConnector
echo "${YELLOW}Put connectors file-sink-products${NC}"

curl -s -X PUT https://localhost:8083/connectors/file-sink-products/config \
  --cacert ${CACERT} \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "tasks.max": "1",
    "topics": "'"${TOPIC_PRODUCTS_PUBLISHED}"'",
    "file": "/var/lib/kafka-connect-data/products_published.jsonl",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
    "value.converter.schema.registry.url": "https://schema-registry:8081",
    "value.converter.schema.registry.ssl.truststore.location": "/etc/kafka/secrets/truststore.jks",
    "value.converter.schema.registry.ssl.truststore.password": "'"${CA_PASS}"'",
    "value.converter.schema.registry.ssl.keystore.location": "/etc/kafka/secrets/keystore.pkcs12",
    "value.converter.schema.registry.ssl.keystore.password": "'"${CA_PASS}"'",
    "value.converter.schema.registry.ssl.key.password": "'"${CA_PASS}"'"
  }' | jq


# Статус коннектора — RUNNING или FAILED
sleep 10
echo "\n"
echo "${YELLOW}connectors/file-sink-products/status${NC}"
curl -s https://localhost:8083/connectors/file-sink-products/status \
--cacert ${CACERT} | jq

# Список всех коннекторов
echo "\n"
echo "${YELLOW}connectors${NC}"
curl -s https://localhost:8083/connectors \
--cacert ${CACERT} | jq

# Конфигурация коннектора
echo "\n"
echo "${YELLOW}connectors/file-sink-products/config${NC}"
curl -s https://localhost:8083/connectors/file-sink-products/config \
--cacert ${CACERT} | jq