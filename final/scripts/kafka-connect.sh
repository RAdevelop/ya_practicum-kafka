#!/bin/bash

set -euo pipefail

YELLOW='\033[0;33m'
NC='\033[0m' # No Color

CACERT="./mount_dir/kafka-connect/creds/truststore.pem"
CERT="./mount_dir/kafka-connect/creds/keystore.pem"
KEY="./mount_dir/kafka-connect/creds/keystore.key"



start_time=$SECONDS
max_seconds=700
i=0
ready=false
echo "${YELLOW}Waiting for Kafka Connect (max ${max_seconds} seconds)...${NC}"
while (( SECONDS - start_time < max_seconds )); do
  ((i++))
  if curl -sk \
      --cacert "$CACERT" \
      https://localhost:8083/connectors >/dev/null 2>&1; then
    echo "Kafka Connect (8083) is ready!"
    ready=true
    break
  fi
  echo "  [$i] Not ready yet... (elapsed: $((SECONDS - start_time))s)"
  sleep 5
done

if [[ "$ready" != "true" ]]; then
  echo "ERROR: Kafka Connect did not start within ${max_seconds} seconds!"
  exit 1
fi

# Ждём второй Connect (HDFS) на порту 8084
echo "${YELLOW}Waiting for Kafka Connect HDFS (8084) (max ${max_seconds} seconds)...${NC}"

start_time=$SECONDS
i=0
ready_hdfs=false

while (( SECONDS - start_time < max_seconds )); do
  ((i++))
  if curl -sk \
      --cacert "$CACERT" \
      https://localhost:8084/connectors >/dev/null 2>&1; then
    echo "Kafka Connect HDFS (8084) is ready!"
    ready_hdfs=true
    break
  fi
  echo "  [$i] Not ready yet... (elapsed: $((SECONDS - start_time))s)"
  sleep 5
done

if [[ "$ready_hdfs" != "true" ]]; then
  echo "ERROR: Kafka Connect HDFS did not start within ${max_seconds} seconds!"
  exit 1
fi

sleep 10

################################ kafka-connect

######## FileStreamSinkConnector
#curl -sk -X DELETE --cacert ${CACERT} https://localhost:8083/connectors/file-sink-products

echo "${YELLOW}Put connectors file-sink-products${NC}"

curl -s -X PUT https://localhost:8083/connectors/file-sink-products/config \
  --cacert ${CACERT} \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "tasks.max": "1",
    "topics": "'"${TOPIC_PRODUCTS_PUBLISHED}"'",
    "file": "/var/lib/kafka-connect-data/'${TOPIC_PRODUCTS_PUBLISHED}'.jsonl"
  }' | jq

# Статус коннектора
printf "\n"
echo "${YELLOW}connectors/file-sink-products/status${NC}"
for s in $(seq 1 10); do
  resp=$(curl -s https://localhost:8083/connectors/file-sink-products/status --cacert ${CACERT})
  if echo "$resp" | jq -e . >/dev/null 2>&1 && ! echo "$resp" | jq -e '.error_code' >/dev/null 2>&1; then
    echo "$resp" | jq
    break
  fi
  echo "  [$s/10] Status not ready, waiting..."
  sleep 5
done

# Список всех коннекторов
printf "\n"
echo "${YELLOW}connectors${NC}"
curl -s https://localhost:8083/connectors \
--cacert ${CACERT} | jq

# Конфигурация коннектора
printf "\n"
echo "${YELLOW}connectors/file-sink-products/config${NC}"
curl -s https://localhost:8083/connectors/file-sink-products/config \
--cacert ${CACERT} | jq

################################ kafka-connect-hdfs
####### Hdfs3SinkConnector

#curl -sk -X DELETE --cacert ${CACERT} https://localhost:8084/connectors/hdfs3-sink-products

printf "\n"
echo "${YELLOW}POST connectors Hdfs3SinkConnector${NC}"
curl -s -X POST https://localhost:8084/connectors \
  -H "Content-Type: application/json" \
  --cacert ${CACERT} \
  -d '{
    "name": "hdfs3-sink-products",
    "config": {
      "connector.class": "io.confluent.connect.hdfs3.Hdfs3SinkConnector",
      "tasks.max": "1",
      "topics": "'"${TOPIC_PRODUCTS_PUBLISHED}"'",
      "topics.dir": "topics",
      "hdfs.url": "hdfs://hdfs-namenode:9000",
      "hadoop.conf.dir": "/etc/hadoop/conf",
      "format.class": "io.confluent.connect.hdfs3.json.JsonFormat",
      "flush.size": "3",
      "rotate.interval.ms": "60000",
      "schema.compatibility": "NONE",
      "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
      "log.dir": "/tmp/logs",
      "locale": "en",
      "timezone": "UTC",
      "confluent.topic.bootstrap.servers": "'"${BOOTSTRAP_SERVER2}"'",
      "confluent.topic.replication.factor": "1",
      "confluent.topic.security.protocol": "SSL",
      "confluent.topic.ssl.truststore.location": "/etc/kafka/secrets/truststore.jks",
      "confluent.topic.ssl.truststore.password": "'"${CA_PASS}"'",
      "confluent.topic.ssl.keystore.location": "/etc/kafka/secrets/keystore.pkcs12",
      "confluent.topic.ssl.keystore.password": "'"${CA_PASS}"'",
      "confluent.topic.ssl.key.password": "'"${CA_PASS}"'"
    }
  }' | jq


#По параметрам:
#"topics.dir": "topics" - имя "папки", куда будут складываться файлы в hdfs
#format.class — JsonFormat, потому что ты используешь JsonSchemaConverter. Если бы был AvroConverter — нужен был бы AvroFormat.
#flush.size — 3: каждый 3-й сообщение триггерит запись файла в HDFS. Маленькое число для быстрой проверки; в проде обычно 100+.
#store.url — адрес NameNode, как в core-site.xml.
#hadoop.conf.dir — смонтированные конфиги Hadoop (core-site.xml, hdfs-site.xml).
#Конвертеры (value.converter, schema.registry.*) не указываем — они наследуются из worker-конфига.

# Статус коннектора — RUNNING или FAILED
printf "\n"
echo "${YELLOW}connectors/hdfs3-sink-products/status${NC}"
for s in $(seq 1 10); do
  resp=$(curl -s https://localhost:8084/connectors/hdfs3-sink-products/status --cacert ${CACERT})
  if echo "$resp" | jq -e . >/dev/null 2>&1 && ! echo "$resp" | jq -e '.error_code' >/dev/null 2>&1; then
    echo "$resp" | jq
    break
  fi
  echo "  [$s/10] Status not ready, waiting..."
  sleep 5
done


# Список всех коннекторов
printf "\n"
echo "${YELLOW}connectors${NC}"
curl -s https://localhost:8084/connectors \
--cacert ${CACERT} | jq

# Конфигурация коннектора
printf "\n"
echo "${YELLOW}connectors/hdfs3-sink-products/config${NC}"
curl -s https://localhost:8084/connectors/hdfs3-sink-products/config \
--cacert ${CACERT} | jq

printf "\n"
