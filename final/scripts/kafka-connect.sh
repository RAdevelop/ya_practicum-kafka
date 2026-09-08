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

################################ kafka-connect-hdfs
####### Hdfs3SinkConnector
echo "${YELLOW}POST connectors Hdfs3SinkConnector${NC}"
curl -X POST https://localhost:8084/connectors \
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
      "confluent.topic.bootstrap.servers": "kafka2-b-1:9093,kafka2-b-2:9093,kafka2-b-3:9093",
      "confluent.topic.replication.factor": "3",
      "confluent.topic.security.protocol": "SSL",
      "confluent.topic.ssl.truststore.location": "/etc/kafka/secrets/truststore.jks",
      "confluent.topic.ssl.truststore.password": "'"${CA_PASS}"'",
      "confluent.topic.ssl.keystore.location": "/etc/kafka/secrets/keystore.pkcs12",
      "confluent.topic.ssl.keystore.password": "'"${CA_PASS}"'",
      "confluent.topic.ssl.key.password": "'"${CA_PASS}"'"
    }
  }'



#По параметрам:
#"topics.dir": "topics" - имя "папки", куда будут скалыдваться файлы в hdfs
#format.class — JsonFormat, потому что ты используешь JsonSchemaConverter. Если бы был AvroConverter — нужен был бы AvroFormat.
#flush.size — 3: каждый 3-й сообщение триггерит запись файла в HDFS. Маленькое число для быстрой проверки; в проде обычно 100+.
#store.url — адрес NameNode, как в core-site.xml.
#hadoop.conf.dir — смонтированные конфиги Hadoop (core-site.xml, hdfs-site.xml).
#Конвертеры (value.converter, schema.registry.*) не указываем — они наследуются из worker-конфига.

# Статус коннектора — RUNNING или FAILED
sleep 10
echo "\n"
echo "${YELLOW}connectors/hdfs3-sink-products/status${NC}"
curl -s https://localhost:8084/connectors/hdfs3-sink-products/status \
--cacert ${CACERT} | jq

# Список всех коннекторов
echo "\n"
echo "${YELLOW}connectors${NC}"
curl -s https://localhost:8084/connectors \
--cacert ${CACERT} | jq

# Конфигурация коннектора
echo "\n"
echo "${YELLOW}connectors/hdfs3-sink-products/config${NC}"
curl -s https://localhost:8084/connectors/hdfs3-sink-products/config \
--cacert ${CACERT} | jq