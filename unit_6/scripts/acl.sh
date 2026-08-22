#!/bin/bash

YELLOW='\033[0;33m'
NC='\033[0m' # No Color

COMMAND_CONFIG="/etc/kafka/secrets/admin/admin-client.properties"
BOOTSTRAP_SERVER="kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093"
TOPIC_NAME="metric"

############################################################ User schema-registry
echo "${YELLOW}Дадим schema-registry права на группу 'schema-registry' ${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=schema-registry,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation All \
--group "schema-registry"

echo "${YELLOW}Дадим schema-registry права на топик '_schemas'${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=schema-registry,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation All \
--topic "_schemas"

############################################################ User kafka-ui
echo "${YELLOW}Дадим Kafka-UI права на Describe cluster${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=kafka-ui,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Describe \
--cluster

echo "${YELLOW}Дадим Kafka-UI права на Describe group${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=kafka-ui,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Describe \
--operation Read \
--group "*"

echo "${YELLOW}Дадим Kafka-UI права Describe,Read на все топики${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=kafka-ui,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Describe \
--operation Read \
--topic "*"

echo "\n"
echo "${YELLOW}Топик '${TOPIC_NAME}': Доступен как для продюсеров, так и для консьюмеров.${NC}"
echo "${YELLOW}Дадим producer права на запись в топик:${NC}"

############################################################ User producer
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=producer,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Write \
--operation Describe \
--topic ${TOPIC_NAME}


############################################################ User consumer
echo "\n"
echo "${YELLOW}Дадим consumer права на чтение из группы:${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=consumer,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Read \
--operation Describe \
--group "*"

echo "${YELLOW}Дадим consumer права на чтение из топика:${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=consumer,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Read \
--operation Describe \
--topic ${TOPIC_NAME}
