#!/bin/bash

YELLOW='\033[0;33m'
NC='\033[0m' # No Color

COMMAND_CONFIG="/etc/kafka/secrets/admin/admin-client.properties"
BOOTSTRAP_SERVER="kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093"

echo "${YELLOW}Дадим Kafka-UI права на все топики${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=kafka-ui,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Describe \
--topic "*"

sleep 2

echo "\n"
echo "${YELLOW}topic-1: Доступен как для продюсеров, так и для консьюмеров.${NC}"
echo "${YELLOW}Дадим producer права на запись в топик:${NC}"

docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=producer,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Write \
--operation Describe \
--topic "topic-1"

sleep 2

echo "\n"
echo "${YELLOW}Дадим consumer права на чтение из топика:${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=consumer,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Read \
--operation Describe \
--topic "topic-1"

sleep 2

echo "\n"
echo "${YELLOW}topic-2:${NC}"
echo "${YELLOW} - Продюсеры могут отправлять сообщения.${NC}"
echo "${YELLOW} - Консьюмеры не имеют доступа к чтению данных.${NC}"
echo "${YELLOW}Дадим producer права на запись в топик:${NC}"

EOF
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=producer,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Write \
--operation Describe \
--topic "topic-2"

sleep 2

echo "\n"
echo "${YELLOW}Дадим consumer права на топик (только для метаданных):${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=consumer,L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Describe \
--topic "topic-2"

sleep 2

echo "\n"
echo "${YELLOW}Посмотрим список прав - какие кому выдали:${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--list \
--topic "topic-1" \
--topic "topic-2"

docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--list \
--topic "*"