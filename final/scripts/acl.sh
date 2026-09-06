#!/bin/bash

YELLOW='\033[0;33m'
NC='\033[0m' # No Color

############################################################ User schema-registry
echo "${YELLOW}Дадим ${USER_SCHEMA_REGISTRY} права на группу 'schema-registry' ${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=${USER_SCHEMA_REGISTRY},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation All \
--group "schema-registry"

echo "${YELLOW}Дадим ${USER_SCHEMA_REGISTRY} права на топик '_schemas'${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=${USER_SCHEMA_REGISTRY},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation All \
--topic "_schemas"

############################################################ User kafka-ui
echo "${YELLOW}Дадим ${USER_KAFKA_UI} права на Describe cluster${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=${USER_KAFKA_UI},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Describe \
--cluster

echo "${YELLOW}Дадим ${USER_KAFKA_UI} права на Describe group${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=${USER_KAFKA_UI},L=Moscow,OU=Practice,O=Yandex,C=RU" \
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

############################################################ User shop-api

echo "${YELLOW}Дадим ${USER_SHOP_API} права на Describe group${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=${USER_SHOP_API},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Describe \
--operation Read \
--group "*"

echo "\n"
echo "${YELLOW}Топик '${TOPIC_PRODUCTS}': Доступен для ${USER_SHOP_API}.${NC}"
echo "${YELLOW}Дадим ${USER_SHOP_API} права на запись и чтение в топик:${NC}"

docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=${USER_SHOP_API},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Read \
--operation Write \
--operation Describe \
--topic ${TOPIC_PRODUCTS}

echo "\n"
echo "${YELLOW}Топик '${TOPIC_PRODUCTS_BLOCKED}': Доступен для ${USER_SHOP_API}.${NC}"
echo "${YELLOW}Дадим ${USER_SHOP_API} права на запись и чтение в топик:${NC}"

docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=${USER_SHOP_API},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Read \
--operation Write \
--operation Describe \
--topic ${TOPIC_PRODUCTS_BLOCKED}

echo "\n"
echo "${YELLOW}Топик '${TOPIC_PRODUCTS_PUBLISHED}': Доступен для ${USER_SHOP_API}.${NC}"
echo "${YELLOW}Дадим ${USER_SHOP_API} права на запись и чтение в топик:${NC}"

docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=${USER_SHOP_API},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Read \
--operation Write \
--operation Describe \
--topic ${TOPIC_PRODUCTS_PUBLISHED}

echo "\n"
echo "${YELLOW}Дадим ${USER_SHOP_API} права на работы с топиками групп group-*:${NC}"

docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=${USER_SHOP_API},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Alter \
--operation Create \
--operation Read \
--operation Write \
--operation Describe \
--operation DescribeConfigs \
--topic "group-" \
--resource-pattern-type PREFIXED
