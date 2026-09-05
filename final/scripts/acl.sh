#!/bin/bash

YELLOW='\033[0;33m'
NC='\033[0m' # No Color

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

############################################################ User shop-api
echo "\n"
echo "${YELLOW}Топик '${TOPIC_PRODUCTS}': Доступен для ${USER_SHOP_API}.${NC}"
echo "${YELLOW}Дадим ${USER_SHOP_API} права на запись в топик:${NC}"

docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add \
--allow-principal "User:CN=${USER_SHOP_API},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation Write \
--operation Describe \
--topic ${TOPIC_PRODUCTS}
