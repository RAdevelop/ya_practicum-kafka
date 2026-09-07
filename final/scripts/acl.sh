#!/bin/bash

YELLOW='\033[0;33m'
NC='\033[0m' # No Color


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

########################################################################## FOR MIRROR

echo "\n"
echo "${YELLOW}2й кластер - Дадим ${USER_SPARK} права на работы с топиками:${NC}"
docker exec -it kafka2-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER2} \
--allow-principal "User:CN=${USER_SPARK},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--add \
--operation Read \
--operation Describe \
--topic ${TOPIC_PRODUCTS}

echo "\n"
echo "${YELLOW}2й кластер - Дадим ${USER_SPARK} права на работы с топиками:${NC}"
docker exec -it kafka2-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER2} \
--allow-principal "User:CN=${USER_SPARK},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--add \
--operation Write \
--operation Read \
--operation Describe \
--topic ${TOPIC_RECOMMENDATIONS}


################################################################## kafka-connect
echo "\n"
echo "${YELLOW}Дадим ${USER_KAFKA_CONNECT} права на первом кластере на чтение ${TOPIC_PRODUCTS_PUBLISHED}:${NC}"
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add --allow-principal "User:CN=${USER_KAFKA_CONNECT},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation READ \
--operation DESCRIBE \
--topic ${TOPIC_PRODUCTS_PUBLISHED}


# Права на внутренние топики Connect
docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add --allow-principal "User:CN=${USER_KAFKA_CONNECT},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation READ \
--operation WRITE \
--operation CREATE \
--operation DESCRIBE \
--topic "connect-" \
--resource-pattern-type PREFIXED

docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add --allow-principal "User:CN=${USER_KAFKA_CONNECT},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation READ \
--operation DESCRIBE \
--group "connect-" \
--resource-pattern-type PREFIXED

docker exec -it kafka-b-1 kafka-acls \
--command-config ${COMMAND_CONFIG} \
--bootstrap-server ${BOOTSTRAP_SERVER} \
--add --allow-principal "User:CN=${USER_KAFKA_CONNECT},L=Moscow,OU=Practice,O=Yandex,C=RU" \
--operation READ \
--operation DESCRIBE \
--group connect-file-sink-products