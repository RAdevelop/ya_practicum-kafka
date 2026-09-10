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

################################################################## client-api

echo "${YELLOW}Дадим ${USER_CLIENT_API} права на consumer groups${NC}"
docker exec -it kafka-b-1 kafka-acls \
  --command-config ${COMMAND_CONFIG} \
  --bootstrap-server ${BOOTSTRAP_SERVER} \
  --add \
  --allow-principal "User:CN=${USER_CLIENT_API},L=Moscow,OU=Practice,O=Yandex,C=RU" \
  --operation Describe \
  --operation Read \
  --group "*"

echo "${YELLOW}Дадим ${USER_CLIENT_API} права на чтение топика ${TOPIC_CLIENT_SEARCH}${NC}"
docker exec -it kafka-b-1 kafka-acls \
  --command-config ${COMMAND_CONFIG} \
  --bootstrap-server ${BOOTSTRAP_SERVER} \
  --add \
  --allow-principal "User:CN=${USER_CLIENT_API},L=Moscow,OU=Practice,O=Yandex,C=RU" \
  --operation Read \
  --operation Write \
  --operation Describe \
  --topic ${TOPIC_CLIENT_SEARCH}

echo "${YELLOW}Дадим ${USER_CLIENT_API} права на чтение топика recommendations${NC}"
docker exec -it kafka-b-1 kafka-acls \
  --command-config ${COMMAND_CONFIG} \
  --bootstrap-server ${BOOTSTRAP_SERVER} \
  --add \
  --allow-principal "User:CN=${USER_CLIENT_API},L=Moscow,OU=Practice,O=Yandex,C=RU" \
  --operation Read \
  --operation Describe \
  --operation DescribeConfigs \
  --topic ${TOPIC_RECOMMENDATIONS}

echo "${YELLOW}Дадим ${USER_CLIENT_API} права на чтение table-топиков group-*${NC}"
docker exec -it kafka-b-1 kafka-acls \
  --command-config ${COMMAND_CONFIG} \
  --bootstrap-server ${BOOTSTRAP_SERVER} \
  --add \
  --allow-principal "User:CN=${USER_CLIENT_API},L=Moscow,OU=Practice,O=Yandex,C=RU" \
  --operation Read \
  --operation Describe \
  --operation DescribeConfigs \
  --topic "group-" \
  --resource-pattern-type PREFIXED


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

# Права на чтение products-published
docker exec -it kafka2-b-1 kafka-acls \
  --command-config ${COMMAND_CONFIG} \
  --bootstrap-server ${BOOTSTRAP_SERVER2} \
  --add --allow-principal "User:CN=${USER_KAFKA_CONNECT},L=Moscow,OU=Practice,O=Yandex,C=RU" \
  --operation READ \
  --operation DESCRIBE \
  --topic ${TOPIC_PRODUCTS_PUBLISHED}

# Права на внутренние топики нового воркера (префикс hdfs-connect-)
docker exec -it kafka2-b-1 kafka-acls \
  --command-config ${COMMAND_CONFIG} \
  --bootstrap-server ${BOOTSTRAP_SERVER2} \
  --add --allow-principal "User:CN=${USER_KAFKA_CONNECT},L=Moscow,OU=Practice,O=Yandex,C=RU" \
  --operation READ \
  --operation WRITE \
  --operation CREATE \
  --operation DESCRIBE \
  --topic "hdfs-connect-" \
  --resource-pattern-type PREFIXED

# Права на consumer group
docker exec -it kafka2-b-1 kafka-acls \
  --command-config ${COMMAND_CONFIG} \
  --bootstrap-server ${BOOTSTRAP_SERVER2} \
  --add --allow-principal "User:CN=${USER_KAFKA_CONNECT},L=Moscow,OU=Practice,O=Yandex,C=RU" \
  --operation READ \
  --operation DESCRIBE \
  --group "connect-hdfs-" \
  --resource-pattern-type PREFIXED

# Права на consumer group
docker exec -it kafka2-b-1 kafka-acls \
  --command-config ${COMMAND_CONFIG} \
  --bootstrap-server ${BOOTSTRAP_SERVER2} \
  --add --allow-principal "User:CN=${USER_KAFKA_CONNECT},L=Moscow,OU=Practice,O=Yandex,C=RU" \
  --operation READ \
  --operation DESCRIBE \
  --group "connect-hdfs3-" \
  --resource-pattern-type PREFIXED

docker exec -it kafka2-b-1 kafka-acls \
  --command-config ${COMMAND_CONFIG} \
  --bootstrap-server ${BOOTSTRAP_SERVER2} \
  --add --allow-principal "User:CN=${USER_KAFKA_CONNECT},L=Moscow,OU=Practice,O=Yandex,C=RU" \
  --operation READ \
  --operation DESCRIBE \
  --group "connect-hdfs3-sink-products"

docker exec -it kafka2-b-1 kafka-acls \
  --command-config ${COMMAND_CONFIG} \
  --bootstrap-server ${BOOTSTRAP_SERVER2} \
  --add --allow-principal "User:CN=${USER_KAFKA_CONNECT},L=Moscow,OU=Practice,O=Yandex,C=RU" \
  --operation WRITE \
  --operation DESCRIBE \
  --topic ${TOPIC_RECOMMENDATIONS}
