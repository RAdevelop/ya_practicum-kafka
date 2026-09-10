#!/bin/bash
YELLOW='\033[0;33m'
NC='\033[0m' # No Color


create_topic() {
  local BROKER=$1
  local SERVER=$2
  local TOPIC_NAME=$3
  local PARTITIONS=${4:-3}  # ← если не передано — используем 3
  local CLEANUP_POLICY=${5:-delete}  # ← если не передано — cleanup.policy=delete

  echo "${YELLOW}Create topic: ${TOPIC_NAME}${NC}"
  echo "${YELLOW}On BROKER: ${BROKER} and SERVER: ${SERVER}${NC}"
  #cleanup.policy=compact
  # - delete   - Удаляет старые данные по времени или размеру
  # - compact  - Сохраняет только последнее значение для каждого ключа
  # - compact,delete - Сначала compact, потом удаляет (delete) по времени
  # retention.ms=604800000 - Время хранения данных в топике в миллисекундах (7 дней)
  # segment.bytes=536870912 - Максимальный размер файла сегмента лога в байтах (512 МБ)
  # min.insync.replicas=3 - минимальное число реплик (в синхронном состоянии), которые должны подтвердить получение сообщения для выполнения успешной записи
  docker exec -it ${BROKER} kafka-topics \
  --command-config ${COMMAND_CONFIG} \
  --bootstrap-server ${SERVER} \
  --create \
  --topic ${TOPIC_NAME} \
  --partitions ${PARTITIONS} \
  --replication-factor 1 \
  --config cleanup.policy=${CLEANUP_POLICY} \
  --config retention.ms=604800000 \
  --config segment.bytes=536870912 \
  --config min.insync.replicas=1

  echo "\n"
  echo "${YELLOW}Describe topic: ${TOPIC_NAME}${NC}"
  docker exec -it kafka-b-1 kafka-topics \
  --command-config ${COMMAND_CONFIG} \
  --bootstrap-server ${BOOTSTRAP_SERVER} \
  --describe \
  --topic ${TOPIC_NAME}
}

for t in ${TOPIC_PRODUCTS} ${TOPIC_PRODUCTS_PUBLISHED} ${TOPIC_CLIENT_SEARCH}; do
  create_topic "kafka-b-1" ${BOOTSTRAP_SERVER} ${t} 1 "delete"
done

create_topic "kafka-b-1" ${BOOTSTRAP_SERVER} ${TOPIC_PRODUCTS_BLOCKED} 1 "delete"
create_topic "kafka-b-1" ${BOOTSTRAP_SERVER} ${TOPIC_RECOMMENDATIONS} 1 "compact"


for t in ${TOPIC_PRODUCTS} ${TOPIC_PRODUCTS_PUBLISHED} ${TOPIC_CLIENT_SEARCH}; do
  create_topic "kafka2-b-1" ${BOOTSTRAP_SERVER2} ${t} 1 "delete"
done

create_topic "kafka2-b-1" ${BOOTSTRAP_SERVER2} ${TOPIC_PRODUCTS_BLOCKED} 1 "delete"
create_topic "kafka2-b-1" ${BOOTSTRAP_SERVER2} ${TOPIC_RECOMMENDATIONS} 1 "compact"