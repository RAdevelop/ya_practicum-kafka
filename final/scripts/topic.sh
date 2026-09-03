#!/bin/bash
YELLOW='\033[0;33m'
NC='\033[0m' # No Color


create_topic() {
  local TOPIC_NAME=$1
  echo "${YELLOW}Create topic: ${TOPIC_NAME}${NC}"
  #cleanup.policy=compact
  # - delete   - Удаляет старые данные по времени или размеру
  # - compact  - Сохраняет только последнее значение для каждого ключа
  # - compact,delete - Сначала compact, потом удаляет (delete) по времени
  # retention.ms=604800000 - Время хранения данных в топике в миллисекундах (7 дней)
  # segment.bytes=536870912 - Максимальный размер файла сегмента лога в байтах (512 МБ)
  # min.insync.replicas=3 - минимальное число реплик (в синхронном состоянии), которые должны подтвердить получение сообщения для выполнения успешной записи
  docker exec -it kafka-b-1 kafka-topics \
  --command-config ${COMMAND_CONFIG} \
  --bootstrap-server ${BOOTSTRAP_SERVER} \
  --create \
  --topic ${TOPIC_NAME} \
  --partitions 3 \
  --replication-factor 3 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config segment.bytes=536870912 \
  --config min.insync.replicas=3

  echo "\n"
  echo "${YELLOW}Describe topic: ${TOPIC_NAME}${NC}"
  docker exec -it kafka-b-1 kafka-topics \
  --command-config ${COMMAND_CONFIG} \
  --bootstrap-server ${BOOTSTRAP_SERVER} \
  --describe \
  --topic ${TOPIC_NAME}
}


for t in ${TOPIC_SHOP_PRODUCTS}; do
  create_topic ${t}
done