#!/bin/bash

YELLOW='\033[0;33m'
NC='\033[0m' # No Color

#COMMAND_CONFIG="/etc/kafka/secrets/admin/admin-client.properties"
#BOOTSTRAP_SERVER="kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093"

CACERT="./mount_dir/schema-registry/creds/truststore.pem"
CERT="./mount_dir/schema-registry/creds/keystore.pem"
KEY="./mount_dir/schema-registry/creds/keystore.key"

SCHEMAS_DIR="./schemas"

echo "${YELLOW}Регистрируем схемы 'metric-value'${NC}"
curl -X POST https://localhost:8081/subjects/metric-value/versions \
--cacert ${CACERT} \
--cert ${CERT} \
--key ${KEY} \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
-d "{\"schema\": $(cat ${SCHEMAS_DIR}/metric.avsc | jq -c @json)}"

echo "\n"
echo "${YELLOW}Проверка регистрации схемы 'metric-value'${NC}"
curl -X GET https://localhost:8081/subjects \
--cacert ${CACERT} \
--cert ${CERT} \
--key ${KEY} \
-H "Content-Type: application/vnd.schemaregistry.v1+json"

echo "\n"
echo "${YELLOW}Получить все версии схемы 'metric-value'${NC}"
curl -X GET https://localhost:8081/subjects/metric-value/versions \
--cacert ${CACERT} \
--cert ${CERT} \
--key ${KEY} \
-H "Content-Type: application/vnd.schemaregistry.v1+json"

echo "\n"