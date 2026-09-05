#!/bin/bash

YELLOW='\033[0;33m'
NC='\033[0m' # No Color

CACERT="./mount_dir/schema-registry/creds/truststore.pem"
CERT="./mount_dir/schema-registry/creds/keystore.pem"
KEY="./mount_dir/schema-registry/creds/keystore.key"

SCHEMAS_DIR="./schemas"

#echo "${YELLOW}Регистрируем схемы 'products-value'${NC}"
#curl -X POST https://localhost:8081/subjects/products-value/versions \
#--cacert ${CACERT} \
#--cert ${CERT} \
#--key ${KEY} \
#-H "Content-Type: application/vnd.schemaregistry.v1+json" \
#-d "{\"schema\": $(cat ${SCHEMAS_DIR}/products.avsc | jq -c @json),\"schemaType\": \"AVRO\"}"

#curl -X DELETE https://localhost:8081/subjects/products-value \
#--cacert ${CACERT} \
#--cert ${CERT} \
#--key ${KEY} \
#-H "Content-Type: application/vnd.schemaregistry.v1+json"


SCHEMA=$(cat ${SCHEMAS_DIR}/products.json | jq -c | jq -R)

echo "${YELLOW}Регистрируем схемы 'products-value'${NC}"
curl -X POST https://localhost:8081/subjects/products-value/versions \
--cacert ${CACERT} \
--cert ${CERT} \
--key ${KEY} \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
-d "{\"schema\": ${SCHEMA}, \"schemaType\": \"JSON\"}"



echo "\n"
echo "${YELLOW}Проверка регистрации схемы 'products-value'${NC}"
curl -X GET https://localhost:8081/subjects \
--cacert ${CACERT} \
--cert ${CERT} \
--key ${KEY} \
-H "Content-Type: application/vnd.schemaregistry.v1+json"

echo "\n"
echo "${YELLOW}Получить все версии схемы 'products-value'${NC}"
curl -X GET https://localhost:8081/subjects/products-value/versions \
--cacert ${CACERT} \
--cert ${CERT} \
--key ${KEY} \
-H "Content-Type: application/vnd.schemaregistry.v1+json"

echo "\n"