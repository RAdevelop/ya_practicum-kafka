#!/bin/bash

YELLOW='\033[0;33m'
NC='\033[0m' # No Color

CACERT="./mount_dir/schema-registry/creds/truststore.pem"
CERT="./mount_dir/schema-registry/creds/keystore.pem"
KEY="./mount_dir/schema-registry/creds/keystore.key"

SCHEMAS_DIR="./schemas"

SCHEMA=$(cat ${SCHEMAS_DIR}/products.json | jq -c | jq -R)



start_time=$SECONDS
max_seconds=700
i=0
ready=false
echo "${YELLOW}Waiting for Schema Registry (max ${max_seconds} seconds)...${NC}"
while (( SECONDS - start_time < max_seconds )); do
  ((i++))
  if curl -sk \
      --cacert "$CACERT" \
      --cert "$CERT" \
      --key "$KEY" \
      https://localhost:8081/subjects >/dev/null 2>&1; then
    echo "Schema Registry is ready!"
    ready=true
    break
  fi
  echo "  [$i] Not ready yet... (elapsed: $((SECONDS - start_time))s)"
  sleep 5
done

if [[ "$ready" != "true" ]]; then
  echo "ERROR: Schema Registry did not start within ${max_seconds} seconds!"
  exit 1
fi


echo "${YELLOW}Регистрируем схемы 'products-value'${NC}"
curl -s -X POST https://localhost:8081/subjects/products-value/versions \
--cacert ${CACERT} \
--cert ${CERT} \
--key ${KEY} \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
-d "{\"schema\": ${SCHEMA}, \"schemaType\": \"JSON\"}"

printf "\n"
echo "${YELLOW}Проверка регистрации схемы 'products-value'${NC}"
curl -s -X GET https://localhost:8081/subjects \
--cacert ${CACERT} \
--cert ${CERT} \
--key ${KEY} \
-H "Content-Type: application/vnd.schemaregistry.v1+json"

printf "\n"
echo "${YELLOW}Получить все версии схемы 'products-value'${NC}"
curl -s -X GET https://localhost:8081/subjects/products-value/versions \
--cacert ${CACERT} \
--cert ${CERT} \
--key ${KEY} \
-H "Content-Type: application/vnd.schemaregistry.v1+json"

printf "\n"
