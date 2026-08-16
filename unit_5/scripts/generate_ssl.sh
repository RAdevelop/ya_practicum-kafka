#!/bin/bash

set -e

SSL_DIR="./ssl"
rm -Rf ${SSL_DIR}

CA_DIR="${SSL_DIR}/ca"
CONTROLLERS_DIR="${SSL_DIR}/controllers"
BROKERS_DIR="${SSL_DIR}/brokers"
CLIENTS_DIR="${SSL_DIR}/clients"
ADMIN_USER="${SSL_DIR}/users/admin"

mkdir -p ${CA_DIR} ${CONTROLLERS_DIR} ${BROKERS_DIR} ${CLIENTS_DIR}

PASSWORD="kafka123"

# CA
openssl genrsa -out ${CA_DIR}/ca.key 4096
openssl req -new -x509 -days 3650 -sha256 \
    -key ${CA_DIR}/ca.key \
    -subj "/C=RU/ST=Moscow/L=Moscow/O=Company/CN=KafkaCA" \
    -out ${CA_DIR}/ca.crt

# Функция для создания сертификата в PKCS12
create_cert() {
    local NAME=$1
    local DIR=$2
    local SSL_DIR="${DIR}/ssl"
    local CREDS_DIR="${DIR}/creds"

    mkdir -p ${SSL_DIR} ${CREDS_DIR}

    openssl genrsa -out ${DIR}/server.key 4096

    cat > ${DIR}/openssl.cnf << EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no
default_md = sha256

[req_distinguished_name]
C = RU
ST = Moscow
L = Moscow
O = Company
CN = ${NAME}

[v3_req]
keyUsage = digitalSignature, keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = ${NAME}
DNS.2 = localhost
DNS.3 = ${NAME}.local
IP.1 = 127.0.0.1
EOF

    openssl req -new -sha256 \
        -key ${DIR}/server.key \
        -out ${DIR}/server.csr \
        -config ${DIR}/openssl.cnf

    openssl x509 -req -days 3650 -sha256 \
        -in ${DIR}/server.csr \
        -CA ${CA_DIR}/ca.crt \
        -CAkey ${CA_DIR}/ca.key \
        -CAcreateserial \
        -out ${DIR}/server.crt \
        -extensions v3_req \
        -extfile ${DIR}/openssl.cnf

    # Создаем keystore в PKCS12 (без JKS конвертации!)
    openssl pkcs12 -export \
        -in ${DIR}/server.crt \
        -inkey ${DIR}/server.key \
        -out ${SSL_DIR}/keystore.p12 \
        -name ${NAME} \
        -password pass:${PASSWORD}

    # Создаем truststore в PKCS12
    keytool -keystore ${SSL_DIR}/truststore.p12 \
        -alias CARoot \
        -import -file ${CA_DIR}/ca.crt \
        -storepass ${PASSWORD} \
        -storetype PKCS12 \
        -noprompt

    # Создаем файлы с паролями
    printf '%s' "${PASSWORD}" > ${CREDS_DIR}/keystore_creds
    printf '%s' "${PASSWORD}" > ${CREDS_DIR}/ssl_key_creds
    printf '%s' "${PASSWORD}" > ${CREDS_DIR}/truststore_creds

    # Удаляем временные файлы
    rm ${DIR}/server.csr
    rm ${DIR}/server.crt
    rm ${DIR}/openssl.cnf
}

# Контроллеры
for i in 1 2 3; do
    create_cert "kafka-c-${i}" "${CONTROLLERS_DIR}/kafka-c-${i}"
done

# Брокеры
for i in 1 2 3; do
    create_cert "kafka-b-${i}" "${BROKERS_DIR}/kafka-b-${i}"
done

# Клиентский сертификат для Kafka UI
create_cert "kafka-ui-client" "${CLIENTS_DIR}"

# admin
create_cert "admin" "${ADMIN_USER}"
cat > ${ADMIN_USER}/creds/admin-client.properties << 'EOF'
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/users/admin/ssl/truststore.p12
ssl.truststore.password=kafka123
ssl.truststore.type=PKCS12
ssl.keystore.location=/etc/kafka/secrets/users/admin/ssl/keystore.p12
ssl.keystore.password=kafka123
ssl.keystore.type=PKCS12
ssl.key.password=kafka123
ssl.endpoint.identification.algorithm=HTTPS
EOF

for i in 1 2 3; do
    mkdir -p "${BROKERS_DIR}/kafka-b-${i}/users"
    cp -r "${ADMIN_USER}" "${BROKERS_DIR}/kafka-b-${i}/users"
done

# Создаем копии в корне для Kafka UI
cp ${CLIENTS_DIR}/ssl/keystore.p12 ${CLIENTS_DIR}/keystore.p12
cp ${CLIENTS_DIR}/ssl/truststore.p12 ${CLIENTS_DIR}/truststore.p12

echo "=== SSL certificates generated successfully (PKCS12) ==="
echo "Password: ${PASSWORD}"
echo "Files:"
echo "  - Controllers: ${CONTROLLERS_DIR}/kafka-c-*/ssl/keystore.p12, truststore.p12"
echo "  - Brokers: ${BROKERS_DIR}/kafka-b-*/ssl/keystore.p12, truststore.p12"
echo "  - Clients: ${CLIENTS_DIR}/ssl/keystore.p12, truststore.p12"