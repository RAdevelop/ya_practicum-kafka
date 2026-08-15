#!/bin/bash

set -e

SSL_DIR="./ssl"
rm -Rf ${SSL_DIR}

CA_DIR="${SSL_DIR}/ca"
CONTROLLERS_DIR="${SSL_DIR}/controllers"
BROKERS_DIR="${SSL_DIR}/brokers"
CLIENTS_DIR="${SSL_DIR}/clients"

mkdir -p ${CA_DIR} ${CONTROLLERS_DIR} ${BROKERS_DIR} ${CLIENTS_DIR}

PASSWORD="kafka123"

# CA
openssl genrsa -out ${CA_DIR}/ca.key 4096
openssl req -new -x509 -days 3650 -sha256 \
    -key ${CA_DIR}/ca.key \
    -subj "/C=RU/ST=Moscow/L=Moscow/O=Company/CN=KafkaCA" \
    -out ${CA_DIR}/ca.crt

# Функция для создания сертификата
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

    # Создаем PKCS12
    openssl pkcs12 -export \
        -in ${DIR}/server.crt \
        -inkey ${DIR}/server.key \
        -out ${SSL_DIR}/keystore.p12 \
        -name ${NAME} \
        -password pass:${PASSWORD}

    # Конвертируем PKCS12 в JKS (Java Key Store)
    keytool -importkeystore \
        -srckeystore ${SSL_DIR}/keystore.p12 \
        -srcstoretype PKCS12 \
        -srcstorepass ${PASSWORD} \
        -destkeystore ${SSL_DIR}/keystore.jks \
        -deststoretype JKS \
        -deststorepass ${PASSWORD}

    # Создаем truststore в JKS
    keytool -keystore ${SSL_DIR}/truststore.jks \
        -alias CARoot \
        -import -file ${CA_DIR}/ca.crt \
        -storepass ${PASSWORD} \
        -storetype JKS \
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

echo "=== SSL certificates generated successfully ==="
echo "Password: ${PASSWORD}"