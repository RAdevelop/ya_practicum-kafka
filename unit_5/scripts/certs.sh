#!/usr/bin/env bash
set -euo pipefail

# Установку ${CA_PASS} см в makefile

MOUNT_DIR="./mount_dir"
TMP_DIR="./tmp_dir"
CA_FILE="${TMP_DIR}/ca.cnf"

# Очищаем и готовим директорию
rm -rf ${TMP_DIR}
mkdir -p ${TMP_DIR} ${MOUNT_DIR}

cat > ${CA_FILE} << EOF
[ policy_match ]
countryName = match
stateOrProvinceName = match
organizationName = match
organizationalUnitName = optional
commonName = supplied
emailAddress = optional


[ req ]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
x509_extensions = v3_ca


[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = yandex-practice-kafka-ca


[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign
EOF


# Создадим корневой сертификат (Root CA)
openssl req -new -nodes -x509 -days 365 -newkey rsa:2048 -keyout "${TMP_DIR}/ca.key" -out "${TMP_DIR}/ca.crt" -config ${CA_FILE}

# Создадим файл для хранения сертификата безопасности
cat "${TMP_DIR}/ca.crt" "${TMP_DIR}/ca.key" > "${TMP_DIR}/ca.pem"

#################


## Создаем сертификаты
create_cert() {
  local NAME=$1
  local DIR_CREDS="${TMP_DIR}/${NAME}/creds"
  mkdir -p ${DIR_CREDS}

# Создадим файл конфигурации
  cat > "${DIR_CREDS}/kafka.cnf" << EOF
[req]
prompt = no
distinguished_name = dn
default_md = sha256
default_bits = 4096
req_extensions = v3_req

[ dn ]
countryName = RU
organizationName = Yandex
organizationalUnitName = Practice
localityName = Moscow
commonName = ${NAME}

[ v3_ca ]
subjectKeyIdentifier = hash
basicConstraints = critical,CA:true
authorityKeyIdentifier = keyid:always,issuer:always
keyUsage = critical,keyCertSign,cRLSign

[ v3_req ]
subjectKeyIdentifier = hash
basicConstraints = CA:FALSE
nsComment = "OpenSSL Generated Certificate"
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = ${NAME}
DNS.2 = ${NAME}-external
DNS.3 = localhost
EOF

  # Создадим приватный ключ и запрос на сертификат (CSR)

  openssl req -new \
      -newkey rsa:2048 \
      -keyout "${DIR_CREDS}/kafka.key" \
      -out "${DIR_CREDS}/kafka.csr" \
      -config "${DIR_CREDS}/kafka.cnf" \
      -nodes

  # Создадим сертификат, подписанный CA

  openssl x509 -req \
      -days 3650 \
      -in "${DIR_CREDS}/kafka.csr" \
      -CA "${TMP_DIR}/ca.crt" \
      -CAkey "${TMP_DIR}/ca.key" \
      -CAcreateserial \
      -out "${DIR_CREDS}/kafka.crt" \
      -extfile "${DIR_CREDS}/kafka.cnf" \
      -extensions v3_req

  # Создадим PKCS12-хранилище
  openssl pkcs12 -export \
      -in "${DIR_CREDS}/kafka.crt" \
      -inkey "${DIR_CREDS}/kafka.key" \
      -chain \
      -CAfile "${TMP_DIR}/ca.pem" \
      -name ${NAME} \
      -out "${DIR_CREDS}/kafka.p12" \
      -password pass:${CA_PASS}

  # Создадим keystore для Kafka
  keytool -importkeystore \
      -deststorepass ${CA_PASS} \
      -destkeystore "${DIR_CREDS}/keystore.pkcs12" \
      -srckeystore "${DIR_CREDS}/kafka.p12" \
      -deststoretype PKCS12  \
      -srcstoretype PKCS12 \
      -noprompt \
      -srcstorepass ${CA_PASS}

  # Создадим truststore для Kafka
  keytool -import \
      -file "${TMP_DIR}/ca.crt" \
      -alias ca \
      -keystore "${DIR_CREDS}/truststore.jks" \
      -storepass ${CA_PASS} \
      -noprompt

  keytool -exportcert -alias ca -keystore "${DIR_CREDS}/truststore.jks" -storepass ${CA_PASS} -rfc -file "${DIR_CREDS}/truststore.pem"

  openssl pkcs12 -in "${DIR_CREDS}/keystore.pkcs12" -out "${DIR_CREDS}/keystore.pem" -nokeys -passin pass:${CA_PASS} -passout pass:${CA_PASS}

  openssl pkcs12 -in "${DIR_CREDS}/keystore.pkcs12" -out "${DIR_CREDS}/keystore.key" -nocerts -nodes -passin pass:${CA_PASS}

  # Сохраним пароли
  echo ${CA_PASS} > "${DIR_CREDS}/sslkey_creds"
  echo ${CA_PASS} > "${DIR_CREDS}/keystore_creds"
  echo ${CA_PASS} > "${DIR_CREDS}/truststore_creds"

}

# admin
create_cert "admin"
cat > "${TMP_DIR}/admin/creds/admin-client.properties" << EOF
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/admin/truststore.jks
ssl.truststore.password=${CA_PASS}
ssl.truststore.type=JKS
ssl.keystore.location=/etc/kafka/secrets/admin/keystore.pkcs12
ssl.keystore.password=${CA_PASS}
ssl.keystore.type=PKCS12
ssl.key.password=${CA_PASS}
ssl.endpoint.identification.algorithm=https
EOF

# producer
create_cert "producer"
cat > "${TMP_DIR}/producer/creds/producer-client.properties" << EOF
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/producer/truststore.jks
ssl.truststore.password=${CA_PASS}
ssl.truststore.type=JKS
ssl.keystore.location=/etc/kafka/secrets/producer/keystore.pkcs12
ssl.keystore.password=${CA_PASS}
ssl.keystore.type=PKCS12
ssl.key.password=${CA_PASS}
ssl.endpoint.identification.algorithm=https
EOF

GO_APP_PRODUCER="./go-app/creds/producer"
mkdir -p ${GO_APP_PRODUCER}
rm -rf ${GO_APP_PRODUCER}/*
cp -r "${TMP_DIR}/producer/creds/" ${GO_APP_PRODUCER}

# consumer
create_cert "consumer"
cat > "${TMP_DIR}/consumer/creds/consumer-client.properties" << EOF
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/consumer/truststore.jks
ssl.truststore.password=${CA_PASS}
ssl.truststore.type=JKS
ssl.keystore.location=/etc/kafka/secrets/consumer/keystore.pkcs12
ssl.keystore.password=${CA_PASS}
ssl.keystore.type=PKCS12
ssl.key.password=${CA_PASS}
ssl.endpoint.identification.algorithm=https
EOF

GO_APP_CONSUMER="./go-app/creds/consumer"
mkdir -p ${GO_APP_CONSUMER}
rm -rf ${GO_APP_CONSUMER}/*
cp -r "${TMP_DIR}/consumer/creds/" ${GO_APP_CONSUMER}

for i in 1 2 3; do

  ##### контроллеры
  create_cert "kafka-c-${i}"

  mkdir -p "${TMP_DIR}/kafka-c-${i}/creds/admin"
  cp -r "${TMP_DIR}/admin/creds/" "${TMP_DIR}/kafka-c-${i}/creds/admin/"

  mkdir -p "${TMP_DIR}/kafka-c-${i}/creds/producer"
  cp -r "${TMP_DIR}/producer/creds/" "${TMP_DIR}/kafka-c-${i}/creds/producer/"

  mkdir -p "${TMP_DIR}/kafka-c-${i}/creds/consumer"
  cp -r "${TMP_DIR}/consumer/creds/" "${TMP_DIR}/kafka-c-${i}/creds/consumer/"

  ##### брокеры
  create_cert "kafka-b-${i}"
  mkdir -p "${TMP_DIR}/kafka-b-${i}/creds/admin"
  cp -r "${TMP_DIR}/admin/creds/" "${TMP_DIR}/kafka-b-${i}/creds/admin/"

  mkdir -p "${TMP_DIR}/kafka-b-${i}/creds/producer"
  cp -r "${TMP_DIR}/producer/creds/" "${TMP_DIR}/kafka-b-${i}/creds/producer/"

  mkdir -p "${TMP_DIR}/kafka-b-${i}/creds/consumer"
  cp -r "${TMP_DIR}/consumer/creds/" "${TMP_DIR}/kafka-b-${i}/creds/consumer/"
done

# kafka-ui
create_cert "kafka-ui"

rm -rf ${MOUNT_DIR}/*
cp -r ${TMP_DIR}/ ${MOUNT_DIR}/
rm -rf ${TMP_DIR}/