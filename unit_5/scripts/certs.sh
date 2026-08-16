#!/usr/bin/env bash
set -euo pipefail
clear

CA_PASS="kafka123"

CA_DIR="./ca"
CA_FILE="${CA_DIR}/ca.cnf"

# Очищаем и готовим директорию
rm -rf ${CA_DIR}
mkdir -p ${CA_DIR}

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
openssl req -new -nodes -x509 -days 365 -newkey rsa:2048 -keyout "${CA_DIR}/ca.key" -out "${CA_DIR}/ca.crt" -config ${CA_FILE}

# Создадим файл для хранения сертификата безопасности
cat "${CA_DIR}/ca.crt" "${CA_DIR}/ca.key" > "${CA_DIR}/ca.pem"

#################

for i in 1 2 3; do

  # Создадим файл конфигурации для брокера
  mkdir -p "${CA_DIR}/kafka-b-${i}-creds"

  cat > "${CA_DIR}/kafka-b-${i}-creds/kafka-b.cnf" << EOF
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
commonName = kafka-b-${i}

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
DNS.1 = kafka-b-${i}
DNS.2 = kafka-b-${i}-external
DNS.3 = localhost
EOF

  # Создадим приватный ключ и запрос на сертификат (CSR)

  openssl req -new \
      -newkey rsa:2048 \
      -keyout "${CA_DIR}/kafka-b-${i}-creds/kafka-b.key" \
      -out "${CA_DIR}/kafka-b-${i}-creds/kafka-b.csr" \
      -config "${CA_DIR}/kafka-b-${i}-creds/kafka-b.cnf" \
      -nodes

  # Создадим сертификат брокера, подписанный CA

  openssl x509 -req \
      -days 3650 \
      -in "${CA_DIR}/kafka-b-${i}-creds/kafka-b.csr" \
      -CA "${CA_DIR}/ca.crt" \
      -CAkey "${CA_DIR}/ca.key" \
      -CAcreateserial \
      -out "${CA_DIR}/kafka-b-${i}-creds/kafka-b.crt" \
      -extfile "${CA_DIR}/kafka-b-${i}-creds/kafka-b.cnf" \
      -extensions v3_req

  # Создадим PKCS12-хранилище
  openssl pkcs12 -export \
      -in "${CA_DIR}/kafka-b-${i}-creds/kafka-b.crt" \
      -inkey "${CA_DIR}/kafka-b-${i}-creds/kafka-b.key" \
      -chain \
      -CAfile "${CA_DIR}/ca.pem" \
      -name kafka-b-${i} \
      -out "${CA_DIR}/kafka-b-${i}-creds/kafka-b.p12" \
      -password pass:${CA_PASS}

  # Создадим keystore для Kafka
  keytool -importkeystore \
      -deststorepass ${CA_PASS} \
      -destkeystore "${CA_DIR}/kafka-b-${i}-creds/kafka.kafka-b.keystore.pkcs12" \
      -srckeystore "${CA_DIR}/kafka-b-${i}-creds/kafka-b.p12" \
      -deststoretype PKCS12  \
      -srcstoretype PKCS12 \
      -noprompt \
      -srcstorepass ${CA_PASS}

  # Создадим truststore для Kafka
  keytool -import \
      -file "${CA_DIR}/ca.crt" \
      -alias ca \
      -keystore "${CA_DIR}/kafka-b-${i}-creds/kafka.kafka-b.truststore.jks" \
      -storepass ${CA_PASS} \
      -noprompt

  # Сохраним пароли
  echo ${CA_PASS} > "${CA_DIR}/kafka-b-${i}-creds/kafka-b_sslkey_creds"
  echo ${CA_PASS} > "${CA_DIR}/kafka-b-${i}-creds/kafka-b_keystore_creds"
  echo ${CA_PASS} > "${CA_DIR}/kafka-b-${i}-creds/kafka-b_truststore_creds"

done