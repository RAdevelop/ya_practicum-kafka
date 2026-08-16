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


## Создаем сертификаты
create_cert() {
  local NAME=$1
  mkdir -p "${CA_DIR}/${NAME}/creds"

# Создадим файл конфигурации
  cat > "${CA_DIR}/${NAME}/creds/kafka.cnf" << EOF
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
      -keyout "${CA_DIR}/${NAME}/creds/kafka.key" \
      -out "${CA_DIR}/${NAME}/creds/kafka.csr" \
      -config "${CA_DIR}/${NAME}/creds/kafka.cnf" \
      -nodes

  # Создадим сертификат, подписанный CA

  openssl x509 -req \
      -days 3650 \
      -in "${CA_DIR}/${NAME}/creds/kafka.csr" \
      -CA "${CA_DIR}/ca.crt" \
      -CAkey "${CA_DIR}/ca.key" \
      -CAcreateserial \
      -out "${CA_DIR}/${NAME}/creds/kafka.crt" \
      -extfile "${CA_DIR}/${NAME}/creds/kafka.cnf" \
      -extensions v3_req

  # Создадим PKCS12-хранилище
  openssl pkcs12 -export \
      -in "${CA_DIR}/${NAME}/creds/kafka.crt" \
      -inkey "${CA_DIR}/${NAME}/creds/kafka.key" \
      -chain \
      -CAfile "${CA_DIR}/ca.pem" \
      -name ${NAME} \
      -out "${CA_DIR}/${NAME}/creds/kafka.p12" \
      -password pass:${CA_PASS}

  # Создадим keystore для Kafka
  keytool -importkeystore \
      -deststorepass ${CA_PASS} \
      -destkeystore "${CA_DIR}/${NAME}/creds/keystore.pkcs12" \
      -srckeystore "${CA_DIR}/${NAME}/creds/kafka.p12" \
      -deststoretype PKCS12  \
      -srcstoretype PKCS12 \
      -noprompt \
      -srcstorepass ${CA_PASS}

  # Создадим truststore для Kafka
  keytool -import \
      -file "${CA_DIR}/ca.crt" \
      -alias ca \
      -keystore "${CA_DIR}/${NAME}/creds/truststore.jks" \
      -storepass ${CA_PASS} \
      -noprompt

  # Сохраним пароли
  echo ${CA_PASS} > "${CA_DIR}/${NAME}/creds/sslkey_creds"
  echo ${CA_PASS} > "${CA_DIR}/${NAME}/creds/keystore_creds"
  echo ${CA_PASS} > "${CA_DIR}/${NAME}/creds/truststore_creds"

}

# брокеры:
for i in 1; do
  create_cert "kafka-b-${i}"
done

# kafka-ui
create_cert "kafka-ui"