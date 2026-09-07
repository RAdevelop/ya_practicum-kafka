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
IP.1 = 127.0.0.1
IP.2 = ${CRT_ALT_NAMES_IP_2}
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

  openssl pkcs8 -topk8 -inform PEM -in "${DIR_CREDS}/keystore.key" -out "${DIR_CREDS}/keystore.pk8" -nocrypt

  # Сохраним пароли
  echo ${CA_PASS} > "${DIR_CREDS}/sslkey_creds"
  echo ${CA_PASS} > "${DIR_CREDS}/keystore_creds"
  echo ${CA_PASS} > "${DIR_CREDS}/truststore_creds"

  cat > "${TMP_DIR}/${NAME}/creds/${NAME}-client.properties" << EOF
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/${NAME}/truststore.jks
ssl.truststore.password=${CA_PASS}
ssl.truststore.type=JKS
ssl.keystore.location=/etc/kafka/secrets/${NAME}/keystore.pkcs12
ssl.keystore.password=${CA_PASS}
ssl.keystore.type=PKCS12
ssl.key.password=${CA_PASS}
ssl.endpoint.identification.algorithm=https
EOF
}



# users:
for u in ${USER_ADMIN} ${USER_KAFKA_UI} ${USER_SCHEMA_REGISTRY} ${USER_SHOP_API} ${USER_CLIENT_API} ${USER_MIRROR_MAKER} ${USER_SPARK}; do
  create_cert ${u}
done

for i in 1 2 3; do

  ##### контроллеры
  create_cert "kafka-c-${i}"
  create_cert "kafka2-c-${i}"

  mkdir -p "${TMP_DIR}/kafka-c-${i}/creds/${USER_ADMIN}"
  cp -r "${TMP_DIR}/${USER_ADMIN}/creds/" "${TMP_DIR}/kafka-c-${i}/creds/${USER_ADMIN}/"

  mkdir -p "${TMP_DIR}/kafka2-c-${i}/creds/${USER_ADMIN}"
  cp -r "${TMP_DIR}/${USER_ADMIN}/creds/" "${TMP_DIR}/kafka2-c-${i}/creds/${USER_ADMIN}/"

  ##### брокеры
  create_cert "kafka-b-${i}"
  create_cert "kafka2-b-${i}"

  mkdir -p "${TMP_DIR}/kafka-b-${i}/creds/${USER_ADMIN}"
  cp -r "${TMP_DIR}/${USER_ADMIN}/creds/" "${TMP_DIR}/kafka-b-${i}/creds/${USER_ADMIN}/"

  mkdir -p "${TMP_DIR}/kafka2-b-${i}/creds/${USER_ADMIN}"
  cp -r "${TMP_DIR}/${USER_ADMIN}/creds/" "${TMP_DIR}/kafka2-b-${i}/creds/${USER_ADMIN}/"

done

#for go-app
for u in ${USER_SCHEMA_REGISTRY} ${USER_SHOP_API} ${USER_CLIENT_API}; do
  GO_APP_DIR="./go-app/creds/${u}"
  mkdir -p ${GO_APP_DIR}
  rm -rf ${GO_APP_DIR}/*
  cp -r "${TMP_DIR}/${u}/creds/" ${GO_APP_DIR}
done

MIRROR_MAKER_DIR="${TMP_DIR}/${USER_MIRROR_MAKER}/config"
mkdir -p ${MIRROR_MAKER_DIR}
cat > "${MIRROR_MAKER_DIR}/mirror-maker.properties" << EOF
# ─── Исключения ───
topics.exclude = __.*|.*[\-\.]internal|.*[\-\.]._replica|_schemas


# ─── Кластеры ───
clusters = kafka, kafka2

kafka.bootstrap.servers = kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093
kafka2.bootstrap.servers = kafka2-b-1:9093,kafka2-b-2:9093,kafka2-b-3:9093

# ─── SSL для исходного кластера (kafka) ───
kafka.security.protocol = SSL
kafka.ssl.truststore.type = JKS
kafka.ssl.truststore.location = /etc/kafka/secrets/source/truststore.jks
kafka.ssl.truststore.password = ${CA_PASS}
kafka.ssl.keystore.type = PKCS12
kafka.ssl.keystore.location = /etc/kafka/secrets/source/keystore.pkcs12
kafka.ssl.keystore.password = ${CA_PASS}
kafka.ssl.key.password = ${CA_PASS}
kafka.ssl.endpoint.identification.algorithm = https

# ─── SSL для целевого кластера (kafka2) ───
kafka2.security.protocol = SSL
kafka2.ssl.truststore.type = JKS
kafka2.ssl.truststore.location = /etc/kafka/secrets/target/truststore.jks
kafka2.ssl.truststore.password = ${CA_PASS}
kafka2.ssl.keystore.type = PKCS12
kafka2.ssl.keystore.location = /etc/kafka/secrets/target/keystore.pkcs12
kafka2.ssl.keystore.password = ${CA_PASS}
kafka2.ssl.key.password = ${CA_PASS}
kafka2.ssl.endpoint.identification.algorithm = https

# ─── Репликация kafka → kafka2 ───
kafka->kafka2.enabled = true
kafka->kafka2.topics = .*
kafka->kafka2.groups = .*
kafka->kafka2.sync.group.offsets.enabled = true
kafka->kafka2.emit.checkpoints.enabled = true
kafka->kafka2.emit.heartbeats.enabled = true

# ─── Репликация kafka2 → kafka (если нужна двусторонняя) ───
# kafka2->kafka.enabled = true
# kafka2->kafka.topics = .*
# kafka2->kafka.groups = .*
# kafka2->kafka.sync.group.offsets.enabled = true
# kafka2->kafka.emit.checkpoints.enabled = true
# kafka2->kafka.emit.heartbeats.enabled = true

# ─── Фактор репликации внутренних топиков ───
replication.factor=3
checkpoints.topic.replication.factor = 3
heartbeats.topic.replication.factor = 3
offset-syncs.topic.replication.factor = 3
offset.storage.replication.factor = 3
config.storage.replication.factor = 3
status.storage.replication.factor = 3

# ─── Синхронизация метаданных ───
sync.topic.acls.enabled = true
sync.topic.configs.enabled = true
refresh.topics.enabled=true
refresh.groups.enabled = true
refresh.topics.interval.seconds = 60
refresh.groups.interval.seconds = 60

# ─── Политика именования реплицированных топиков ───
# По умолчанию топики получат префикс: kafka.my-topic → kafka2
# Если нужны одинаковые имена — раскомментируйте:
replication.policy.class = org.apache.kafka.connect.mirror.IdentityReplicationPolicy

EOF

############
rm -rf ${MOUNT_DIR}/*
cp -r ${TMP_DIR}/ ${MOUNT_DIR}/
rm -rf ${TMP_DIR}/