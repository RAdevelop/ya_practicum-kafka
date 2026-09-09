#!/usr/bin/env bash
set -euo pipefail

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
# Короткие таймауты для скриптов ожидания и создания топиков
request.timeout.ms=10000
default.api.timeout.ms=15000

EOF
}



# users:
for u in ${USER_ADMIN} ${USER_KAFKA_UI} ${USER_SCHEMA_REGISTRY} ${USER_SHOP_API} ${USER_CLIENT_API} ${USER_MIRROR_MAKER} ${USER_KAFKA_CONNECT}; do
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

# для записи данных в файл
mkdir -p ${TMP_DIR}/${USER_KAFKA_CONNECT}/output
chmod -R 777 ${TMP_DIR}/${USER_KAFKA_CONNECT}/output

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
topics.exclude = __.*|.*[\-\.]internal|.*[\-\.]._replica|_schemas|heartbeats|checkpoints|offset-syncs

# ─── Кластеры ───
clusters = kafka, kafka2

kafka.bootstrap.servers = ${BOOTSTRAP_SERVER}
kafka2.bootstrap.servers = ${BOOTSTRAP_SERVER2}

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

# ─── Репликация kafka → kafka2 (всё, кроме recommendations) ───
kafka->kafka2.enabled = true
kafka->kafka2.topics = ^(?!recommendations$).*
kafka->kafka2.groups = .*
kafka->kafka2.sync.group.offsets.enabled = true
kafka->kafka2.emit.checkpoints.enabled = true
kafka->kafka2.emit.heartbeats.enabled = true

# ─── Репликация kafka2 → kafka (только recommendations) ───
kafka2->kafka.enabled = true
kafka2->kafka.topics = recommendations
kafka2->kafka.groups = ^$
kafka2->kafka.sync.group.offsets.enabled = false
kafka2->kafka.emit.checkpoints.enabled = false
kafka2->kafka.emit.heartbeats.enabled = true

# ─── Фактор репликации внутренних топиков ───
replication.factor=1
checkpoints.topic.replication.factor = 1
heartbeats.topic.replication.factor = 1
offset-syncs.topic.replication.factor = 1
offset.storage.replication.factor = 1
config.storage.replication.factor = 1
status.storage.replication.factor = 1

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


################################### hadoop

HADOOP_DIR="${TMP_DIR}/hadoop"
mkdir -p "${HADOOP_DIR}/configs"
cat > "${HADOOP_DIR}/configs/core-site.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://hdfs-namenode:9000</value>
  </property>
</configuration>

EOF

cat > "${HADOOP_DIR}/configs/hdfs-site.xml" << EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/hadoop/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/hadoop/dfs/data</value>
  </property>
</configuration>

EOF

#cat > "${HADOOP_DIR}/configs/hadoop.env" << EOF
#CORE_CONF_fs_defaultFS=hdfs://hdfs-namenode:9000
#HDFS_CONF_dfs_webhdfs_enabled=true
#HDFS_CONF_dfs_permissions_enabled=false
#HDFS_CONF_dfs_replication=1
#
#EOF

###################################### spark analytics
SPARK_DIR="${TMP_DIR}/spark"
mkdir -p "${SPARK_DIR}/apps"
#Py не знаю, скрипт писала ИИ
cat > "${SPARK_DIR}/apps/analytics.py" << EOF
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, collect_list, regexp_replace

spark = SparkSession.builder \
    .appName("product-recommendations") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw = spark.read.text("hdfs://hdfs-namenode:9000/topics/products_published/*/*")

# Шаг 1: убираем внешние кавычки
# Шаг 2: разэкранируем \" -> "
# Шаг 3: парсим чистый JSON (spark.read.json сам выведет схему)
clean = raw.withColumn(
    "no_quotes", regexp_replace(col("value"), r'^"|"$', '')
).withColumn(
    "json_str", regexp_replace(col("no_quotes"), r'\\"', '"')
)

products = spark.read.json(clean.select("json_str").rdd.map(lambda r: r[0]))

# Оставляем только нужные поля
products = products.select("product_id", "name", "category", "brand")

# Фильтруем записи, где нет product_id или category (битые/пустые не пройдут)
products_valid = products.filter(col("product_id").isNotNull() & col("category").isNotNull())

products_dedup = products_valid.dropDuplicates(["product_id"])

result = products_dedup.groupBy("category").agg(
    to_json(struct(
        col("category"),
        collect_list(struct("product_id", "name", "brand")).alias("recommended_products")
    )).alias("value")
).select(col("category").cast("string").alias("key"), col("value"))

# Пишем только если есть валидные данные
if result.count() > 0:
result.write.format("kafka") \
    .option("kafka.bootstrap.servers", "${BOOTSTRAP_SERVER2}") \
    .option("topic", "${TOPIC_RECOMMENDATIONS}") \
    .option("kafka.security.protocol", "SSL") \
    .option("kafka.ssl.truststore.location", "/etc/kafka/secrets/truststore.jks") \
    .option("kafka.ssl.truststore.password", "${CA_PASS}") \
    .option("kafka.ssl.keystore.location", "/etc/kafka/secrets/keystore.pkcs12") \
    .option("kafka.ssl.keystore.password", "${CA_PASS}") \
    .option("kafka.ssl.key.password", "${CA_PASS}") \
    .save()
else:
    print("No valid products found, skipping write to Kafka")

spark.stop()

EOF

############
rm -rf ${MOUNT_DIR}/*
cp -r ${TMP_DIR}/ ${MOUNT_DIR}/
rm -rf ${TMP_DIR}/