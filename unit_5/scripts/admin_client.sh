#!/bin/bash


# Создаем директорию для конфигов, если её нет
mkdir -p ./ssl/clients/creds

# Копируем сертификаты для клиентов
cp ./ssl/clients/ssl/keystore.p12 ./ssl/clients/creds/
cp ./ssl/clients/ssl/truststore.p12 ./ssl/clients/creds/

# Создаем конфигурационные файлы
cat > ./ssl/clients/creds/admin-client.conf << EOF
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/creds/truststore.p12
ssl.truststore.password=kafka123
ssl.truststore.type=PKCS12
ssl.keystore.location=/etc/kafka/secrets/creds/keystore.p12
ssl.keystore.password=kafka123
ssl.keystore.type=PKCS12
ssl.key.password=kafka123
ssl.endpoint.identification.algorithm=HTTPS
EOF

cat > ./ssl/clients/creds/producer-client.conf << EOF
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/creds/truststore.p12
ssl.truststore.password=kafka123
ssl.truststore.type=PKCS12
ssl.keystore.location=/etc/kafka/secrets/creds/keystore.p12
ssl.keystore.password=kafka123
ssl.keystore.type=PKCS12
ssl.key.password=kafka123
ssl.endpoint.identification.algorithm=HTTPS
EOF

cat > ./ssl/clients/creds/consumer-client.conf << EOF
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/creds/truststore.p12
ssl.truststore.password=kafka123
ssl.truststore.type=PKCS12
ssl.keystore.location=/etc/kafka/secrets/creds/keystore.p12
ssl.keystore.password=kafka123
ssl.keystore.type=PKCS12
ssl.key.password=kafka123
ssl.endpoint.identification.algorithm=HTTPS
EOF