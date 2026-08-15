#!/bin/bash

cat > ssl/admin-client.conf << EOF
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/ssl/truststore.p12
ssl.truststore.password=kafka123
ssl.keystore.location=/etc/kafka/secrets/ssl/keystore.p12
ssl.keystore.password=kafka123
ssl.key.password=kafka123
ssl.endpoint.identification.algorithm=
EOF
