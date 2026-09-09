FROM confluentinc/cp-kafka-connect:8.3.0

USER root

# Копируем уже распакованный confluent-hub-client с хоста
COPY ./docker/kafka-connect/bin/ /tmp/bin/
COPY ./docker/kafka-connect/share/ /tmp/share/

# Устанавливаем коннектор с retry (до 5 попыток)
RUN echo "plugin.path=/usr/share/java,/usr/share/confluent-hub-components" > /tmp/worker.props \
    && for i in 1 2 3 4 5; do \
        /tmp/bin/confluent-hub install --no-prompt \
    confluentinc/kafka-connect-hdfs3:latest \
    --component-dir /usr/share/confluent-hub-components \
        --worker-configs /tmp/worker.props \
        && break || echo "Attempt $i failed, retrying..." && sleep 10; \
    done

# Права
RUN chown -R appuser:appuser /usr/share/confluent-hub-components

# Очистка
RUN rm -rf /tmp/bin /tmp/share /tmp/worker.props

USER appuser
