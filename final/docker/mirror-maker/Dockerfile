# Используем официальный образ Kafka Connect
FROM confluentinc/cp-kafka-connect:8.3.0

# Переключаемся на root, чтобы установить конвертер
USER root

# Копируем JSON Schema конвертер из локальной папки
COPY ./docker/mirror-maker/*.jar /usr/share/java/

# Создаём директорию для конфигов
RUN mkdir -p /etc/mirror-maker

# Копируем конфиг MirrorMaker
COPY ./mount_dir/mirror-maker/config/mirror-maker.properties /etc/mirror-maker/mirror-maker.properties

# Переключаемся обратно на appuser
USER appuser

# Запускаем MirrorMaker встроенной командой (она использует переменные окружения CONNECT_*)
CMD ["/usr/bin/connect-mirror-maker", "/etc/mirror-maker/mirror-maker.properties"]