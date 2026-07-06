# Этап 1: Сборка
FROM golang:1.26.3-bookworm AS builder

# Устанавливаем зависимости для CGO
RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    gcc \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

ENV CGO_ENABLED=1

WORKDIR /app

# Кешируем зависимости
COPY go.mod go.sum ./
RUN go mod download

# Копируем код
COPY . .

# Собираем бинарник
ARG CMD_DIR="cmd"
RUN go build -mod=mod -ldflags="-s -w" -v -o go-app ./${CMD_DIR}/main.go

# Этап 2: Финальный образ
FROM debian:bookworm-slim

# Устанавливаем librdkafka для запуска
RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/go-app .

CMD ["./go-app"]