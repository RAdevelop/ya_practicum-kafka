# Makefile для управления Kafka кластером

.PHONY: help
help: ## Показать справку
	@echo "$(GREEN)Доступные команды:$(NC)"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(CURDIR)/Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(YELLOW)  make %-20s$(NC) %s\n", $$1, $$2}'

ENV_FILE ?= ./build/.env_make

export CRT_ALT_NAMES_IP_2 := 127.0.0.1

# users
export USER_ADMIN := admin
export USER_KAFKA_UI := kafka-ui
export USER_SCHEMA_REGISTRY := schema-registry
export USER_SHOP_API := shop-api
export USER_CLIENT_API := client-api
export USER_MIRROR_MAKER := mirror-maker
export USER_KAFKA_CONNECT := kafka-connect

export TOPIC_PRODUCTS := products
export TOPIC_PRODUCTS_BLOCKED := products-blocked
export TOPIC_PRODUCTS_PUBLISHED := products-published
export TOPIC_RECOMMENDATIONS := recommendations
export TOPIC_CLIENT_SEARCH := client-search


export COMMAND_CONFIG := /etc/kafka/secrets/admin/admin-client.properties
export BOOTSTRAP_SERVER := kafka-b-1:9093
export BOOTSTRAP_SERVER2 := kafka2-b-1:9093

# Загружаем .env глобально через include
ifneq ($(wildcard $(ENV_FILE)),)
    include $(ENV_FILE)
    export
endif

# Переменные
DOCKER_COMPOSE = docker-compose
SSL_SCRIPT = ./scripts/certs.sh
TOPIC_SCRIPT = ./scripts/topic.sh
ACL_SCRIPT = ./scripts/acl.sh
SCHEMA_REG_SCRIPT = ./scripts/schema-registry.sh
KAFKA_CONNECT_SCRIPT = ./scripts/kafka-connect.sh
WAIT_FOR_KAFKA_SCRIPT = ./scripts/wait_for_kafka.sh

# Цвета для вывода
GREEN = \033[0;32m
YELLOW = \033[0;33m
NC = \033[0m # No Color

#имя контейнера
name ?=

.PHONY: restart-name
restart-name: ## Перезапустить указанные контейнеры name="имя_контейнера1 имя_контейнера2"
	@$(DOCKER_COMPOSE) TODO up -d ${name}

.PHONY: rebuild-name
rebuild-name: ## Пересобрать указанный контейнер name=имя_контейнера
	@$(DOCKER_COMPOSE) TODO down ${name}
	@$(DOCKER_COMPOSE) down -v ${name}
	@$(DOCKER_COMPOSE) up -d --no-deps --build ${name}

.PHONY: kafka-connect
kafka-connect: ## Добавить коннекторы в KAFKA-CONNECT
	@echo "$(GREEN)=== Add connectors ===$(NC)"
	@sh $(KAFKA_CONNECT_SCRIPT)
	@echo "$(GREEN)✅ Add connectors finished$(NC)"

.PHONY: wait-kafka
wait-kafka: ## Ждем готовности кластера Kafka
	@echo "$(GREEN)=== Wait for kafka ===$(NC)"
	@sh $(WAIT_FOR_KAFKA_SCRIPT)
	@echo "$(GREEN)✅ Wait for kafka finished$(NC)"

.PHONY: ssl
ssl: ## Сгенерировать SSL сертификаты
	@echo "$(GREEN)=== Generating SSL certificates ===$(NC)"
	@sh $(SSL_SCRIPT)
	@echo "$(GREEN)✅ SSL certificates generated successfully$(NC)"

.PHONY: schema-reg
schema-reg: ## Регистрация схем в schema-registry
	@echo "$(GREEN)=== Uploading Schema to SchemaRegistry start===$(NC)"
	@sh $(SCHEMA_REG_SCRIPT)
	@echo "$(GREEN)✅ Uploading Schema to SchemaRegistry finished$(NC)"

.PHONY: topic
topic: ## Создать топики
	@echo "$(GREEN)=== Creating topics started ===$(NC)"
	@sh $(TOPIC_SCRIPT)
	@echo "$(GREEN)✅ Creating topics finished$(NC)"

.PHONY: acl
acl: ## Выдать права
	@echo "$(GREEN)=== ACL started ===$(NC)"
	@sh $(ACL_SCRIPT)
	@echo "$(GREEN)✅ ACL finished$(NC)"

.PHONY: down
down: ## Остановить кластер
	@echo "$(YELLOW)=== Stopping cluster ===$(NC)"
	@$(DOCKER_COMPOSE) -f docker-compose-go.yml down
	@$(DOCKER_COMPOSE) -f docker-compose.spark.yml down
	@$(DOCKER_COMPOSE) -f docker-compose.apps1.yml down
	@$(DOCKER_COMPOSE) -f docker-compose.kafka1.yml down
	@echo "$(GREEN)✅ Cluster stopped$(NC)"

.PHONY: clean
clean: ## Остановить кластер и удалить данные (volumes)
	@echo "$(YELLOW)=== Cleaning cluster data ===$(NC)"
	@$(DOCKER_COMPOSE) -f docker-compose-go.yml down -v
	@$(DOCKER_COMPOSE) -f docker-compose.spark.yml down -v
	@$(DOCKER_COMPOSE) -f docker-compose.apps1.yml down -v
	@$(DOCKER_COMPOSE) -f docker-compose.kafka1.yml down -v
	@echo "$(GREEN)✅ Cluster data cleaned$(NC)"

.PHONY: status
status: ## Проверить статус контейнеров
	@echo "$(GREEN)=== Cluster status ===$(NC)"
	@$(DOCKER_COMPOSE) ps

.PHONY: wait70
wait70: ## Подождать 70 секунд для готовности Kafka
	@echo "$(YELLOW)=== Waiting 70 seconds for Kafka to be ready ===$(NC)"
	@sleep 70
	@echo "$(GREEN)✅ Wait complete$(NC)"

.PHONY: wait20
wait20: ## Подождать 20 секунд для готовности Kafka
	@echo "$(YELLOW)=== Waiting 20 seconds for Kafka to be ready ===$(NC)"
	@sleep 20
	@echo "$(GREEN)✅ Wait complete$(NC)"


.PHONY: up-app
up-app: ## Запустить go-app
	@echo "$(GREEN)=== Starting go-app ===$(NC)"
	@$(DOCKER_COMPOSE) -f docker-compose-go.yml up -d --no-deps --build app
	@echo "$(GREEN)✅ go-app started$(NC)"
	@make status

## Пересобрать все с очисткой
.PHONY: rebuild
rebuild:  ## Пересобрать кластер, создать топики, применить ACL
	@make down
	@make clean
	@make ssl
	@echo "$(GREEN)========= Starting kafka cluster =========$(NC)"
	@$(DOCKER_COMPOSE) -f docker-compose.kafka1.yml  up -d --build
	@make status
	@make wait-kafka
	@make topic
	@make acl
	@echo "$(GREEN)========= Starting apps  =========$(NC)"
	@$(DOCKER_COMPOSE) -f docker-compose.apps1.yml  up -d --build
	@make status
	@make wait-kafka
	@make schema-reg
	@make kafka-connect
	@echo "$(GREEN)========= Starting spark  =========$(NC)"
	@$(DOCKER_COMPOSE) -f docker-compose.spark.yml  up -d --build
	@make status
	@echo "$(GREEN)✅ Cluster started$(NC)"
	@make wait-kafka
	@make up-app

.DEFAULT_GOAL := help