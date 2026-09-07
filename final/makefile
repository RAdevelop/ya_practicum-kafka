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
export USER_SPARK := spark
export USER_KAFKA_CONNECT := kafka-connect

export TOPIC_PRODUCTS := products
export TOPIC_PRODUCTS_BLOCKED := products_blocked
export TOPIC_PRODUCTS_PUBLISHED := products_published
export TOPIC_RECOMMENDATIONS := recommendations


export COMMAND_CONFIG := /etc/kafka/secrets/admin/admin-client.properties
export BOOTSTRAP_SERVER := kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093
export BOOTSTRAP_SERVER2 := kafka2-b-1:9093,kafka2-b-2:9093,kafka2-b-3:9093

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

# Цвета для вывода
GREEN = \033[0;32m
YELLOW = \033[0;33m
NC = \033[0m # No Color

#имя контейнера
name ?=

.PHONY: restart-name
restart-name: ## Перезапустить указанный контейнер name=имя_контейнера
	@$(DOCKER_COMPOSE) restart ${name}

.PHONY: rebuild-name
rebuild-name: ## Пересобрать указанный контейнер name=имя_контейнера
	@$(DOCKER_COMPOSE) down ${name}
	@$(DOCKER_COMPOSE) down -v ${name}
	@$(DOCKER_COMPOSE) up -d --no-deps --build ${name}

.PHONY: kafka-connect
kafka-connect: ## Добавить коннекторы в KAFKA-CONNECT
	@echo "$(GREEN)=== Add connectors ===$(NC)"
	@sh $(KAFKA_CONNECT_SCRIPT)
	@echo "$(GREEN)✅ Add connectors finished$(NC)"

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
	@$(DOCKER_COMPOSE) -f docker-compose-app.yml down app
	@$(DOCKER_COMPOSE) down
	@echo "$(GREEN)✅ Cluster stopped$(NC)"

.PHONY: clean
clean: ## Остановить кластер и удалить данные (volumes)
	@echo "$(YELLOW)=== Cleaning cluster data ===$(NC)"
	@$(DOCKER_COMPOSE) -f docker-compose-app.yml down -v app
	@$(DOCKER_COMPOSE) down -v
	@echo "$(GREEN)✅ Cluster data cleaned$(NC)"

.PHONY: up
up: ## Запустить кластер
	@echo "$(GREEN)=== Starting cluster ===$(NC)"
	@$(DOCKER_COMPOSE) up -d --build
	@echo "$(GREEN)✅ Cluster started$(NC)"
	@make status

.PHONY: status
status: ## Проверить статус контейнеров
	@echo "$(GREEN)=== Cluster status ===$(NC)"
	@$(DOCKER_COMPOSE) ps

.PHONY: wait60
wait60: ## Подождать 40 секунд для готовности Kafka
	@echo "$(YELLOW)=== Waiting 60 seconds for Kafka to be ready ===$(NC)"
	@sleep 60
	@echo "$(GREEN)✅ Wait complete$(NC)"

.PHONY: wait10
wait10: ## Подождать 10 секунд для готовности Kafka
	@echo "$(YELLOW)=== Waiting 10 seconds for Kafka to be ready ===$(NC)"
	@sleep 10
	@echo "$(GREEN)✅ Wait complete$(NC)"


.PHONY: up-app
up-app: ## Запустить go-app
	@echo "$(GREEN)=== Starting go-app ===$(NC)"
	@$(DOCKER_COMPOSE) -f docker-compose-app.yml up -d --no-deps --build app
	@echo "$(GREEN)✅ go-app started$(NC)"
	@make status

## Пересобрать все с очисткой
.PHONY: rebuild
rebuild:  ## Пересобрать кластер, создать топики, применить ACL
	@make down
	@make clean
	@make ssl
	@make up
	@make wait60
	@make topic
	@make wait10
	@make acl
	@make wait60
	@make schema-reg
	@make kafka-connect

# TODO #@make up-app

.DEFAULT_GOAL := help