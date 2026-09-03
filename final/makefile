# Makefile для управления Kafka кластером

#ENV
export CA_PASS := kafka123
# users
export USER_ADMIN := admin
export USER_KAFKA_UI := kafka-ui
export USER_SCHEMA_REGISTRY := schema-registry
export USER_SHOP_API := shop-api
export USER_CLIENT_API := client-api

export TOPIC_NAME := metric
export COMMAND_CONFIG := /etc/kafka/secrets/admin/admin-client.properties
export BOOTSTRAP_SERVER := kafka-b-1:9093,kafka-b-2:9093,kafka-b-3:9093

# Переменные
DOCKER_COMPOSE = docker-compose
SSL_SCRIPT = ./scripts/certs.sh
TOPIC_SCRIPT = ./scripts/topic.sh
ACL_SCRIPT = ./scripts/acl.sh
SCHEMA_REG_SCRIPT = ./scripts/schema-registry.sh

# Цвета для вывода
GREEN = \033[0;32m
YELLOW = \033[0;33m
NC = \033[0m # No Color

.PHONY: help
help: ## Показать справку
	@echo "$(GREEN)Доступные команды:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(YELLOW)  make %-20s$(NC) %s\n", $$1, $$2}'

.PHONY: ssl
ssl: ## Сгенерировать SSL сертификаты
	@echo "$(GREEN)=== Generating SSL certificates ===$(NC)"
	@sh $(SSL_SCRIPT)
	@echo "$(GREEN)✅ SSL certificates generated successfully$(NC)"


.PHONY: unset-env
unset-env: ## Удалить ENV переменные для скриптов
	@echo "$(GREEN)=== Cleaning environment ===$(NC)"
	@for var in CA_PASS TOPIC_NAME COMMAND_CONFIG BOOTSTRAP_SERVER; do \
		unset $$var; \
	done
	@echo "$(GREEN)✅ Environment cleaned$(NC)"

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
	@$(DOCKER_COMPOSE) up -d
	@echo "$(GREEN)✅ Cluster started$(NC)"
	@make status

.PHONY: status
status: ## Проверить статус контейнеров
	@echo "$(GREEN)=== Cluster status ===$(NC)"
	@$(DOCKER_COMPOSE) ps

.PHONY: wait40
wait40:
	@echo "$(YELLOW)=== Waiting 40 seconds for Kafka to be ready ===$(NC)"
	@sleep 40
	@echo "$(GREEN)✅ Wait complete$(NC)"

.PHONY: wait10
wait10:
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
	@make wait40
	@make topic
	@make wait10
	@make acl
	@make wait40
	@make schema-reg
	@make unset-env
	#@make up-app


.DEFAULT_GOAL := help