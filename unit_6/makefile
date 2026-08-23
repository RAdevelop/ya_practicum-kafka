# Makefile для управления Kafka кластером

#ENV
export CA_PASS := kafka123
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
	@$(DOCKER_COMPOSE) down
	@echo "$(GREEN)✅ Cluster stopped$(NC)"

.PHONY: clean
clean: ## Остановить кластер и удалить данные (volumes)
	@echo "$(YELLOW)=== Cleaning cluster data ===$(NC)"
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

.PHONY: wait
wait:
	@echo "$(YELLOW)=== Waiting 40 seconds for Kafka to be ready ===$(NC)"
	@sleep 40
	@echo "$(GREEN)✅ Wait complete$(NC)"

.PHONY: wait10
wait10:
	@echo "$(YELLOW)=== Waiting 10 seconds for Kafka to be ready ===$(NC)"
	@sleep 10
	@echo "$(GREEN)✅ Wait complete$(NC)"

.PHONY: rebuild
rebuild: ## Пересобрать кластер
	@$(MAKE) down
	@$(MAKE) clean
	@$(MAKE) ssl
	@$(MAKE) up
	@$(MAKE) unset-env

## Пересобрать все с очисткой
.PHONY: rebuild-all
rebuild-all:  ## Пересобрать кластер, создать топики, применить ACL
	@$(MAKE) down
	@$(MAKE) clean
	@$(MAKE) ssl
	@$(MAKE) up
	@$(MAKE) wait
	@$(MAKE) topic
	@$(MAKE) wait10
	@$(MAKE) acl
	@$(MAKE) wait
	@$(MAKE) schema-reg
	@$(MAKE) unset-env
	@$(DOCKER_COMPOSE) up -d --no-deps --build app

.DEFAULT_GOAL := help