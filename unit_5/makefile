# Makefile для управления Kafka кластером

#ENV
export CA_PASS := kafka123

# Переменные
DOCKER_COMPOSE = docker-compose
SSL_SCRIPT = ./scripts/certs.sh
TOPIC_SCRIPT = ./scripts/topic.sh
ACL_SCRIPT = ./scripts/acl.sh

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
	@echo "$(YELLOW)=== Waiting 30 seconds for Kafka to be ready ===$(NC)"
	@sleep 30
	@echo "$(GREEN)✅ Wait complete$(NC)"

.PHONY: wait10
wait10:
	@sleep 10

.PHONY: rebuild
rebuild: down clean ssl up ## Пересобрать с очисткой

.PHONY: rebuild-all
rebuild-all: down clean ssl up wait topic wait10 acl ## Пересобрать все с очисткой

.DEFAULT_GOAL := help