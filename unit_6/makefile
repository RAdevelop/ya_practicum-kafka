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

#.PHONY: nifi-cp-proc
#nifi-cp-proc: ## скопировать ранее настроенные процессоры в NiFi
#	@echo "$(GREEN)=== Starting nifi-cp-proc ===$(NC)"
#	@$(DOCKER_COMPOSE) stop nifi
#	@$(DOCKER_COMPOSE) rm -f nifi
#	@docker volume rm -f nifi-conf nifi-state nifi-flowfile_repository nifi-provenance_repository nifi-content_repository nifi-database_repository nifi-nar_extensions nifi-logs nifi-python_extensions 2>/dev/null || true
#	@$(DOCKER_COMPOSE) up -d nifi
#	@sleep 5
#	@docker cp ./nifi/flow.json.gz nifi:/opt/nifi/nifi-current/conf/flow.json.gz
#	@docker exec --user root nifi chown nifi:nifi /opt/nifi/nifi-current/conf/flow.json.gz
#	@$(DOCKER_COMPOSE) restart nifi
#	@echo "$(GREEN)✅ nifi-cp-proc complete$(NC)"

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
	@make up-app


#.PHONY: rebuild-with-nifi-proc
#rebuild-with-nifi-proc:  ## Пересобрать кластер, создать топики, применить ACL + применить настроенные процессоры NiFi
#	@make down
#	@make clean
#	@make ssl
#	@make up
#	@make wait40
#	@make topic
#	@make wait10
#	@make acl
#	@make wait40
#	@make schema-reg
#	@make unset-env
#	@make nifi-cp-proc
#	@make up-app

.DEFAULT_GOAL := help