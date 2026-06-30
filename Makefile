COMPOSE_DIR := 01-environment
COMPOSE := docker compose -f $(COMPOSE_DIR)/docker-compose.yml

.PHONY: up down restart logs ps

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart: down up

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps