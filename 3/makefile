.DEFAULT_GOAL := help

.PHONY: help install run test lint type-check init-db

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Установить зависимости
	uv sync --extra dev

run: ## Запустить скрапер
	uv run python main.py

test: ## Запустить тесты
	uv run pytest

test-v: ## Запустить тесты с подробным выводом
	uv run pytest -v

lint: ## Проверить код линтером (ruff)
	uv run ruff check .

type-check: ## Проверить типы (mypy)
	uv run mypy .

init-db: ## Создать таблицы в ClickHouse
	uv run python -c "import clickhouse_connect; from config import Settings; s = Settings(); c = clickhouse_connect.get_client(host=s.clickhouse_host, port=s.clickhouse_port, username=s.clickhouse_user, password=s.clickhouse_password); [c.command(q) for q in open('db/tables.sql').read().split(';') if q.strip()]"