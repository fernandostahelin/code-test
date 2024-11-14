.PHONY: up
up:
	docker compose up -d;

.PHONY: down
down:
	docker compose down -v
	@if [[ "$(docker ps -q -f name=${DOCKER_CONTAINER})" ]]; then \
		echo "Terminating running container..."; \
		docker rm ${POSTGRES_SOURCE_CONTAINER}; \
		docker rm ${POSTGRES_ANALYTICS_CONTAINER}; \
	fi

.PHONY: restart
restart:
	docker compose down -v; \
	sleep 5; \
	docker compose up -d;

.PHONY: logs-source
logs-source:
	docker container logs ${POSTGRES_SOURCE_CONTAINER};

.PHONY: logs-analytics
logs-analytics:
	docker container logs ${POSTGRES_ANALYTICS_CONTAINER};

.PHONY: all
all:
	uv run isort .; \
	uv run ruff check .; \
	uv run ruff format .; \
	uv run sqlfluff lint . --dialect postgres; \
	uv run sqlfluff fix . --dialect postgres; \
	uv run sqlfluff format . --dialect postgres; \


