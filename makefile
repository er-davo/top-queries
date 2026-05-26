TARGET_RPS ?= 100
KAFKA_BROKER ?= localhost:29092
KAFKA_TOPIC ?= wb-search-queries

api-gen:
	oapi-codegen -config=oapi-codegen.yaml api.yaml

setup:
	cp .env.example .env

up:
	docker compose up --build -d

down:
	docker compose down -v

docker-logs:
	docker compose logs -f top-queries-app

load-test:
	docker run --rm -i --network=host grafana/k6 run - <load-test.js

load-test-blazing:
	docker run --rm --network=host williamyeh/wrk -t16 -c400 -d30s http://localhost:8080/api/v1/top?limit=50

cover:
	go -C ./app test -coverprofile=coverage.out ./...
	go -C ./app tool cover -func=coverage.out
	@del /f /q app\coverage.out 2>nul || rm -f app/coverage.out

cover-html:
	go -C ./app test -coverprofile=coverage.out ./...
	go -C ./app tool cover -html=coverage.out
	@del /f /q app\coverage.out 2>nul || rm -f app/coverage.out