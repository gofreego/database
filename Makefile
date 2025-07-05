# constants 
TEST_DB_COMPOSE_FILE = docker-compose-test-sql-db.yml

setup-db-up:
	docker-compose -f $(TEST_DB_COMPOSE_FILE) up -d\

setup-db-down:
	docker-compose -f $(TEST_DB_COMPOSE_FILE) down

test:
	make setup-db-up
	go test -v -cover -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	make setup-db-down