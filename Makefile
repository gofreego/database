# constants 
TEST_DB_COMPOSE_FILE = docker-compose-test-sql-db.yml

# This will setup database on local env. 
setup-db-up:
	docker-compose -f $(TEST_DB_COMPOSE_FILE) up -d\

# This will take databases down on local env.
setup-db-down:
	docker-compose -f $(TEST_DB_COMPOSE_FILE) down

# To run the test cases and generate coverage file
test:
	make setup-db-up
	echo "waiting 5 seconds for db to be up and running"
	sleep 5
	go test -v -count=1 -cover -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	make setup-db-down


test-failed:
	make test | grep -i "FAIL"

# To view the coverage on Google Chrome
view-coverage:
	open -a "Google Chrome" coverage.html