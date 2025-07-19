# constants 
TEST_DB_COMPOSE_FILE = docker-compose-test-sql-db.yml

# This will setup database on local env. 
setup-db-up:
	docker-compose -f $(TEST_DB_COMPOSE_FILE) up -d

# This will take databases down on local env.
setup-db-down:
	docker-compose -f $(TEST_DB_COMPOSE_FILE) down

# To run the test cases and generate coverage file
test-static:
	go test -v -count=1 -cover -coverprofile=coverage.out ./sql ./sql/impls/... ./sql/migrator ./sql/sqlfactory ./sql/internal | grep -E "(coverage|FAIL)"
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html

test:
	make setup-db-up
	echo "waiting 10 seconds for db to be up and running"
	sleep 10
	go test -v -count=1 -cover -coverprofile=coverage.out ./... | grep -E "(coverage|FAIL)"
	make setup-db-down

# To view the coverage on Google Chrome
view-coverage:
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	open -a "Google Chrome" coverage.html

clean:
	rm -f coverage.out
	rm -f coverage.html