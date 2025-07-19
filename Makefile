# constants 
TEST_DB_COMPOSE_FILE = docker-compose-test-sql-db.yml

# This will setup database on local env. 
setup-db-up:
	docker-compose -f $(TEST_DB_COMPOSE_FILE) up -d

# This will take databases down on local env.
setup-db-down:
	docker-compose -f $(TEST_DB_COMPOSE_FILE) down

# Wait for databases to be ready
wait-for-dbs:
	@echo "Waiting for databases to be ready..."
	@until docker exec postgres_db pg_isready -U root -d postgres > /dev/null 2>&1; do echo "Waiting for PostgreSQL..."; sleep 2; done
	@until docker exec mysql_db mysqladmin ping -h localhost -u root -proot@1234 > /dev/null 2>&1; do echo "Waiting for MySQL..."; sleep 2; done
	@until docker exec mssql_db /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P root@1234 -Q "SELECT 1" -C > /dev/null 2>&1; do echo "Waiting for MSSQL..."; sleep 2; done
	@echo "All databases are ready!"

# To run the test cases and generate coverage file
test-static:
	go test -v -count=1 -cover -coverprofile=coverage.out ./sql ./sql/impls/... ./sql/migrator ./sql/sqlfactory ./sql/internal | grep -E "(coverage|FAIL)"

test:
	make setup-db-up
	make wait-for-dbs
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