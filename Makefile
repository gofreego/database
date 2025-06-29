setup-db:
	docker-compose -f docker-compose.yml up -d\

test:
	docker-compose up -d
	go test -v ./...
	docker-compose down