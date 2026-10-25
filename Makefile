build:
	@go build -o bin/shardvault .

run: build
	@./bin/shardvault

test:
	@go test ./... -v

docker-build:
	@docker build -t shardvault .

docker-run:
	@docker compose up --build