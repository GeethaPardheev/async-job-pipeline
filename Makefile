SHELL := /bin/bash
BIN := bin/server
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

# Local defaults target native PostgreSQL (see `make db-setup`). Override as needed.
DATABASE_URL ?= postgres://jobq:jobq@localhost:5432/jobq?sslmode=disable
TEST_DATABASE_URL ?= $(DATABASE_URL)

.PHONY: all build fmt fmt-check vet test test-integration test-all migrate run-api run-worker db-setup docker-build compose-up compose-down clean

all: fmt-check vet build test-all

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/server

fmt:
	gofmt -w .

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...
	go vet -tags=integration ./...

test:
	go test -race -count=1 ./...

test-integration:
	TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -race -count=1 -tags=integration ./internal/integration/...

test-all: test test-integration

migrate: build
	DATABASE_URL='$(DATABASE_URL)' ./$(BIN) migrate

run-api: build
	DATABASE_URL='$(DATABASE_URL)' ./$(BIN) api

run-worker: build
	DATABASE_URL='$(DATABASE_URL)' ./$(BIN) worker

# Creates the local role and database on a native PostgreSQL install (needs sudo; never used by the app).
db-setup:
	sudo -u postgres psql -qv ON_ERROR_STOP=1 -c "DO \$$\$$BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='jobq') THEN CREATE ROLE jobq LOGIN PASSWORD 'jobq'; END IF; END\$$\$$;"
	sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='jobq'" | grep -q 1 || sudo -u postgres createdb -O jobq jobq

docker-build:
	docker build --build-arg VERSION=$(VERSION) -t jobq:$(VERSION) .

compose-up:
	docker compose up --build -d

compose-down:
	docker compose down -v

clean:
	rm -rf bin
