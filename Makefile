.PHONY: build start watch sync sync-once help clean install

BINARY_NAME=blnk-watch
CMD_DIR=cmd/blnk-watch
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS = -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

build: ## Build the blnk-watch binary
	@echo "Building $(BINARY_NAME)..."
	@cd $(CMD_DIR) && go build -ldflags "$(LDFLAGS)" -o ../../$(BINARY_NAME) .
	@echo "Build complete: ./$(BINARY_NAME)"

install: build ## Build and install to GOPATH/bin
	@go install -ldflags "$(LDFLAGS)" ./$(CMD_DIR)
	@echo "Installed to $$(go env GOPATH)/bin/$(BINARY_NAME)"

start: build ## Run watch service and watermark sync in one process
	@./$(BINARY_NAME) -command=start

watch: build ## Run the watch service (default command)
	@./$(BINARY_NAME) -command=watch

sync: build ## Run the watermark sync service continuously
	@./$(BINARY_NAME) -command=sync

sync-once: build ## Run a one-time watermark sync
	@./$(BINARY_NAME) -command=sync-once

clean: ## Remove built binaries
	@rm -f $(BINARY_NAME)
	@echo "Cleaned up binaries"

test: ## Run tests
	@go test ./...
