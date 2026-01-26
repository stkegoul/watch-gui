.PHONY: build watch sync sync-once help clean install

BINARY_NAME=blnk-watch
CMD_DIR=cmd/blnk-watch

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

build: ## Build the blnk-watch binary
	@echo "Building $(BINARY_NAME)..."
	@cd $(CMD_DIR) && go build -o ../../$(BINARY_NAME) .
	@echo "Build complete: ./$(BINARY_NAME)"

install: build ## Build and install to GOPATH/bin
	@go install ./$(CMD_DIR)
	@echo "Installed to $$(go env GOPATH)/bin/$(BINARY_NAME)"

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
