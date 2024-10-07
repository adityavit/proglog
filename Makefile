# Makefile for proglog project

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get

# Protoc parameters
PROTOC=protoc
PROTOC_VERSION=3.20.3
PROTO_PATH=api/v1
GO_OUT=api/v1

# Main package path
MAIN_PATH=./cmd

# Artifacts
ARTIFACTS_PATH=./artifacts

# Binary name
BINARY_NAME=proglog

# Build the project
build: compile_proto
	$(GOBUILD) -o $(ARTIFACTS_PATH)/$(BINARY_NAME) -v $(MAIN_PATH)

# Compile Protocol Buffers
compile_proto:
	$(PROTOC) --proto_path=$(PROTO_PATH) --go_out=$(GO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(GO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_PATH)/log.proto

# Run the server
run: build
	./$(ARTIFACTS_PATH)/$(BINARY_NAME)

# Clean build files
clean:
	$(GOCLEAN)
	rm -f $(ARTIFACTS_PATH)/$(BINARY_NAME)
	rm -f $(GO_OUT)/v1/*.pb.go

# Run tests
test:
	$(GOTEST) -race -v ./...

# Install Protoc
install_protoc:
	curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-linux-x86_64.zip
	sudo unzip -o protoc-$(PROTOC_VERSION)-linux-x86_64.zip -d /usr/local
	rm -f protoc-$(PROTOC_VERSION)-linux-x86_64.zip
	protoc --version

# Get dependencies
deps:
	$(GOGET) ./...
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Build and run
all: build run

.PHONY: build compile_proto run clean test deps all

.PHONY: vet
vet:
	go vet ./...

.PHONY: lint
lint:
	@if ! command -v golangci-lint &> /dev/null; then \
		echo "golangci-lint not found, installing..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
	fi
	golangci-lint run

.PHONY: check
check: vet lint test