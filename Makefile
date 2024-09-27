# Makefile for proglog project

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get

# Main package path
MAIN_PATH=./cmd

# Binary name
BINARY_NAME=proglog

# Build the project
build:
	$(GOBUILD) -o $(BINARY_NAME) -v $(MAIN_PATH)

# Run the server
run: build
	./$(BINARY_NAME)

# Clean build files
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)

# Run tests
test:
	$(GOTEST) -v ./...

# Get dependencies
deps:
	$(GOGET) ./...

# Build and run
all: build run

.PHONY: build run clean test deps all