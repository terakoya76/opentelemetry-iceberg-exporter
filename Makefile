.PHONY: build test clean docker-build docker-tag docker-push docker-up docker-down lint fmt install-builder generate

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
BINARY_NAME=opentelemetry-collector

# Docker parameters
DOCKER_IMAGE=opentelemetry-collector
DOCKER_REGISTRY=ghcr.io/terakoya76/opentelemetry-iceberg-exporter
DOCKER_TAG=latest

# OpenTelemetry Collector Builder version (must match otelcol_version in builder-config.yaml)
OCB_VERSION=0.143.0
OCB=$(shell which builder 2>/dev/null || echo $(GOPATH)/bin/builder)

all: build

# Install OpenTelemetry Collector Builder
install-builder:
	$(GOCMD) install go.opentelemetry.io/collector/cmd/builder@v$(OCB_VERSION)

# Generate collector code using ocb
generate: install-builder
	$(OCB) --config builder-config.yaml

# Build the collector binary
build: generate
	cd dist && $(GOBUILD) -o ../$(BINARY_NAME) .

# Build without regenerating (for faster iteration after initial generate)
build-only:
	cd dist && $(GOBUILD) -o ../$(BINARY_NAME) .

test:
	$(GOTEST) -v ./...

clean:
	rm -f $(BINARY_NAME)
	rm -rf dist/

lint:
	golangci-lint run ./...

fmt:
	gofmt -s -w .

tidy:
	$(GOMOD) tidy

docker-build:
	docker build -t $(DOCKER_IMAGE):latest .

docker-tag:
	docker tag $(DOCKER_IMAGE):latest $(DOCKER_REGISTRY)/$(DOCKER_IMAGE):$(DOCKER_TAG)

docker-push:
	docker push $(DOCKER_REGISTRY)/$(DOCKER_IMAGE):$(DOCKER_TAG)

docker-up:
	docker-compose -f example/docker-compose.yaml up -d

docker-down:
	docker-compose -f example/docker-compose.yaml down -v

docker-logs:
	docker-compose -f example/docker-compose.yaml logs -f
