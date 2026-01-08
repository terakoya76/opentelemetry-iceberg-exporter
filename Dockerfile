# Build stage
FROM golang:1.24-alpine AS builder

ARG OCB_VERSION=0.142.0

WORKDIR /build

RUN apk add --no-cache git gcc musl-dev
RUN go install go.opentelemetry.io/collector/cmd/builder@v${OCB_VERSION}

COPY . .

# Generate collector code using ocb
RUN builder --config builder-config.yaml

# Build the collector binary
WORKDIR /build/dist
RUN CGO_ENABLED=0 GOOS=linux go build -o /opentelemetry-collector .

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install CA certificates for HTTPS and curl for healthcheck
RUN apk add --no-cache ca-certificates curl

COPY --from=builder /opentelemetry-collector /app/opentelemetry-collector

EXPOSE 4317 4318 13133

ENTRYPOINT ["/app/opentelemetry-collector"]
