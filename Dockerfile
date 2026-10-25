# Multi-stage Docker build for ShardVault
FROM golang:alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and embedded web UI
COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /shardvault .

# Production runner image
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

COPY --from=builder /shardvault /root/shardvault

# Expose HTTP dashboard port and P2P TCP ports
EXPOSE 8080 3000 5000 7000

ENV PORT=8080

CMD ["./shardvault"]
