# Build stage: golang image must satisfy go.mod's `go 1.27.0`.
FROM golang:1.27-alpine AS build
WORKDIR /src

# Copy module files first so `go mod download` is cached unless they change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 keeps the binary static and self-contained (no libc needed at
# runtime); the Redis and Docker clients are pure Go over TCP/unix socket.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /app/bin/relay ./cmd

# Runtime stage: only the binary and CA/timezone data are needed.
FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=build /app/bin/relay ./bin/relay

CMD ["./bin/relay"]
