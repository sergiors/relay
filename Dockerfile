# Build stage: golang image must satisfy go.mod's `go 1.27.0`.
FROM golang:1.27-alpine AS build

WORKDIR /src

# Copy module files first so `go mod download` is cached unless they change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 keeps the binary static and self-contained.
RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /relay \
    ./cmd

# Runtime stage.
FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata

COPY --from=build /relay /usr/local/bin/relay

WORKDIR /app

CMD ["relay"]
