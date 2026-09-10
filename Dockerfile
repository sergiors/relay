FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

# Copy module files first so `go mod download` is cached unless they change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 keeps the binaries static and allows native Go
# cross-compilation for the requested target architecture.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /relay \
    ./cmd/cli && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /relay-worker \
    ./cmd/worker

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata

COPY --from=build /relay /usr/local/bin/relay
COPY --from=build /relay-worker /usr/local/bin/relay-worker

WORKDIR /app

CMD ["relay-worker"]
