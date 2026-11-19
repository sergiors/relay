FROM golang:1.27.0-alpine3.24 AS golang
FROM ghcr.io/astral-sh/uv:0.9.26 AS uv

FROM docker:29.9.2-dind-alpine3.24

RUN apk add --no-cache \
    bash \
    build-base \
    ca-certificates \
    git \
    make \
    nodejs \
    npm \
    python3 \
    openssh-client \
    su-exec

COPY --from=golang /usr/local/go /usr/local/go
COPY --from=uv /uv /uvx /usr/local/bin/

ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH="/go"

RUN npm install -g pnpm@12.15.0

RUN adduser -D -u 1000 -G docker ci \
    && mkdir -p /home/ci/.cache/go-build /go/pkg/mod /home/ci/.cache/uv \
    && chown -R ci:docker /home/ci/.cache /go/pkg/mod /home/ci

ENV HOME="/home/ci"

COPY run.sh /usr/local/bin/run-ci
RUN chmod +x /usr/local/bin/run-ci

WORKDIR /src

ENTRYPOINT ["/usr/local/bin/run-ci"]
