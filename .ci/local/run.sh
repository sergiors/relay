#!/bin/sh
set -eu

/usr/local/bin/dockerd-entrypoint.sh dockerd > /var/log/dockerd.log 2>&1 &
docker_pid=$!

trap 'kill "$docker_pid" 2>/dev/null || true' EXIT

attempt=0
until docker info >/dev/null 2>&1; do
    attempt=$((attempt + 1))

    if [ "$attempt" -ge 60 ]; then
        cat /var/log/dockerd.log
        exit 1
    fi

    sleep 1
done

ensure_go_cache_ownership() {
    ci_uid=$(id -u ci)
    mkdir -p /home/ci/.cache/go-build /go/pkg/mod
    if [ "$(stat -c %u /home/ci/.cache/go-build)" != "$ci_uid" ] ||
        [ "$(stat -c %u /go/pkg/mod)" != "$ci_uid" ]; then
        chown -R ci:docker /home/ci/.cache/go-build /go/pkg/mod
    fi
}

run_unit() {
    ensure_go_cache_ownership
    su-exec ci env GOCACHE=/home/ci/.cache/go-build go vet ./...
    su-exec ci make GOCACHE=/home/ci/.cache/go-build test-unit
    su-exec ci make GOCACHE=/home/ci/.cache/go-build test-unit-race
}

run_integration() {
    ensure_go_cache_ownership
    docker pull redis:8-alpine
    su-exec ci env GOCACHE=/home/ci/.cache/go-build go vet -tags=integration ./...
    su-exec ci make GOCACHE=/home/ci/.cache/go-build test-integration
    su-exec ci make GOCACHE=/home/ci/.cache/go-build test-integration-race
}

run_python() {
    (
        cd packages/python
        uv sync --locked
        uv run mypy
        uv build
        uv run pytest
    )
}

run_node() {
    (
        export npm_config_store_dir=/home/ci/.local/share/pnpm/store
        cd packages/node
        pnpm install --frozen-lockfile
        pnpm test
        pnpm pack --dry-run
    )
}

case "${1:-all}" in
    unit)
        run_unit
        ;;
    integration)
        run_integration
        ;;
    python)
        run_python
        ;;
    node)
        run_node
        ;;
    all)
        run_unit
        run_integration
        run_python
        run_node
        ;;
    *)
        echo "Usage: run-ci [unit|integration|python|node|all]"
        exit 2
        ;;
esac
