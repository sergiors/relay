package config

import (
	"fmt"
	"os"
)

// ConsumerName resolves the Redis consumer-group consumer name for this worker
// instance: always the hostname. Docker and Kubernetes give each container/pod
// a unique hostname (the container ID / pod name), so scaled replicas get
// distinct consumer identities by default with no per-replica configuration;
// uniqueness is the platform's job. The name is stable for the process lifetime
// — the hostname does not change while running. There is deliberately NO
// environment override: a static shared name would reintroduce the
// shared-PEL/reclaim-race hazard this resolves. It returns an error only when
// the hostname cannot be resolved or is empty.
func ConsumerName() (string, error) {
	return consumerNameFromHost(os.Hostname())
}

// consumerNameFromHost validates a resolved hostname into a consumer name. It
// is a tiny pure helper so the failure paths (hostname error / empty hostname)
// are unit-testable without stubbing os.Hostname, which is not injectable.
func consumerNameFromHost(host string, err error) (string, error) {
	if err != nil {
		return "", fmt.Errorf("resolve consumer name: hostname unavailable: %w", err)
	}
	if host == "" {
		return "", fmt.Errorf("resolve consumer name: hostname is empty")
	}
	return host, nil
}
