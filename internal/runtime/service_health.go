package runtime

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/app"
	"relay/internal/runtime/plan"
)

// ServiceHealthcheckLabel returns the canonical identity of a service's desired
// healthcheck configuration, stamped as relay.healthcheck so a configuration
// change replaces the running container even when its image and environment are
// unchanged. An empty result (no healthcheck) omits the label, keeping an
// unrelated service's label set byte-for-byte identical to the pre-healthcheck
// shape and letting an existing unlabeled container remain converged.
func ServiceHealthcheckLabel(hc *app.ServiceHealthcheck) string {
	if hc == nil {
		return ""
	}
	h := sha256.New()
	for _, field := range []string{
		hc.Path,
		hc.Interval.String(),
		hc.Timeout.String(),
		strconv.Itoa(hc.Retries),
		hc.StartPeriod.String(),
	} {
		_, _ = h.Write([]byte(field))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:serviceIdentityHashLen]
}

// EffectiveServiceHealthcheck returns the healthcheck Relay actually applies to
// a service: the declared probe, unless the service's current generation runs
// from a live source mount (HOT_RELOAD). Callers pass the source-mount fact they
// already resolved so reconciliation and discovery share one policy rule.
func EffectiveServiceHealthcheck(
	hc *app.ServiceHealthcheck, sourceMounted bool,
) *app.ServiceHealthcheck {
	if sourceMounted {
		return nil
	}
	return hc
}

// serviceHealthConfig builds Docker's native HEALTHCHECK configuration for a
// managed entrypoint service. The probe is an exec-form command using the same
// node/python executable the service entrypoint runs with, so it relies on no
// shell and no external tool inside the image. It performs one HTTP GET against
// 127.0.0.1:<port><path> and exits 0 for a 2xx/3xx response, 1 otherwise
// (connection error, timeout, 4xx/5xx).
//
// External `image` services never reach this path: the template parser rejects a
// healthcheck on them, because Relay cannot assume any probe tooling in an image
// it does not build.
func serviceHealthConfig(
	runtimeName string,
	port int,
	hc *app.ServiceHealthcheck,
) (*container.HealthConfig, error) {
	if hc == nil {
		return nil, nil
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("healthcheck: invalid service port %d", port)
	}
	if err := validateServiceHealthcheck(hc); err != nil {
		return nil, err
	}
	spec, err := lookup(runtimeName)
	if err != nil {
		return nil, fmt.Errorf("healthcheck: unsupported runtime %q", runtimeName)
	}
	var test []string
	switch spec.Engine {
	case plan.EngineNode:
		test = []string{
			"CMD", "node", "-e", nodeHealthcheckScript,
			strconv.Itoa(port), hc.Path,
			strconv.FormatInt(hc.Timeout.Milliseconds(), 10),
		}
	case plan.EnginePython:
		test = []string{
			"CMD", "python", "-c", pythonHealthcheckScript,
			strconv.Itoa(port), hc.Path,
			strconv.FormatFloat(hc.Timeout.Seconds(), 'f', -1, 64),
		}
	default:
		return nil, fmt.Errorf("healthcheck: unsupported runtime %q", runtimeName)
	}
	return &container.HealthConfig{
		Test:        test,
		Interval:    hc.Interval,
		Timeout:     hc.Timeout,
		StartPeriod: hc.StartPeriod,
		Retries:     hc.Retries,
	}, nil
}

// nodeHealthcheckScript is invoked as
// `node -e <script> <port> <path> <timeout-ms>`. It uses only Node's built-in
// http module and never follows redirects, so the probe verdict is the status
// of the exact response to the configured path. The source is embedded from
// healthcheck.mjs, which is not part of the runtime module set shipped into an
// image (see the node engine).
//
//go:embed node/healthcheck.mjs
var nodeHealthcheckScript string

// pythonHealthcheckScript is invoked as
// `python -c <script> <port> <path> <timeout-seconds>`. It uses only the standard
// library and does not follow redirects. The source is embedded from
// healthcheck.py, which is not part of the runtime module set shipped into an
// image (see the python engine).
//
//go:embed python/healthcheck.py
var pythonHealthcheckScript string

// validateServiceHealthcheck enforces the same policy as template parsing at the
// runtime boundary, so a direct ServiceSpec caller cannot install a zero or
// inverted probe configuration.
func validateServiceHealthcheck(hc *app.ServiceHealthcheck) error {
	if hc == nil {
		return nil
	}
	if hc.Path == "" || hc.Path[0] != '/' {
		return fmt.Errorf("healthcheck: path %q must start with %q", hc.Path, "/")
	}
	if strings.ContainsAny(hc.Path, " \t\r\n?#\\") {
		return fmt.Errorf(
			"healthcheck: path %q must not contain whitespace, a query, a fragment, or a backslash", hc.Path)
	}
	if hc.Interval < app.MinServiceHealthcheckInterval || hc.Interval > app.MaxServiceHealthcheckInterval {
		return fmt.Errorf("healthcheck: interval %s is out of range", hc.Interval)
	}
	if hc.Timeout < app.MinServiceHealthcheckTimeout || hc.Timeout > app.MaxServiceHealthcheckTimeout {
		return fmt.Errorf("healthcheck: timeout %s is out of range", hc.Timeout)
	}
	if hc.Timeout > hc.Interval {
		return fmt.Errorf("healthcheck: timeout %s must not exceed interval %s", hc.Timeout, hc.Interval)
	}
	if hc.Retries < 1 || hc.Retries > app.MaxServiceHealthcheckRetries {
		return fmt.Errorf("healthcheck: retries %d is out of range", hc.Retries)
	}
	if hc.StartPeriod < 0 || hc.StartPeriod > app.MaxServiceHealthcheckStartPeriod {
		return fmt.Errorf("healthcheck: start_period %s is out of range", hc.StartPeriod)
	}
	return nil
}

// ServiceHealthCandidateExpired reports whether a config-matching replacement
// has exhausted its startup grace: Docker reports it unhealthy, or it is still
// "starting" past the durable deadline derived from the instant Docker actually
// STARTED it. Create time is deliberately not used: a container can be created
// long before it starts (a delayed start, an engine that serialized the start),
// and measuring the grace from create time would fail a freshly started
// candidate early. Without a durable start instant the window cannot be proven
// elapsed, so the candidate is not expired; Docker's own unhealthy transition
// remains the terminal signal and the coordinator re-observes at a bounded
// cadence.
func ServiceHealthCandidateExpired(ctr ServiceContainer, hc *app.ServiceHealthcheck) bool {
	if hc == nil {
		return false
	}
	if ctr.Health == container.Unhealthy {
		return true
	}
	if ctr.Health != container.Starting && ctr.Health != "" {
		return false
	}
	if ctr.StartedAt.IsZero() {
		return false
	}
	deadline := ctr.StartedAt.Add(
		hc.StartPeriod + time.Duration(hc.Retries+1)*hc.Interval)
	return !time.Now().Before(deadline)
}
