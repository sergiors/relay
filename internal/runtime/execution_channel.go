package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/moby/moby/api/types/network"
)

// Bounds for establishing the execution channel. A cold container must become
// reachable and authenticated within executionReadyTimeout; that whole window
// (rather than a single dial) covers the gap between ContainerStart returning
// and the bootstrap entrypoint binding its listener.
const (
	executionConnectTimeout   = 2 * time.Second
	executionReadyTimeout     = 10 * time.Second
	executionHandshakeTimeout = 5 * time.Second
	executionDialRetryDelay   = 100 * time.Millisecond
)

// executionBootstrapEnv appends the Relay-owned channel variables to an app's
// plan env. Same-named entries supplied by the app are removed first, so an app
// template can never redirect the bootstrap listener or replace the credential.
func executionBootstrapEnv(env []string, secret string) []string {
	out := make([]string, 0, len(env)+2)
	for _, kv := range env {
		if strings.HasPrefix(kv, executionEnvPort+"=") ||
			strings.HasPrefix(kv, executionEnvSecret+"=") {
			continue
		}
		out = append(out, kv)
	}
	return append(out,
		executionEnvPort+"="+strconv.Itoa(executionPort),
		executionEnvSecret+"="+secret,
	)
}

// executionAddresses returns every usable bootstrap address for a just-started
// execution container, in deterministic order. Addresses are considered only on
// the caller-supplied networks; production passes exactly the internal control
// network, so an application network never becomes the execution IPC path. An
// unconfigured network, even one Docker reports, is never used. IPv4 candidates
// come first across all given networks, then IPv6, and duplicate addresses are
// removed. Relay tries the candidates in this order until one authenticates.
//
// This verifies the container's side of the control-network requirement: Relay
// created the container on the control network and the daemon reports a usable
// address there. It cannot verify that the Relay PROCESS has IP connectivity to
// the address (a containerized Relay attaches itself to the same network; a
// host process relies on verified native bridge routing). That remains a
// startup-readiness concern, and no reachable candidate fails closed.
func executionAddresses(
	networks []string, settings map[string]*network.EndpointSettings, port int,
) ([]string, error) {
	set := NetworkSet(networks)
	if len(set) == 0 {
		return nil, errors.New(
			"execution container has no configured control network",
		)
	}
	var addresses []string
	seen := make(map[string]bool, len(set)*2)
	for _, wantIPv4 := range []bool{true, false} {
		for _, name := range set {
			endpoint, ok := settings[name]
			if !ok || endpoint == nil {
				continue
			}
			addr := endpoint.IPAddress
			if !wantIPv4 {
				addr = endpoint.GlobalIPv6Address
			}
			if !usableExecutionAddress(addr, wantIPv4) {
				continue
			}
			address := net.JoinHostPort(addr.String(), strconv.Itoa(port))
			if seen[address] {
				continue
			}
			seen[address] = true
			addresses = append(addresses, address)
		}
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf(
			"execution container has no usable address on configured networks %v; "+
				"verify the container joined one of them and Relay can route to its bridge",
			set,
		)
	}
	return addresses, nil
}

// usableExecutionAddress reports whether addr is a concrete unicast address
// suitable for dialing.
func usableExecutionAddress(addr netip.Addr, wantIPv4 bool) bool {
	if !addr.IsValid() || addr.IsUnspecified() || addr.IsLoopback() || addr.IsLinkLocalUnicast() {
		return false
	}
	if wantIPv4 {
		return addr.Is4()
	}
	return addr.Is6() && !addr.Is4In6()
}

// connectExecutionChannel dials the container's bootstrap and authenticates the
// first reachable candidate. The returned connection is ready for invocation
// frames; the caller owns it for the container's lifetime.
//
// Candidates are tried in deterministic order on every pass. Connection-refused
// is the normal transient result while the container entrypoint is still binding,
// so a pass that saw one is retried until the bounded readiness window expires.
// A candidate that cannot be dialed does not abort the pass: a container with
// several candidate addresses succeeds if any candidate is reachable. A TCP peer
// that completes dialing but fails authentication fails the start rather than
// being retried, because another pass cannot repair a wrong credential or
// protocol.
func connectExecutionChannel(ctx context.Context, addresses []string, secret string) (net.Conn, error) {
	if len(addresses) == 0 {
		return nil, errors.New("execution channel: no candidate addresses")
	}
	readyCtx, cancel := context.WithTimeout(ctx, executionReadyTimeout)
	defer cancel()

	var lastErr error
	for {
		retryable := false
		var authErr error
		for _, address := range addresses {
			if err := readyCtx.Err(); err != nil {
				return nil, executionChannelError(err, addresses, lastErr)
			}
			dialCtx, dialCancel := context.WithTimeout(readyCtx, executionConnectTimeout)
			conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", address)
			dialCancel()
			if err != nil {
				lastErr = fmt.Errorf("%s: %w", address, err)
				if errors.Is(err, syscall.ECONNREFUSED) {
					retryable = true
				}
				continue
			}
			if err := handshakeExecutionChannel(readyCtx, conn, secret); err != nil {
				_ = conn.Close()
				authErr = fmt.Errorf("%s: %w", address, err)
				continue
			}
			if ctxErr := readyCtx.Err(); ctxErr != nil {
				_ = conn.Close()
				return nil, executionChannelError(ctxErr, addresses, lastErr)
			}
			return conn, nil
		}
		if authErr != nil {
			if ctxErr := readyCtx.Err(); ctxErr != nil {
				return nil, executionChannelError(ctxErr, addresses, lastErr)
			}
			return nil, executionChannelError(authErr, addresses, lastErr)
		}
		if !retryable {
			if lastErr == nil {
				lastErr = errors.New("no candidate could be dialed")
			}
			return nil, executionChannelError(lastErr, addresses, nil)
		}
		timer := time.NewTimer(executionDialRetryDelay)
		select {
		case <-readyCtx.Done():
			timer.Stop()
			return nil, executionChannelError(readyCtx.Err(), addresses, lastErr)
		case <-timer.C:
		}
	}
}

// executionChannelError reports the connection failure with every attempted
// candidate address. It never includes the credential or other secret material.
func executionChannelError(cause error, addresses []string, lastErr error) error {
	if lastErr == nil {
		return fmt.Errorf("execution channel: %w (candidates %v)", cause, addresses)
	}
	return fmt.Errorf("execution channel: %w (candidates %v; last error: %v)",
		cause, addresses, lastErr)
}

// handshakeExecutionChannel performs the nonce/HMAC challenge-response and
// leaves the connection with no read/write deadline. The bootstrap only reports
// auth once it is ready to read invocation frames, so a verified response is also
// the readiness check.
func handshakeExecutionChannel(ctx context.Context, conn net.Conn, secret string) error {
	nonce, err := newHandshakeNonce()
	if err != nil {
		return err
	}
	deadline := time.Now().Add(executionHandshakeTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return fmt.Errorf("execution channel handshake: set deadline: %w", err)
	}
	// Cancellation closes the connection so a blocked handshake read returns
	// immediately. stop() reporting false means the callback started (or will),
	// so the connection is no longer usable and cancellation wins.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	payload, err := json.Marshal(handshakeRequest{
		Type:    frameTypeHello,
		Version: executionProtocolVersion,
		Nonce:   nonce,
	})
	if err != nil {
		stop()
		return fmt.Errorf("execution channel handshake: encode hello: %w", err)
	}
	if err := writeFrame(conn, payload, maxHandshakeFrame); err != nil {
		stop()
		return fmt.Errorf("execution channel handshake: write hello: %w", err)
	}
	raw, err := readFrame(conn, maxHandshakeFrame)
	if err != nil {
		stop()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("execution channel handshake: %w", ctxErr)
		}
		return fmt.Errorf("execution channel handshake: read auth: %w", err)
	}
	var response handshakeResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		stop()
		return fmt.Errorf("execution channel handshake: malformed auth frame: %w", err)
	}
	if response.Type != frameTypeAuth || response.Version != executionProtocolVersion {
		stop()
		return errors.New("execution channel handshake: unexpected auth frame")
	}
	if len(response.Nonce) != 64 {
		stop()
		return errors.New("execution channel handshake: malformed bootstrap nonce")
	}
	if !verifyExecutionChallenge(
		secret, executionBootstrapAuthContext, nonce, response.MAC,
	) {
		stop()
		return errors.New("execution channel handshake: bootstrap authentication failed")
	}
	// Answer the bootstrap's challenge. The bootstrap verifies this before it
	// accepts invocation frames, so an unauthenticated sibling container cannot
	// drive handlers directly.
	ready, err := json.Marshal(handshakeReady{
		Type:    frameTypeReady,
		Version: executionProtocolVersion,
		MAC:     executionChallengeHex(secret, executionClientAuthContext, response.Nonce),
	})
	if err != nil {
		stop()
		return fmt.Errorf("execution channel handshake: encode ready: %w", err)
	}
	if err := writeFrame(conn, ready, maxHandshakeFrame); err != nil {
		stop()
		return fmt.Errorf("execution channel handshake: write ready: %w", err)
	}
	if !stop() {
		return fmt.Errorf("execution channel handshake: %w", ctx.Err())
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return fmt.Errorf("execution channel handshake: clear deadline: %w", err)
	}
	return nil
}
