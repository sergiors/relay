package runtime

// Unit tests for establishing the execution channel (execution_channel.go):
// the scripted nonce/HMAC handshake, the dial/retry policy over loopback TCP,
// candidate-address selection on the configured shared networks, and the
// Relay-owned bootstrap environment. Loopback stands in for the container
// address so these tests exercise the REAL sockets without Docker; the
// production container-address dial is covered by the integration suite.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/network"

	"relay/internal/testutil"
)

// bootstrapScript customizes one scripted bootstrap side of the handshake.
type bootstrapScript struct {
	// wrongMAC answers the hello with a MAC computed under the WRONG direction
	// context, which the client must reject.
	wrongMAC bool
	// malformedAuth is sent verbatim instead of a valid auth frame.
	malformedAuth []byte
	// silent reads the hello and never answers.
	silent bool
	// onReadyVerified, when set, is called with nil after the client's ready
	// frame verifies; with a non-nil error the handshake was rejected.
	onReadyVerified func(err error)
	// afterHandshake, when set, receives ONE invoke-limit frame the client
	// sends after a successful handshake (peer-liveness proof), or the read
	// error.
	afterHandshake chan []byte
}

// serveBootstrapHandshake runs one bootstrap side of the contract handshake on
// conn: read hello, reply auth (HMAC over the Relay nonce), read ready, and
// verify it over the bootstrap's own nonce. It returns the first violation.
func serveBootstrapHandshake(conn net.Conn, secret string, s bootstrapScript) error {
	report := func(err error) error {
		if s.onReadyVerified != nil {
			s.onReadyVerified(err)
		}
		return err
	}
	raw, err := readFrame(conn, maxHandshakeFrame)
	if err != nil {
		return report(err)
	}
	var hello handshakeRequest
	if err := json.Unmarshal(raw, &hello); err != nil {
		return report(err)
	}
	if hello.Type != frameTypeHello || hello.Version != executionProtocolVersion ||
		!isLowerHex64(hello.Nonce) {
		return report(errors.New("scripted bootstrap: invalid hello frame"))
	}
	if s.silent {
		_, _ = io.Copy(io.Discard, conn)
		return report(nil)
	}
	if s.malformedAuth != nil {
		if err := writeFrame(conn, s.malformedAuth, maxHandshakeFrame); err != nil {
			return report(err)
		}
		_, _ = io.Copy(io.Discard, conn)
		return report(nil)
	}
	authContext := executionBootstrapAuthContext
	if s.wrongMAC {
		authContext = executionClientAuthContext
	}
	bootstrapNonce, err := newHandshakeNonce()
	if err != nil {
		return report(err)
	}
	auth, err := json.Marshal(handshakeResponse{
		Type:    frameTypeAuth,
		Version: executionProtocolVersion,
		MAC:     executionChallengeHex(secret, authContext, hello.Nonce),
		Nonce:   bootstrapNonce,
	})
	if err != nil {
		return report(err)
	}
	if err := writeFrame(conn, auth, maxHandshakeFrame); err != nil {
		return report(err)
	}
	raw, err = readFrame(conn, maxHandshakeFrame)
	if err != nil {
		return report(err)
	}
	var ready handshakeReady
	if err := json.Unmarshal(raw, &ready); err != nil {
		return report(err)
	}
	if ready.Type != frameTypeReady || ready.Version != executionProtocolVersion {
		return report(errors.New("scripted bootstrap: unexpected ready frame"))
	}
	expected := executionChallengeHex(secret, executionClientAuthContext, bootstrapNonce)
	if !hmacHexEqual(expected, ready.MAC) {
		return report(errors.New("scripted bootstrap: ready MAC does not verify"))
	}
	// Report verification BEFORE any post-handshake exchange so a test waiting
	// on the signal cannot deadlock against its own later write.
	if s.onReadyVerified != nil {
		s.onReadyVerified(nil)
	}
	if s.afterHandshake != nil {
		raw, err = readFrame(conn, maxRequestFrame)
		s.afterHandshake <- raw
		if err != nil {
			return err
		}
	}
	return nil
}

// hmacHexEqual compares two lowercase-hex strings of equal intent without
// pulling crypto/hmac into every test.
func hmacHexEqual(a, b string) bool { return a == b }

func TestHandshakeSuccessOverPipe(t *testing.T) {
	secret, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	relaySide, bootstrapSide := net.Pipe()
	verified := make(chan error, 1)
	go func() {
		_ = serveBootstrapHandshake(bootstrapSide, secret,
			bootstrapScript{onReadyVerified: func(err error) { verified <- err }})
		// Keep the bootstrap side open (draining) until the client closes, so
		// the post-handshake exchange below observes a live connection.
		_, _ = io.Copy(io.Discard, bootstrapSide)
		_ = bootstrapSide.Close()
	}()
	if err := handshakeExecutionChannel(context.Background(), relaySide, secret); err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if err := <-verified; err != nil {
		t.Fatalf("bootstrap side: %v", err)
	}
	// The handshake clears the connection deadline so later frames are not
	// raced by it: a write right now must complete against the live peer.
	if err := writeFrame(relaySide, []byte(`{"probe":true}`), maxHandshakeFrame); err != nil {
		t.Fatalf("post-handshake write: %v", err)
	}
	_ = relaySide.Close()
}

func TestHandshakeRejectsWrongBootstrapMAC(t *testing.T) {
	secret, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	relaySide, bootstrapSide := net.Pipe()
	go func() {
		_ = serveBootstrapHandshake(bootstrapSide, secret, bootstrapScript{wrongMAC: true})
		_ = bootstrapSide.Close()
	}()
	err = handshakeExecutionChannel(context.Background(), relaySide, secret)
	if err == nil || !strings.Contains(err.Error(), "bootstrap authentication failed") {
		t.Fatalf("error = %v, want the authentication-failure rejection", err)
	}
}

func TestHandshakeRejectsMalformedAuth(t *testing.T) {
	shortNonce := strings.Repeat("d", 63)
	cases := []struct {
		name string
		raw  []byte
		want string
	}{
		{"not JSON", []byte("definitely not a response"), "malformed auth frame"},
		{"wrong type", mustMarshalFrame(t, handshakeReady{
			Type: frameTypeReady, Version: executionProtocolVersion,
			MAC: strings.Repeat("e", 64),
		}), "unexpected auth frame"},
		{"wrong version", mustMarshalFrame(t, handshakeResponse{
			Type: frameTypeAuth, Version: 99,
			MAC: strings.Repeat("e", 64), Nonce: strings.Repeat("f", 64),
		}), "unexpected auth frame"},
		{"short nonce", mustMarshalFrame(t, handshakeResponse{
			Type: frameTypeAuth, Version: executionProtocolVersion,
			MAC: strings.Repeat("e", 64), Nonce: shortNonce,
		}), "malformed bootstrap nonce"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			secret, err := newExecutionCredential()
			if err != nil {
				t.Fatalf("credential: %v", err)
			}
			relaySide, bootstrapSide := net.Pipe()
			go func() {
				_ = serveBootstrapHandshake(bootstrapSide, secret,
					bootstrapScript{malformedAuth: tc.raw})
				_ = bootstrapSide.Close()
			}()
			err = handshakeExecutionChannel(context.Background(), relaySide, secret)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestHandshakeCancellationClosesConnection(t *testing.T) {
	secret, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	relaySide, bootstrapSide := net.Pipe()
	silentDone := make(chan struct{})
	go func() {
		_ = serveBootstrapHandshake(bootstrapSide, secret, bootstrapScript{silent: true})
		close(silentDone)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err = handshakeExecutionChannel(ctx, relaySide, secret)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("cancellation took %v, want a prompt release", elapsed)
	}
	// The cancellation callback closed the connection: writing now fails.
	if _, werr := relaySide.Write([]byte("x")); werr == nil {
		t.Fatal("cancellation must close the connection")
	}
	<-silentDone
}

func TestHandshakeBoundedByContextDeadline(t *testing.T) {
	secret, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	relaySide, bootstrapSide := net.Pipe()
	go func() {
		_ = serveBootstrapHandshake(bootstrapSide, secret, bootstrapScript{silent: true})
		_ = bootstrapSide.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = handshakeExecutionChannel(ctx, relaySide, secret)
	if err == nil {
		t.Fatal("a silent peer must fail the handshake")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("handshake took %v, want the context-bound ~200ms", elapsed)
	}
}

// startScriptedBootstrap listens on an ephemeral loopback port and serves the
// scripted bootstrap handshake on every accepted connection, reporting each
// outcome and the accept count.
func startScriptedBootstrap(t *testing.T, secret string, s bootstrapScript) (addr string, results <-chan error, accepts func() int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	out := make(chan error, 8)
	var mu sync.Mutex
	n := 0
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			n++
			mu.Unlock()
			herr := serveBootstrapHandshake(conn, secret, s)
			_ = conn.Close()
			select {
			case out <- herr:
			default:
			}
		}
	}()
	return ln.Addr().String(), out, func() int { mu.Lock(); defer mu.Unlock(); return n }
}

func TestConnectExecutionChannelAuthenticatesLoopback(t *testing.T) {
	secret, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	verified := make(chan error, 1)
	probe := make(chan []byte, 1)
	addr, _, _ := startScriptedBootstrap(t, secret, bootstrapScript{
		onReadyVerified: func(err error) { verified <- err },
		afterHandshake:  probe,
	})
	conn, err := connectExecutionChannel(context.Background(), []string{addr}, secret)
	if err != nil {
		t.Fatalf("connectExecutionChannel: %v", err)
	}
	defer conn.Close()
	if err := <-verified; err != nil {
		t.Fatalf("bootstrap verification: %v", err)
	}
	// The returned connection is live in BOTH directions: a frame written on it
	// reaches the bootstrap peer after the handshake.
	if err := writeFrame(conn, []byte(`{"probe":true}`), maxRequestFrame); err != nil {
		t.Fatalf("write on authenticated channel: %v", err)
	}
	select {
	case raw := <-probe:
		if string(raw) != `{"probe":true}` {
			t.Errorf("peer received %q, want the probe frame", raw)
		}
	case <-time.After(execChannelWatchdog):
		t.Fatal("the bootstrap peer never received the post-handshake frame")
	}
}

func TestConnectExecutionChannelHandshakeFailureIsNotRetried(t *testing.T) {
	secret, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	addr, results, accepts := startScriptedBootstrap(t, secret, bootstrapScript{wrongMAC: true})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	conn, err := connectExecutionChannel(ctx, []string{addr}, secret)
	if err == nil {
		conn.Close()
		t.Fatal("a wrong-MAC bootstrap must fail the start")
	}
	if !strings.Contains(err.Error(), "handshake") &&
		!strings.Contains(err.Error(), "authentication failed") {
		t.Errorf("error = %v, want the handshake/auth failure", err)
	}
	if rerr := <-results; rerr == nil {
		t.Error("the scripted bootstrap must have rejected the ready frame")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("handshake failure took %v, want no dial retry loop", elapsed)
	}
	if got := accepts(); got != 1 {
		t.Errorf("accepted %d connections, want exactly 1 (no retry after a handshake failure)", got)
	}
}

func TestConnectExecutionChannelRefusedRetriesUntilDeadline(t *testing.T) {
	secret, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	// Bind then close to obtain a guaranteed-refused loopback address.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	conn, err := connectExecutionChannel(ctx, []string{addr}, secret)
	if err == nil {
		conn.Close()
		t.Fatal("dialing a closed port must fail")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want the bounded wait to end in context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("refused-dial retry ran for %v, want it bounded by the 500ms context", elapsed)
	}
}

func TestConnectExecutionChannelCancelledContextFailsFast(t *testing.T) {
	secret, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	conn, err := connectExecutionChannel(ctx, []string{"127.0.0.1:1"}, secret)
	if err == nil {
		conn.Close()
		t.Fatal("a cancelled context must fail the connect")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("connect took %v, want an immediate non-retry failure", elapsed)
	}
}

func TestConnectExecutionChannelRejectsEmptyAddressList(t *testing.T) {
	secret, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	conn, err := connectExecutionChannel(context.Background(), nil, secret)
	if err == nil {
		conn.Close()
		t.Fatal("an empty candidate list must fail closed")
	}
	if !strings.Contains(err.Error(), "no candidate addresses") {
		t.Errorf("error = %v, want the empty-candidate rejection", err)
	}
}

// TestConnectExecutionChannelFailsOverAcrossAddresses pins the multi-address
// dial: a refused first candidate (say, an address on a network this process
// cannot use the same way) must not sink the start when a later configured
// candidate authenticates in the same pass — and only the reachable bootstrap
// ever sees a connection.
func TestConnectExecutionChannelFailsOverAcrossAddresses(t *testing.T) {
	secret, err := newExecutionCredential()
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	// A guaranteed-refused loopback address first.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	refused := ln.Addr().String()
	_ = ln.Close()

	verified := make(chan error, 1)
	live, _, accepts := startScriptedBootstrap(t, secret, bootstrapScript{
		onReadyVerified: func(err error) { verified <- err },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := connectExecutionChannel(ctx, []string{refused, live}, secret)
	if err != nil {
		t.Fatalf("connectExecutionChannel: %v", err)
	}
	defer conn.Close()
	if err := <-verified; err != nil {
		t.Fatalf("bootstrap verification: %v", err)
	}
	if got := accepts(); got != 1 {
		t.Errorf("the refused candidate was accepted %d times; only the live one may serve", got)
	}
}

// TestExecutionAddressesRequireControlNetwork pins the fail-closed base: with
// no configured network (production always passes exactly the internal control
// network) there is no candidate at all, so an empty list can never resolve to
// a default-bridge or application-network address.
func TestExecutionAddressesRequireControlNetwork(t *testing.T) {
	settings := map[string]*network.EndpointSettings{
		"backend": {IPAddress: mustAddr(t, "172.18.0.2")},
	}
	if _, err := executionAddresses(nil, settings, executionPort); err == nil ||
		!strings.Contains(err.Error(), "control network") {
		t.Fatalf("error = %v, want the control-network-required failure", err)
	}
	if _, err := executionAddresses([]string{}, settings, executionPort); err == nil {
		t.Fatal("an empty network list must fail closed")
	}
}

// TestConnectExecutionContainerUsesOnlyControlNetwork pins the IPC boundary at
// the channel layer: connectExecutionContainer derives its candidates from the
// configured control network ONLY. A container that has a perfectly usable
// address on an application network but none on the control network fails
// closed naming the control network — the app-network address is never dialed
// as a fallback. A container ops value without a control identity is refused
// before any daemon call (no routes are registered, so any call would fail the
// test through the harness).
func TestConnectExecutionContainerUsesOnlyControlNetwork(t *testing.T) {
	const control = "relay-control-chan"

	t.Run("missing control identity refused with no daemon call", func(t *testing.T) {
		cli := newScriptedDockerClient(t)
		_, err := connectExecutionContainer(context.Background(), containerOps{},
			cli, testutil.DiscardLogger(), "fn", "img", "cid", "secret", RunMeta{})
		if err == nil || !strings.Contains(err.Error(), "internal control network is not configured") {
			t.Fatalf("error = %v, want the required-control-network refusal", err)
		}
	})

	// The inspect reports a running container whose ONLY usable endpoint is on
	// the application network. The start must fail closed naming the control
	// network, and the error must list only the control network as the address
	// set that was considered.
	t.Run("application network address is never the channel target", func(t *testing.T) {
		inspectBody := `{"Id":"cid","State":{"Running":true},"NetworkSettings":{"Networks":{` +
			`"backend":{"IPAddress":"172.18.0.9"},` +
			`"relay-control-chan":{}}}}`
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodGet, path: "/containers/cid/json", body: inspectBody,
		})
		_, err := connectExecutionContainer(context.Background(),
			containerOps{controlNetwork: control},
			cli, testutil.DiscardLogger(), "fn", "img", "cid", "secret", RunMeta{})
		if err == nil {
			t.Fatal("a container with no control-network address must fail closed")
		}
		if !strings.Contains(err.Error(), `execution control network "`+control+`"`) {
			t.Fatalf("error = %v, want it to name the control network", err)
		}
		if !strings.Contains(err.Error(), "["+control+"]") {
			t.Fatalf("error = %v, want the considered-address set to be the control network only", err)
		}
		if strings.Contains(err.Error(), "172.18.0.9") {
			t.Fatalf("error = %v, must not surface (i.e. have dialed) the app-network address", err)
		}
	})

	// Same failure with the control endpoint present but address-less while the
	// app network has an address: membership alone is not enough, and the app
	// network is still never used.
	t.Run("address-less control endpoint fails despite app address", func(t *testing.T) {
		inspectBody := `{"Id":"cid","State":{"Running":true},"NetworkSettings":{"Networks":{` +
			`"backend":{"IPAddress":"172.18.0.9"},` +
			`"relay-control-chan":{"IPAddress":""}}}}`
		cli := newScriptedDockerClient(t, dockerRoute{
			method: http.MethodGet, path: "/containers/cid/json", body: inspectBody,
		})
		_, err := connectExecutionContainer(context.Background(),
			containerOps{controlNetwork: control},
			cli, testutil.DiscardLogger(), "fn", "img", "cid", "secret", RunMeta{})
		if err == nil || !strings.Contains(err.Error(), "no usable address") {
			t.Fatalf("error = %v, want the no-usable-control-address failure", err)
		}
		if strings.Contains(err.Error(), "172.18.0.9") {
			t.Fatalf("error = %v, must not fall back to the app-network address", err)
		}
	})
}

func TestExecutionAddressesOrdersIPv4FirstAndDedupes(t *testing.T) {
	// NetworkSet sorts: neta before netb. The shared address on netb's entry
	// duplicates neta's v4 and must appear once.
	settings := map[string]*network.EndpointSettings{
		"neta": {
			IPAddress:         mustAddr(t, "172.18.0.2"),
			GlobalIPv6Address: mustAddr(t, "fd00::2"),
		},
		"netb": {IPAddress: mustAddr(t, "172.19.0.2")},
		"netc": {IPAddress: mustAddr(t, "172.18.0.2")}, // duplicates neta's v4
	}
	got, err := executionAddresses([]string{"netb", "neta", "netc"}, settings, executionPort)
	if err != nil {
		t.Fatalf("executionAddresses: %v", err)
	}
	want := []string{
		"172.18.0.2:7000", // neta v4 (first sorted name with v4)
		"172.19.0.2:7000", // netb v4
		"[fd00::2]:7000",  // then IPv6
	}
	if len(got) != len(want) {
		t.Fatalf("addresses = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("addresses[%d] = %q, want %q (IPv4-first order; unconfigured networks excluded)", i, got[i], want[i])
		}
	}
}

func TestExecutionAddressesRejectsUnusableAddresses(t *testing.T) {
	cases := map[string]map[string]*network.EndpointSettings{
		"missing endpoint": {
			"backend": {},
		},
		"loopback": {
			"backend": {IPAddress: mustAddr(t, "127.0.0.1")},
		},
		"unspecified": {
			"backend": {IPAddress: mustAddr(t, "0.0.0.0")},
		},
		"link-local": {
			"backend": {IPAddress: mustAddr(t, "169.254.1.2")},
		},
	}
	for name, settings := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := executionAddresses([]string{"backend"}, settings, executionPort)
			if err == nil || !strings.Contains(err.Error(), "no usable address") {
				t.Fatalf("error = %v, want the no-usable-address failure", err)
			}
		})
	}
}

func TestUsableExecutionAddress(t *testing.T) {
	cases := []struct {
		addr     string
		wantIPv4 bool
		want     bool
	}{
		{"172.18.0.2", true, true},
		{"fd00::2", false, true},
		{"fd00::2", true, false},
		{"172.18.0.2", false, false}, // 4-in-6 is not a native IPv6 candidate
		{"::ffff:172.18.0.2", false, false},
		{"127.0.0.1", true, false},
		{"0.0.0.0", true, false},
		{"169.254.1.2", true, false},
	}
	for _, tc := range cases {
		got := usableExecutionAddress(mustAddr(t, tc.addr), tc.wantIPv4)
		if got != tc.want {
			t.Errorf("usableExecutionAddress(%s, ipv4=%v) = %v, want %v",
				tc.addr, tc.wantIPv4, got, tc.want)
		}
	}
	if usableExecutionAddress(netip.Addr{}, true) {
		t.Error("an invalid address must not be usable")
	}
}

func TestExecutionBootstrapEnvInjectsRelayOwnedChannelVars(t *testing.T) {
	secret := strings.Repeat("a", 64)
	env := []string{
		"APP=1",
		"RELAY_EXECUTION_PORT=9999",
		"RELAY_EXECUTION_SECRET=app-supplied",
		"OTHER=x",
	}
	got := executionBootstrapEnv(env, secret)
	want := []string{
		"APP=1",
		"OTHER=x",
		"RELAY_EXECUTION_PORT=7000",
		"RELAY_EXECUTION_SECRET=" + secret,
	}
	if len(got) != len(want) {
		t.Fatalf("env = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("env[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	// The input slice is never mutated.
	if env[1] != "RELAY_EXECUTION_PORT=9999" || env[2] != "RELAY_EXECUTION_SECRET=app-supplied" {
		t.Errorf("input env was mutated: %v", env)
	}
}

func mustMarshalFrame(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return b
}

func mustAddr(t *testing.T, s string) netip.Addr {
	t.Helper()
	a, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatalf("parse addr %q: %v", s, err)
	}
	return a
}
