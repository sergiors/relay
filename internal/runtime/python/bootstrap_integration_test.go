//go:build integration

package python

// Direct-process tests for the Python bootstrap's authenticated, length-
// prefixed execution channel (see internal/runtime/python/bootstrap.py and
// internal/runtime/protocol.go). The real embedded bootstrap runs as a child
// process on a HOST-LOCAL reserved loopback port (RELAY_EXECUTION_PORT is
// test-overridable; production pins 7000 inside isolated containers), and the
// test speaks the wire contract as the Relay client: framed hello/auth/ready
// HMAC handshake, then sequential framed invoke/result exchanges. App output is
// the child's native stdout/stderr — never protocol data — and is observed
// directly on those pipes.
//
// Both bootstrap packages (python, node) may run concurrently under
// `go test ./...`, so each child gets its OWN reserved ephemeral port instead
// of assuming host :7000 is free.

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

// Frame caps from the wire contract (payload bytes, excluding the 4-byte
// header). Hardcoded to avoid an import cycle with the Go runtime package.
const (
	channelHandshakeCap = 4 << 10  // 4 KiB
	channelRequestCap   = 8 << 20  // 8 MiB
	channelResultCap    = 1 << 20  // 1 MiB
	channelErrorCap     = 64 << 10 // 64 KiB

	bootstrapAuthContext = "relay-bootstrap-auth-v1:"
	clientAuthContext    = "relay-client-auth-v1:"
)

// reserveLoopbackPort reserves an ephemeral port bound on ALL interfaces (the
// bootstrap listens on 0.0.0.0), then releases it for the child to bind. The
// spawn/connect path retries with a fresh port if another process wins the
// race, so parallel test packages never collide on a fixed port.
func reserveLoopbackPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatalf("release reserved port: %v", err)
	}
	return port
}

// testSecret returns a fresh 64-lowercase-hex channel credential.
func testSecret(t *testing.T) string {
	t.Helper()
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("generate secret: %v", err)
	}
	return hex.EncodeToString(b[:])
}

// channelWriteFrame writes one 4-byte big-endian length-prefixed frame.
func channelWriteFrame(w io.Writer, payload []byte) error {
	if len(payload) == 0 {
		return fmt.Errorf("empty frame")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// channelReadFrame reads one frame with the contract's zero/oversize/truncated
// checks against max payload bytes.
func channelReadFrame(r io.Reader, max int) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 {
		return nil, fmt.Errorf("empty frame")
	}
	if int64(size) > int64(max) {
		return nil, fmt.Errorf("frame of %d bytes exceeds the %d-byte limit", size, max)
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// channelChallengeMAC is the handshake MAC: HMAC-SHA256 keyed by the ASCII
// secret over the direction context plus the nonce.
func channelChallengeMAC(secret, contextPrefix, nonce string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(contextPrefix + nonce))
	return hex.EncodeToString(mac.Sum(nil))
}

// pyProc is a live bootstrap process driven over its authenticated channel:
// invoke frames are written to the connection, result frames are read from it,
// and stdout/stderr (native app output) are collected separately.
type pyProc struct {
	cmd      *exec.Cmd
	port     int
	secret   string
	conn     net.Conn
	seq      int
	exitDone chan struct{}
	waitErr  error

	stdoutMu sync.Mutex
	stdout   strings.Builder
	stderrMu sync.Mutex
	stderr   strings.Builder
}

func (p *pyProc) Write(b []byte) (int, error) {
	p.stderrMu.Lock()
	defer p.stderrMu.Unlock()
	_, _ = p.stderr.Write(b)
	return len(b), nil
}

func (p *pyProc) stderrString() string {
	p.stderrMu.Lock()
	defer p.stderrMu.Unlock()
	return p.stderr.String()
}

func (p *pyProc) stdoutLen() int {
	p.stdoutMu.Lock()
	defer p.stdoutMu.Unlock()
	return p.stdout.Len()
}

func (p *pyProc) stdoutSince(off int) string {
	p.stdoutMu.Lock()
	defer p.stdoutMu.Unlock()
	return p.stdout.String()[off:]
}

func (p *pyProc) stdoutString() string {
	return p.stdoutSince(0)
}

// stdoutWriter is assigned as cmd.Stdout so os/exec's own copy goroutine —
// joined by cmd.Wait — appends through stdoutMu, the same lock every stdout
// accessor takes, so collecting output never races test reads of the builder.
// The lock is taken per Write, not for the whole copy, keeping reads unblocked
// while output streams in.
type stdoutWriter struct{ p *pyProc }

func (w stdoutWriter) Write(b []byte) (int, error) {
	w.p.stdoutMu.Lock()
	defer w.p.stdoutMu.Unlock()
	_, _ = w.p.stdout.Write(b)
	return len(b), nil
}

// awaitStdoutQuiet waits until the child's stdout has stopped growing (stable
// across a 30ms window) so an invocation's prints are observable right after
// its result frame, bounded by 2s. The protocol arrives on the connection
// while app output arrives on the stdout pipe; the two schedulings are not
// ordered relative to each other.
func (p *pyProc) awaitStdoutQuiet() {
	deadline := time.Now().Add(2 * time.Second)
	last := -1
	stable := 0
	for time.Now().Before(deadline) {
		n := p.stdoutLen()
		if n == last {
			stable++
			if stable >= 2 {
				return
			}
		} else {
			stable = 0
			last = n
		}
		time.Sleep(15 * time.Millisecond)
	}
}

// closeChannel closes the authenticated connection (Relay being done).
func (p *pyProc) closeChannel() error {
	if p.conn == nil {
		return nil
	}
	return p.conn.Close()
}

// waitExit waits for the process to exit within bound and returns its error
// (nil for exit 0).
func (p *pyProc) waitExit(t *testing.T, bound time.Duration) error {
	t.Helper()
	select {
	case <-p.exitDone:
		return p.waitErr
	case <-time.After(bound):
		t.Fatalf("bootstrap did not exit within %v; stderr:\n%s", bound, p.stderrString())
		return nil
	}
}

// dialChannel dials the child's reserved loopback port, retrying while the
// process is alive (the listener binds slightly after startup) and failing as
// soon as the process dies (e.g. it lost the port race), within bound.
func (p *pyProc) dialChannel(t *testing.T, bound time.Duration) net.Conn {
	t.Helper()
	deadline := time.Now().Add(bound)
	addr := fmt.Sprintf("127.0.0.1:%d", p.port)
	for {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			return conn
		}
		select {
		case <-p.exitDone:
			t.Fatalf("bootstrap exited before accepting a connection (port %d): %v; stderr:\n%s",
				p.port, p.waitErr, p.stderrString())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("no connection to the bootstrap on %s within %v; stderr:\n%s",
				addr, bound, p.stderrString())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// clientHandshake performs the Relay side of the contract handshake on conn:
// hello with a fresh nonce, verify the bootstrap's auth MAC, answer its
// challenge with ready, then clear the handshake deadline.
func clientHandshake(t *testing.T, conn net.Conn, secret string) error {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set handshake deadline: %v", err)
	}
	var nonceBytes [32]byte
	if _, err := rand.Read(nonceBytes[:]); err != nil {
		t.Fatalf("hello nonce: %v", err)
	}
	relayNonce := hex.EncodeToString(nonceBytes[:])
	hello, err := json.Marshal(map[string]any{
		"type": "hello", "version": 1, "nonce": relayNonce,
	})
	if err != nil {
		t.Fatalf("marshal hello: %v", err)
	}
	if err := channelWriteFrame(conn, hello); err != nil {
		return fmt.Errorf("write hello: %w", err)
	}
	raw, err := channelReadFrame(conn, channelHandshakeCap)
	if err != nil {
		return fmt.Errorf("read auth: %w", err)
	}
	var auth struct {
		Type    string `json:"type"`
		Version int    `json:"version"`
		MAC     string `json:"mac"`
		Nonce   string `json:"nonce"`
	}
	if err := json.Unmarshal(raw, &auth); err != nil {
		return fmt.Errorf("malformed auth frame: %w", err)
	}
	if auth.Type != "auth" || auth.Version != 1 || len(auth.Nonce) != 64 {
		return fmt.Errorf("unexpected auth frame: %q", raw)
	}
	want := channelChallengeMAC(secret, bootstrapAuthContext, relayNonce)
	if !hmac.Equal([]byte(want), []byte(auth.MAC)) {
		return fmt.Errorf("auth MAC does not verify (got %q)", auth.MAC)
	}
	ready, err := json.Marshal(map[string]any{
		"type": "ready", "version": 1,
		"mac": channelChallengeMAC(secret, clientAuthContext, auth.Nonce),
	})
	if err != nil {
		t.Fatalf("marshal ready: %v", err)
	}
	if err := channelWriteFrame(conn, ready); err != nil {
		return fmt.Errorf("write ready: %w", err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Fatalf("clear handshake deadline: %v", err)
	}
	return nil
}

// startBootstrap spawns the embedded bootstrap and completes one authenticated
// channel. It retries with a fresh reserved port when the child loses the
// bind race to another process.
func startBootstrap(t *testing.T, dir string) *pyProc {
	t.Helper()
	return startEmbeddedBootstrap(t, dir, Bootstrap, nil)
}

// writeEmbeddedRuntimeModules materializes every embedded Python runtime module
// beside the bootstrap, mirroring the image's /relay directory: the bootstrap
// and the _relay_* sibling modules it imports, with the test PYTHONPATH playing
// the in-image import path. The lifecycle module's static metadata path is
// rewritten to dir, so a test stages one exactly when it wants INIT_START,
// like the image always carries.
func writeEmbeddedRuntimeModules(t *testing.T, dir string) {
	t.Helper()
	entries, err := runtimeModules.ReadDir(".")
	if err != nil {
		t.Fatalf("read embedded runtime modules: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".py") ||
			name == "bootstrap.py" || name == healthcheckFile {
			continue
		}
		content, err := runtimeModules.ReadFile(name)
		if err != nil {
			t.Fatalf("read embedded module %s: %v", name, err)
		}
		source := strings.Replace(
			string(content),
			`RUNTIME_METADATA_PATH = "/relay/runtime-metadata.json"`,
			fmt.Sprintf("RUNTIME_METADATA_PATH = %q", filepath.Join(dir, "runtime-metadata.json")),
			1,
		)
		if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0o644); err != nil {
			t.Fatalf("write module %s: %v", name, err)
		}
	}
}

// startEmbeddedBootstrap is startBootstrap for explicit content and extra
// child environment (used by the zuvloop variant tests). The embedded _relay_*
// sibling modules are materialized beside bootstrap.py first, exactly as the
// image deploys them.
func startEmbeddedBootstrap(t *testing.T, dir string, content []byte, extraEnv []string) *pyProc {
	t.Helper()
	writeEmbeddedRuntimeModules(t, dir)
	if err := os.WriteFile(filepath.Join(dir, "bootstrap.py"), content, 0o644); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		p := spawnProcessEnv(t, dir, extraEnv)
		conn := p.dialChannel(t, 10*time.Second)
		if err := clientHandshake(t, conn, p.secret); err != nil {
			_ = conn.Close()
			select {
			case <-p.exitDone:
				lastErr = fmt.Errorf("attempt %d: process exited during handshake: %v (%v)",
					attempt+1, p.waitErr, err)
				continue // lost the port race; try a fresh port
			default:
				t.Fatalf("handshake: %v; stderr:\n%s", err, p.stderrString())
			}
		}
		p.conn = conn
		return p
	}
	t.Fatalf("could not start the bootstrap channel after retries: %v", lastErr)
	return nil
}

// spawnProcessEnv starts the already-written bootstrap.py in dir on a fresh
// reserved port with the Relay-owned channel environment, the test's controlled
// baseline/system env, and any caller extras (e.g. the zuvloop variant's
// environment). The process exit is tracked exactly once; cmd.Wait joins
// os/exec's output-copy goroutines, so a closed exitDone implies both the
// stdout and stderr collectors have drained. No connection is made yet.
func spawnProcessEnv(t *testing.T, dir string, extraEnv []string) *pyProc {
	t.Helper()
	port := reserveLoopbackPort(t)
	secret := testSecret(t)
	cmd := exec.Command("python3", filepath.Join(dir, "bootstrap.py"))
	cmd.Env = append(append(os.Environ(),
		"PYTHONPATH="+dir,
		"RELAY_EXECUTION_PORT="+strconv.Itoa(port),
		"RELAY_EXECUTION_SECRET="+secret,
		"RELAY_TEST_BASELINE=process-baseline",
		"RELAY_TEST_SYSTEM=system-value",
	), extraEnv...)
	p := &pyProc{
		cmd:      cmd,
		port:     port,
		secret:   secret,
		exitDone: make(chan struct{}),
	}
	// The collectors are assigned as cmd.Stdout/cmd.Stderr rather than served
	// through StdoutPipe/StderrPipe plus detached copies: os/exec then owns the
	// copy goroutines and Wait joins them before returning. With raw pipes,
	// Wait could close the read end while a detached copy still drained it,
	// racing (and truncating) any stdout snapshot taken after exit.
	cmd.Stdout = stdoutWriter{p}
	cmd.Stderr = p
	if err := cmd.Start(); err != nil {
		t.Fatalf("start bootstrap: %v", err)
	}
	go func() {
		p.waitErr = cmd.Wait()
		close(p.exitDone)
	}()
	t.Cleanup(func() {
		_ = p.closeChannel()
		select {
		case <-p.exitDone:
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-p.exitDone
		}
	})
	return p
}

// invokeResult is one result frame plus the native stdout produced while the
// invocation ran.
type invokeResult struct {
	OK    bool
	Err   string
	Out   string
	Frame string
}

// writeOtelStub writes a minimal stand-in for the OpenTelemetry API the runtime
// image installs, into dir (the test PYTHONPATH). The real embedded bootstrap
// imports `opentelemetry.context` and `opentelemetry.propagate`; this stub
// provides just those with the same attach/detach/extract contract backed by
// contextvars, so the process-behavior tests can observe the managed invocation
// context without a network OTel install. Mirrors the Node bootstrap test's stub.
func writeOtelStub(t *testing.T, dir string) {
	t.Helper()
	pkg := filepath.Join(dir, "opentelemetry")
	if err := os.MkdirAll(pkg, 0o755); err != nil {
		t.Fatalf("mkdir opentelemetry stub: %v", err)
	}
	write := func(name, contents string) {
		if err := os.WriteFile(filepath.Join(pkg, name), []byte(contents), 0o644); err != nil {
			t.Fatalf("write opentelemetry stub: %v", err)
		}
	}
	write("__init__.py", "")
	write("context.py", `
import contextvars

_ROOT = "root"
_current = contextvars.ContextVar("relay_otel_context", default=_ROOT)


def attach(ctx):
    return _current.set(ctx)


def detach(token):
    _current.reset(token)


def get_current():
    return _current.get()
`)
	write("propagate.py", `
def extract(carrier, context="root", getter=None):
    if not carrier:
        return context
    return carrier.get("traceparent") or context
`)
}

// startPython runs the real embedded bootstrap under python3 with PYTHONPATH
// pointing at dir (which holds the handler modules and the OTel API stub).
func startPython(t *testing.T, dir string) *pyProc {
	t.Helper()
	writeOtelStub(t, dir)
	return startBootstrap(t, dir)
}

// startPythonWithoutOtel runs the bootstrap in a dir whose `opentelemetry`
// package deliberately raises on import, so the bootstrap's guarded import is
// exercised deterministically regardless of any ambient OTel install on the
// test host.
func startPythonWithoutOtel(t *testing.T, dir string) *pyProc {
	t.Helper()
	pkg := filepath.Join(dir, "opentelemetry")
	if err := os.MkdirAll(pkg, 0o755); err != nil {
		t.Fatalf("mkdir opentelemetry: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pkg, "__init__.py"), []byte(`raise ImportError("otel unavailable")`), 0o644); err != nil {
		t.Fatalf("write broken opentelemetry: %v", err)
	}
	return startBootstrap(t, dir)
}

// startUnauthenticatedPython spawns the embedded bootstrap WITHOUT opening a
// channel: the listener is idle, so the test can drive the raw handshake itself
// and prove the listener-stays contract — a rejected peer is followed by a
// valid one on the SAME still-running process (once a session is
// authenticated, admission closes every further connection before its
// handshake).
func startUnauthenticatedPython(t *testing.T, dir string) *pyProc {
	t.Helper()
	writeOtelStub(t, dir)
	writeEmbeddedRuntimeModules(t, dir)
	if err := os.WriteFile(filepath.Join(dir, "bootstrap.py"), Bootstrap, 0o644); err != nil {
		t.Fatalf("write bootstrap: %v", err)
	}
	return spawnProcessEnv(t, dir, nil)
}

type reqFrame struct {
	Type    string            `json:"type"`
	ID      string            `json:"id"`
	Handler string            `json:"handler"`
	Event   json.RawMessage   `json:"event"`
	Env     map[string]string `json:"env"`
	Trace   map[string]string `json:"trace,omitempty"`
	Context map[string]any    `json:"context,omitempty"`
}

type respFrame struct {
	Type  string `json:"type"`
	ID    string `json:"id"`
	OK    bool   `json:"ok"`
	Error string `json:"error"`
}

// defaultHandlerContext returns a complete per-request context frame in the
// protocol's camelCase shape, with a live future deadline and a handler name
// MATCHING the invocation (the bootstrap rejects a mismatch as a protocol
// violation), so an invocation test that does not care about the context still
// runs the production handler(event, context) path.
func defaultHandlerContext(handler string) map[string]any {
	return map[string]any{
		"requestId":   "test-request",
		"attempt":     1,
		"maxAttempts": 3,
		"deadline":    time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
		"app":         "testapp",
		"handler":     handler,
	}
}

// invoke writes one framed invoke request on the authenticated channel, reads
// the result frame, and collects the native stdout produced meanwhile. A dead
// channel surfaces as invokeResult without OK (Closed set) instead of failing
// the test; every other call validates the frame contract (type, id echo).
func (p *pyProc) invoke(t *testing.T, handler string, event string, env map[string]string) invokeResult {
	return p.invokeTrace(t, handler, event, env, nil)
}

func (p *pyProc) invokeTrace(t *testing.T, handler string, event string, env, trace map[string]string) invokeResult {
	return p.invokeFrame(t, handler, event, env, trace, defaultHandlerContext(handler))
}

func (p *pyProc) invokeFrame(t *testing.T, handler string, event string, env, trace map[string]string, ctxFrame map[string]any) invokeResult {
	t.Helper()
	if p.conn == nil {
		t.Fatalf("invoke on a process without a channel")
	}
	id := fmt.Sprintf("t%d", p.seq)
	p.seq++
	frame, err := json.Marshal(reqFrame{
		Type: "invoke", ID: id, Handler: handler,
		Event: json.RawMessage(event), Env: env, Trace: trace, Context: ctxFrame,
	})
	if err != nil {
		t.Fatalf("build request frame: %v", err)
	}
	off := p.stdoutLen()
	if err := p.conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set invoke deadline: %v", err)
	}
	if err := channelWriteFrame(p.conn, frame); err != nil {
		return invokeResult{Err: "write invoke frame: " + err.Error()}
	}
	raw, err := channelReadFrame(p.conn, channelResultCap)
	if err != nil {
		return invokeResult{Err: "read result frame: " + err.Error()}
	}
	if err := p.conn.SetDeadline(time.Time{}); err != nil {
		t.Fatalf("clear invoke deadline: %v", err)
	}
	p.awaitStdoutQuiet()
	res := invokeResult{Frame: string(raw), Out: p.stdoutSince(off)}
	var resp respFrame
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("result frame is not parseable JSON: %q", raw)
	}
	if resp.Type != "result" {
		t.Errorf("result type = %q, want %q", resp.Type, "result")
	}
	if resp.ID != id {
		t.Errorf("result id = %q, want the echoed request id %q", resp.ID, id)
	}
	res.OK = resp.OK
	res.Err = resp.Error
	return res
}

// expectChannelClosed asserts the channel reads EOF (or a reset) within bound —
// the bootstrap closed the authenticated connection.
func (p *pyProc) expectChannelClosed(t *testing.T, bound time.Duration) {
	t.Helper()
	if p.conn == nil {
		t.Fatal("no channel to observe closed")
	}
	if err := p.conn.SetReadDeadline(time.Now().Add(bound)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var buf [4]byte
	if _, err := io.ReadFull(p.conn, buf[:]); err == nil {
		t.Fatal("expected the channel to be closed, but a frame arrived")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatalf("channel still open after %v (no close observed)", bound)
	}
	// EOF / reset / closed pipe: the bootstrap closed its side.
}

// expectFrameClosed asserts the next read fails within bound with ANY error
// (used right after writing a frame that must get the connection torn down).
func expectFrameReadFails(t *testing.T, conn net.Conn, bound time.Duration) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(bound)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := channelReadFrame(conn, channelResultCap); err == nil {
		t.Fatal("expected a read failure (closed channel), got a frame")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatalf("channel stayed open after %v (no close observed)", bound)
	}
}

// reconnect opens a fresh connection to the same still-running process and
// completes the handshake — the proof that a rejected peer did not kill the
// listener.
func (p *pyProc) reconnect(t *testing.T) net.Conn {
	t.Helper()
	conn := p.dialChannel(t, 5*time.Second)
	if err := clientHandshake(t, conn, p.secret); err != nil {
		t.Fatalf("reconnect handshake: %v; stderr:\n%s", err, p.stderrString())
	}
	return conn
}

// writeHandler writes a handler module into the test PYTHONPATH dir.
func writeHandler(t *testing.T, dir, module, src string) {
	t.Helper()
	path := filepath.Join(dir, filepath.FromSlash(strings.ReplaceAll(module, ".", "/"))+".py")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatalf("write handler: %v", err)
	}
}

func requirePython(t *testing.T) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatalf("Python 3.9 or newer is required for these integration tests: %v", err)
	}
	out, err := exec.Command(python, "-c", "import sys; print('%d.%d' % sys.version_info[:2])").Output()
	if err != nil {
		t.Fatalf("check python3 version: %v", err)
	}
	version := strings.TrimSpace(string(out))
	majorText, minorText, ok := strings.Cut(version, ".")
	major, majorErr := strconv.Atoi(majorText)
	minor, minorErr := strconv.Atoi(minorText)
	if !ok || majorErr != nil || minorErr != nil || major < 3 || (major == 3 && minor < 9) {
		t.Fatalf("Python 3.9 or newer is required for these integration tests; found %q", version)
	}
}

func TestPythonRuntimeTwoInvocationsAndModuleState(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
_counter = {"n": 0}

def count(event, context):
    _counter["n"] += 1
    print("count=%d" % _counter["n"])

def completed(event, context):
    print("completed %s" % event["msg"])
`)
	p := startPython(t, dir)

	r1 := p.invoke(t, "handler.count", `{}`, nil)
	r2 := p.invoke(t, "handler.count", `{}`, nil)
	r3 := p.invoke(t, "handler.completed", `{"msg":"hi"}`, nil)
	for i, r := range []invokeResult{r1, r2, r3} {
		if !r.OK {
			t.Fatalf("invoke %d failed: %s", i+1, r.Err)
		}
	}
	// The invocation window carries the START/END lifecycle lines beside the
	// app prints; match the count prints wherever they land in the window.
	if !strings.Contains(r1.Out, "count=1") {
		t.Errorf("first count out = %q, want count=1", r1.Out)
	}
	if !strings.Contains(r2.Out, "count=2") {
		t.Errorf("second count out = %q, want count=2 (module state persisted, interpreter stayed alive)", r2.Out)
	}
	if !strings.Contains(r3.Out, "completed hi") {
		t.Errorf("third invoke (different handler) out = %q, want it to contain 'completed hi'", r3.Out)
	}
}

// TestPythonRuntimeAsyncHandlerKeepsOneProcessLifetimeLoop proves the warm
// container's async semantics: one event loop lives for the whole process, a
// sync handler and an async handler run on that SAME loop, and a module-level
// asyncio resource (a Queue) created on first use stays usable across
// invocations instead of being orphaned by a per-invocation loop. It also
// proves an async failure fails only that invocation (a later await on the same
// loop succeeds) and that the sync/async transitions and the import/lookup
// distinctions are unchanged.
func TestPythonRuntimeAsyncHandlerKeepsOneProcessLifetimeLoop(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import asyncio

_loop = None
_queue = None

def _remember():
    # The loop this handler runs on must be the SAME live loop every
    # invocation; the module-level Queue binds to it on first use.
    global _loop, _queue
    current = asyncio.get_running_loop()
    if _loop is None:
        _loop = current
        _queue = asyncio.Queue()
    assert current is _loop, "event loop changed between invocations"
    assert not _loop.is_closed(), "event loop was closed between invocations"
    return current

def sync_count(event, context):
    _remember()
    print("sync=on-persistent-loop")

async def async_touch(event, context):
    _remember()
    await _queue.put(event["msg"])
    print("async=%s" % (await _queue.get()))

async def async_boom(event, context):
    _remember()
    await asyncio.sleep(0)
    raise RuntimeError("async-kaboom")
`)
	p := startPython(t, dir)

	// sync -> async -> async -> async failure -> async recovery -> sync: every
	// step must see the same process-lifetime loop, and the module Queue must
	// survive between invocations.
	r := p.invoke(t, "handler.sync_count", `{}`, nil)
	if !r.OK || !strings.Contains(r.Out, "sync=on-persistent-loop") {
		t.Fatalf("sync invoke = %+v, want success on the persistent loop", r)
	}
	for _, msg := range []string{"a", "b"} {
		r := p.invoke(t, "handler.async_touch", fmt.Sprintf(`{"msg":%q}`, msg), nil)
		if !r.OK || !strings.Contains(r.Out, "async="+msg) {
			t.Fatalf("async invoke %q = %+v, want success on the same reused loop", msg, r)
		}
	}
	fail := p.invoke(t, "handler.async_boom", `{}`, nil)
	if fail.OK || !strings.Contains(fail.Err, "async-kaboom") {
		t.Fatalf("async failure = %+v, want ok:false with the handler error", fail)
	}
	recover := p.invoke(t, "handler.async_touch", `{"msg":"c"}`, nil)
	if !recover.OK || !strings.Contains(recover.Out, "async=c") {
		t.Fatalf("post-failure async invoke = %+v, want recovery on the same live loop", recover)
	}
	again := p.invoke(t, "handler.sync_count", `{}`, nil)
	if !again.OK || !strings.Contains(again.Out, "sync=on-persistent-loop") {
		t.Fatalf("post-async sync invoke = %+v, want success", again)
	}

	// Import/lookup distinctions are unchanged: a missing function and an
	// unimportable module both fail only the invocation.
	missing := p.invoke(t, "handler.nope", `{}`, nil)
	if missing.OK || !strings.Contains(missing.Err, "has no function") {
		t.Fatalf("missing function = %+v, want a lookup failure", missing)
	}
	badimport := p.invoke(t, "no_such_module.nope", `{}`, nil)
	if badimport.OK || !strings.Contains(badimport.Err, "failed to import module") {
		t.Fatalf("bad import = %+v, want an import failure", badimport)
	}
}

// TestPythonRuntimeAwaitableReturningHandler proves a sync handler that returns
// an awaitable is awaited on the persistent loop: both a plain sync def
// returning a coroutine and the common decorator shape (a sync wrapper around
// an async body) complete, and the awaited body's output is still emitted before
// the invocation's result.
func TestPythonRuntimeAwaitableReturningHandler(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import asyncio

def returns_coroutine(event, context):
    async def inner():
        await asyncio.sleep(0)
        print("awaited %s" % event["msg"])
    return inner()

class Awaitable:
    def __init__(self, msg):
        self._msg = msg
    def __await__(self):
        async def run():
            await asyncio.sleep(0)
            print("object %s" % self._msg)
        return run().__await__()

def returns_awaitable_object(event, context):
    return Awaitable(event["msg"])

def decorate(fn):
    def wrapper(event, context):
        async def run():
            await asyncio.sleep(0)
            fn(event, context)
        return run()
    return wrapper

@decorate
def decorated(event, context):
    print("decorated %s" % event["msg"])
`)
	p := startPython(t, dir)

	for _, tc := range []struct {
		handler string
		want    string
	}{
		{"handler.returns_coroutine", "awaited hi"},
		{"handler.returns_awaitable_object", "object hi"},
		{"handler.decorated", "decorated hi"},
	} {
		r := p.invoke(t, tc.handler, `{"msg":"hi"}`, nil)
		if !r.OK {
			t.Fatalf("%s failed: %s", tc.handler, r.Err)
		}
		if !strings.Contains(r.Out, tc.want) {
			t.Fatalf("%s out = %q, want %q (the returned awaitable must be awaited)", tc.handler, r.Out, tc.want)
		}
	}
}

func TestPythonRuntimeAsyncHandler(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import asyncio

async def async_handler(event, context):
    await asyncio.sleep(0)
    print("async %s" % event["msg"])
`)
	p := startPython(t, dir)
	r := p.invoke(t, "handler.async_handler", `{"msg":"hi"}`, nil)
	if !r.OK {
		t.Fatalf("expected success for async handler, error: %s", r.Err)
	}
	if !strings.Contains(r.Out, "async hi") {
		t.Errorf("out = %q, want to contain 'async hi'", r.Out)
	}
}

// TestPythonRuntimeTraceContextIsolatedAcrossInvocations proves the managed
// invocation context: the frame's "trace" carrier is ATTACHED for the handler
// call only and DETACHED afterwards — an async handler observes it across an
// await, a raising handler still restores the previous context, and an
// untraced invocation sees no leaked context. The stub OTel API mirrors the
// real attach/detach contract.
func TestPythonRuntimeTraceContextIsolatedAcrossInvocations(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import asyncio
from opentelemetry import context as otel_context

async def inspect(event, context):
    await asyncio.sleep(0)
    print("trace=" + str(otel_context.get_current()))
    if event.get("fail"):
        raise ValueError("trace failure")
`)
	p := startPython(t, dir)

	r1 := p.invokeTrace(t, "handler.inspect", `{"fail":false}`, nil, map[string]string{
		"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
	})
	if !r1.OK || !strings.Contains(r1.Out, "trace=00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01") {
		t.Fatalf("traced invoke = %+v, want the attached traceparent", r1)
	}

	r2 := p.invokeTrace(t, "handler.inspect", `{"fail":true}`, nil, map[string]string{
		"traceparent": "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01",
	})
	if r2.OK || !strings.Contains(r2.Out, "trace=00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01") {
		t.Fatalf("failed traced invoke = %+v, want the attached traceparent before the raise", r2)
	}

	r3 := p.invoke(t, "handler.inspect", `{}`, nil)
	if !r3.OK || !strings.Contains(r3.Out, "trace=root") {
		t.Fatalf("post-failure untraced invoke = %+v, want the restored (detached) root context", r3)
	}
}

// TestPythonRuntimeTraceWithoutOtelIsBestEffort proves the guarded import: when
// the OpenTelemetry API is unavailable the bootstrap still runs the handler with
// the trace field present, degrading to no propagation instead of failing.
func TestPythonRuntimeTraceWithoutOtelIsBestEffort(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", "def f(event, context):\n    print('ran %s' % event['msg'])\n")
	p := startPythonWithoutOtel(t, dir)

	r := p.invokeTrace(t, "handler.f", `{"msg":"hi"}`, nil, map[string]string{
		"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
	})
	if !r.OK || !strings.Contains(r.Out, "ran hi") {
		t.Fatalf("invoke without the OTel API = %+v, want it to run best-effort", r)
	}
}

func TestPythonRuntimeHandlerErrorKeepsProcessAlive(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
_counter = {"n": 0}

def count(event, context):
    _counter["n"] += 1
    print("count=%d" % _counter["n"])

def boom(event, context):
    raise ValueError("kaboom")
`)
	p := startPython(t, dir)

	r := p.invoke(t, "handler.boom", `{}`, nil)
	if r.OK {
		t.Fatal("expected the raising handler to fail the invocation")
	}
	if !strings.Contains(r.Err, "kaboom") {
		t.Errorf("error = %q, want it to contain 'kaboom'", r.Err)
	}
	// The stderr mirror is asynchronous in the test harness; poll briefly.
	stderrOK := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(p.stderrString(), "failed") {
			stderrOK = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !stderrOK {
		t.Errorf("stderr = %q, want the operator-visible failure log", p.stderrString())
	}
	// The process must stay healthy: a THIRD invocation succeeds and the
	// module state survived.
	r2 := p.invoke(t, "handler.count", `{}`, nil)
	if !r2.OK || !strings.Contains(r2.Out, "count=1") {
		t.Fatalf("post-error invoke = %+v, want count=1 on a healthy container", r2)
	}
}

func TestPythonRuntimeEnvRotation(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import os

def showenv(event, context):
    print("VAR=" + os.environ.get("VAR", ""))
`)
	p := startPython(t, dir)

	r1 := p.invoke(t, "handler.showenv", `{}`, map[string]string{"VAR": "one", "SECRET": "aaa"})
	if !r1.OK || !strings.Contains(r1.Out, "VAR=one") {
		t.Fatalf("first env invoke: %+v, want VAR=one", r1)
	}
	r2 := p.invoke(t, "handler.showenv", `{}`, map[string]string{"VAR": "two"})
	if !r2.OK || !strings.Contains(r2.Out, "VAR=two") {
		t.Fatalf("second env invoke: %+v, want VAR=two (rotated)", r2)
	}
}

// TestPythonRuntimeEnvAppliedExactlyAcrossInvocations proves the bootstrap
// applies each request frame's env EXACTLY on the reused process: a key present
// in invocation 1 and absent in invocation 2 is removed (or restored to its
// pre-Relay baseline), unrelated OS/system/container variables are preserved,
// and RELAY_HANDLER is overwritten every invocation.
func TestPythonRuntimeEnvAppliedExactlyAcrossInvocations(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import os

def show(event, context):
    print("FOO=" + os.environ.get("FOO", "<unset>"))
    print("BAR=" + os.environ.get("BAR", "<unset>"))
    print("BASELINE=" + os.environ.get("RELAY_TEST_BASELINE", "<unset>"))
    print("SYSTEM=" + os.environ.get("RELAY_TEST_SYSTEM", "<unset>"))
    print("HANDLER=" + os.environ.get("RELAY_HANDLER", "<unset>"))
`)
	p := startPython(t, dir)

	// Invocation 1 carries FOO and BAR plus an override of an existing baseline
	// variable.
	r1 := p.invoke(t, "handler.show", `{}`, map[string]string{
		"FOO": "one", "BAR": "bar1", "RELAY_TEST_BASELINE": "overridden",
	})
	if !r1.OK {
		t.Fatalf("invoke 1 failed: %s", r1.Err)
	}
	for _, want := range []string{"FOO=one", "BAR=bar1", "BASELINE=overridden", "SYSTEM=system-value", "HANDLER=handler.show"} {
		if !strings.Contains(r1.Out, want) {
			t.Fatalf("invoke 1 out = %q, want %q", r1.Out, want)
		}
	}

	// Invocation 2 carries ONLY FOO: BAR must be gone, and the baseline override
	// must be restored to the process's original value. System env is intact.
	r2 := p.invoke(t, "handler.show", `{}`, map[string]string{"FOO": "two"})
	if !r2.OK {
		t.Fatalf("invoke 2 failed: %s", r2.Err)
	}
	for _, want := range []string{"FOO=two", "BAR=<unset>", "BASELINE=process-baseline", "SYSTEM=system-value"} {
		if !strings.Contains(r2.Out, want) {
			t.Fatalf("invoke 2 out = %q, want %q (removed key must not leak forward)", r2.Out, want)
		}
	}

	// Invocation 3 carries NO env: FOO must be removed too.
	r3 := p.invoke(t, "handler.show", `{}`, nil)
	if !r3.OK {
		t.Fatalf("invoke 3 failed: %s", r3.Err)
	}
	for _, want := range []string{"FOO=<unset>", "BAR=<unset>", "BASELINE=process-baseline", "SYSTEM=system-value"} {
		if !strings.Contains(r3.Out, want) {
			t.Fatalf("invoke 3 out = %q, want %q", r3.Out, want)
		}
	}
}

// TestPythonRuntimeEnvCleanupAfterHandlerFailure proves the top-of-request
// cleanup is robust even when the previous handler failed: a key applied for a
// failing invocation is still removed on the next request.
func TestPythonRuntimeEnvCleanupAfterHandlerFailure(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import os

def boom(event, context):
    raise ValueError("kaboom")

def show(event, context):
    print("TOKEN=" + os.environ.get("TOKEN", "<unset>"))
`)
	p := startPython(t, dir)

	r1 := p.invoke(t, "handler.boom", `{}`, map[string]string{"TOKEN": "secret-v1"})
	if r1.OK {
		t.Fatal("expected the raising handler to fail")
	}
	r2 := p.invoke(t, "handler.show", `{}`, nil)
	if !r2.OK {
		t.Fatalf("post-failure invoke failed: %s", r2.Err)
	}
	if !strings.Contains(r2.Out, "TOKEN=<unset>") {
		t.Fatalf("out = %q, want TOKEN=<unset> (cleanup must run even after a failed handler)", r2.Out)
	}
}

// TestPythonRuntimeNoEnvValueLeaks proves the bootstrap never writes an env
// value to a response error or operator-visible stderr: a resolved secret value
// is visible only to the handler through os.environ.
func TestPythonRuntimeNoEnvValueLeaks(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import os

def leak(event, context):
    print("value=" + os.environ.get("SUPER_SECRET", ""))
    raise ValueError("handler failed")
`)
	p := startPython(t, dir)
	const secret = "CANARY-SECRET-VALUE-abc123"
	r := p.invoke(t, "handler.leak", `{}`, map[string]string{"SUPER_SECRET": secret})
	if r.OK {
		t.Fatal("expected the raising handler to fail")
	}
	if strings.Contains(r.Err, secret) {
		t.Fatalf("response error leaked the secret value: %q", r.Err)
	}
	if !strings.Contains(r.Out, "value="+secret) {
		t.Fatalf("handler must observe its own env value; out = %q", r.Out)
	}
	// stderr carries the handler failure text only, never the env value, and
	// the channel secret never appears anywhere.
	if strings.Contains(p.stderrString(), secret) {
		t.Fatalf("stderr leaked the secret value:\n%s", p.stderrString())
	}
	if strings.Contains(p.stderrString(), p.secret) || strings.Contains(p.stdoutString(), p.secret) {
		t.Fatal("the channel secret must never appear on stdout/stderr")
	}
}

// TestPythonRuntimeChannelCloseExitsZero pins the lifecycle: after Relay is
// done with its authenticated connection the container exits 0 cleanly.
func TestPythonRuntimeChannelCloseExitsZero(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", "def f(event, context):\n    pass\n")
	p := startPython(t, dir)
	r := p.invoke(t, "handler.f", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if err := p.closeChannel(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
	if err := p.waitExit(t, 10*time.Second); err != nil {
		t.Fatalf("bootstrap did not exit 0 on channel close: %v; stderr:\n%s", err, p.stderrString())
	}
}

// TestPythonRuntimeHelloMalformedFrameRejectedAndListenerStays pins the
// unauthenticated rejection half of the contract: garbage as the first frame
// gets the peer closed WITHOUT running a handler, and the listener stays
// available — a subsequent valid connection completes the handshake and
// invokes normally.
func TestPythonRuntimeHelloMalformedFrameRejectedAndListenerStays(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "RAN")
	writeHandler(t, dir, "handler", fmt.Sprintf(`
def mark(event, context):
    with open(%q, "w") as fh:
        fh.write("ran")
`, marker))
	p := startUnauthenticatedPython(t, dir)

	conn := p.dialChannel(t, 5*time.Second)
	if err := channelWriteFrame(conn, []byte("definitely not a response")); err != nil {
		t.Fatalf("write malformed hello: %v", err)
	}
	expectFrameReadFails(t, conn, 5*time.Second)
	_ = conn.Close()
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("a malformed hello must be rejected before any handler runs")
	}

	// The listener accepts again: a valid peer authenticates and invokes.
	conn2 := p.reconnect(t)
	defer conn2.Close()
	p.conn = conn2
	r := p.invoke(t, "handler.mark", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke after a rejected peer failed: %s", r.Err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("handler did not run on the valid connection: %v", err)
	}
}

// TestPythonRuntimeHelloVersionBooleanRejected pins strict version typing: the
// JSON boolean true compares equal to 1 in Python but is NOT the integer
// protocol version, so a hello carrying it is rejected before authentication.
func TestPythonRuntimeHelloVersionBooleanRejected(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", "def f(event, context):\n    pass\n")
	p := startUnauthenticatedPython(t, dir)

	conn := p.dialChannel(t, 5*time.Second)
	nonce := strings.Repeat("a", 64)
	raw := fmt.Sprintf(`{"type":"hello","version":true,"nonce":%q}`, nonce)
	if err := channelWriteFrame(conn, []byte(raw)); err != nil {
		t.Fatalf("write boolean-version hello: %v", err)
	}
	// Rejected before auth: the connection closes with no auth frame.
	expectFrameReadFails(t, conn, 5*time.Second)
	_ = conn.Close()

	// Process stays healthy; an integer-version hello authenticates.
	conn2 := p.reconnect(t)
	defer conn2.Close()
	p.conn = conn2
	r := p.invoke(t, "handler.f", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke after the rejected hello failed: %s", r.Err)
	}
}

// TestPythonRuntimeReadyVersionBooleanRejected pins the same strict typing on
// Relay's ready frame: version:true fails authentication, the peer is closed,
// and the listener survives for the correct ready.
func TestPythonRuntimeReadyVersionBooleanRejected(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", "def f(event, context):\n    pass\n")
	p := startUnauthenticatedPython(t, dir)

	conn := p.dialChannel(t, 5*time.Second)
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	nonce := strings.Repeat("b", 64)
	hello, _ := json.Marshal(map[string]any{"type": "hello", "version": 1, "nonce": nonce})
	if err := channelWriteFrame(conn, hello); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	if _, err := channelReadFrame(conn, channelHandshakeCap); err != nil {
		t.Fatalf("read auth: %v", err)
	}
	ready := fmt.Sprintf(`{"type":"ready","version":true,"mac":%q}`, strings.Repeat("c", 64))
	if err := channelWriteFrame(conn, []byte(ready)); err != nil {
		t.Fatalf("write boolean-version ready: %v", err)
	}
	expectFrameReadFails(t, conn, 5*time.Second)
	_ = conn.Close()

	conn2 := p.reconnect(t)
	defer conn2.Close()
	p.conn = conn2
	r := p.invoke(t, "handler.f", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke after the rejected ready failed: %s", r.Err)
	}
}

// TestPythonRuntimeWrongSecretRejectedNoInvocation pins the credential check:
// a peer that answers the challenge under the WRONG secret is closed before any
// handler runs, and the listener stays available for Relay's correct secret.
func TestPythonRuntimeWrongSecretRejectedNoInvocation(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "RAN")
	writeHandler(t, dir, "handler", fmt.Sprintf(`
def mark(event, context):
    with open(%q, "w") as fh:
        fh.write("ran")
`, marker))
	p := startUnauthenticatedPython(t, dir)

	conn := p.dialChannel(t, 5*time.Second)
	wrong := testSecret(t)
	if err := clientHandshake(t, conn, wrong); err == nil {
		t.Fatal("a handshake under the wrong secret must fail")
	}
	expectFrameReadFails(t, conn, 5*time.Second)
	_ = conn.Close()
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the unauthenticated peer must never reach a handler")
	}

	conn2 := p.reconnect(t)
	defer conn2.Close()
	p.conn = conn2
	r := p.invoke(t, "handler.mark", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke with the correct secret failed: %s", r.Err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("correct peer's handler did not run: %v", err)
	}
}

// TestPythonRuntimeSecondConnectionRejectedWhileActive pins the single-session
// admission: while Relay's authenticated session is active a second connection
// is closed immediately — no handshake completes on it, no handler can be
// invoked through it, its resources are released — and the active session keeps
// working untouched.
func TestPythonRuntimeSecondConnectionRejectedWhileActive(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
_counter = {"n": 0}

def count(event, context):
    _counter["n"] += 1
    print("count=%d" % _counter["n"])
`)
	p := startPython(t, dir)

	// The authenticated session serves one invocation.
	r1 := p.invoke(t, "handler.count", `{}`, nil)
	if !r1.OK || !strings.Contains(r1.Out, "count=1") {
		t.Fatalf("first invoke = %+v, want count=1", r1)
	}

	// A second connection is closed immediately, before any handshake frame:
	// the admission check rejects it without ever reaching a handler.
	intruder := p.dialChannel(t, 5*time.Second)
	expectFrameReadFails(t, intruder, 5*time.Second)
	_ = intruder.Close()

	// The active session is untouched: the counter proves NO handler ran for
	// the intruder and the channel still serves invocations.
	r2 := p.invoke(t, "handler.count", `{}`, nil)
	if !r2.OK || !strings.Contains(r2.Out, "count=2") {
		t.Fatalf("invoke after the rejected second connection = %+v, want count=2 (no extra handler, session intact)", r2)
	}
}

// TestPythonRuntimeNonStandardJSONConstantsRejectedBeforeHandler pins the JSON
// extension rejection: NaN / Infinity / -Infinity (which Python's json accepts
// but JSON.parse does not) are refused at frame decode, BEFORE any handler side
// effect, and the channel closes — matching Node behavior. Standard numbers
// with exponents remain valid and are covered by the sibling test.
func TestPythonRuntimeNonStandardJSONConstantsRejectedBeforeHandler(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "RAN")
	writeHandler(t, dir, "handler", fmt.Sprintf(`
def mark(event, context):
    with open(%q, "w") as fh:
        fh.write("ran")
`, marker))
	ctxJSON, err := json.Marshal(defaultHandlerContext("handler.mark"))
	if err != nil {
		t.Fatalf("marshal context: %v", err)
	}
	for _, constant := range []string{"NaN", "Infinity", "-Infinity"} {
		t.Run(constant, func(t *testing.T) {
			p := startPython(t, dir)
			raw := fmt.Sprintf(
				`{"type":"invoke","id":"bad-const","handler":"handler.mark","event":{"v":%s},"env":{},"context":%s}`,
				constant, ctxJSON,
			)
			if err := channelWriteFrame(p.conn, []byte(raw)); err != nil {
				t.Fatalf("write nonstandard frame: %v", err)
			}
			expectFrameReadFails(t, p.conn, 5*time.Second)
			_ = p.closeChannel()
			if _, err := os.Stat(marker); err == nil {
				t.Fatalf("%s must be rejected before any handler side effect", constant)
			}
			// The protocol failure closes the channel; the process must exit
			// (Relay then discards the container), not hang.
			_ = p.waitExit(t, 10*time.Second)
		})
	}
}

// TestPythonRuntimeValidNumericPayloadPreserved keeps the standard half of the
// contract green: exponents, negatives, and plain integers in the event decode
// and reach the handler unchanged.
func TestPythonRuntimeValidNumericPayloadPreserved(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
def nums(event, context):
    print("sci=%r" % event["sci"])
    print("tiny=%r" % event["tiny"])
    print("int=%r" % event["int"])
`)
	p := startPython(t, dir)
	r := p.invoke(t, "handler.nums",
		`{"sci":1.5e3,"tiny":-2.51e-8,"int":42}`, nil)
	if !r.OK {
		t.Fatalf("standard numeric payload rejected: %s", r.Err)
	}
	for _, want := range []string{"sci=1500.0", "tiny=-2.51e-08", "int=42"} {
		if !strings.Contains(r.Out, want) {
			t.Errorf("out = %q, want %q", r.Out, want)
		}
	}
}

// TestPythonRuntimeContextHandlerMismatchClosesChannel pins the strict routing
// identity: context.handler must equal the invocation's handler. A mismatch is
// a protocol violation that closes the authenticated channel BEFORE handler
// execution — no result, no side effect.
func TestPythonRuntimeContextHandlerMismatchClosesChannel(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "RAN")
	writeHandler(t, dir, "handler", fmt.Sprintf(`
def mark(event, context):
    with open(%q, "w") as fh:
        fh.write("ran")
`, marker))
	p := startPython(t, dir)

	r := p.invokeFrame(t, "handler.mark", `{}`, nil, nil, map[string]any{
		"requestId":   "req-1",
		"attempt":     1,
		"maxAttempts": 1,
		"deadline":    "2031-01-01T00:00:00Z",
		"app":         "fn",
		"handler":     "handler.other", // MISMATCH with handler.mark
	})
	if r.OK {
		t.Fatal("a context.handler mismatch must close the channel, not return a result")
	}
	// The write succeeded but no result arrives: the channel closes instead.
	expectFrameReadFails(t, p.conn, 5*time.Second)
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the mismatch must be rejected before the handler runs")
	}
	_ = p.closeChannel()
	_ = p.waitExit(t, 10*time.Second)
}

// TestPythonRuntimeMissingContextClosesChannel pins the required context
// object: a frame without "context" (or with a context missing a required
// identity field) is a protocol violation — the channel closes before the
// handler runs, never a partial or fabricated context.
func TestPythonRuntimeMissingContextClosesChannel(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "RAN")
	writeHandler(t, dir, "handler", fmt.Sprintf(`
def sink(event, context):
    with open(%q, "w") as fh:
        fh.write("ran")
`, marker))
	p := startPython(t, dir)

	raw := `{"type":"invoke","id":"no-ctx","handler":"handler.sink","event":{},"env":{}}`
	if err := channelWriteFrame(p.conn, []byte(raw)); err != nil {
		t.Fatalf("write context-less frame: %v", err)
	}
	expectFrameReadFails(t, p.conn, 5*time.Second)
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("a frame without the context object must not run the handler")
	}
	_ = p.closeChannel()
	_ = p.waitExit(t, 10*time.Second)
}

// TestPythonRuntimeUnparseableDeadlineFailsInvocation pins the invocation-level
// half: a context with every required field but an unparseable deadline fails
// ONLY that invocation with an ok:false context error, and the container stays
// reusable for the next request.
func TestPythonRuntimeUnparseableDeadlineFailsInvocation(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", "def sink(event, context):\n    print('ran')\n")
	p := startPython(t, dir)

	r := p.invokeFrame(t, "handler.sink", `{}`, nil, nil, map[string]any{
		"requestId":   "req-1",
		"attempt":     1,
		"maxAttempts": 1,
		"deadline":    "not-a-date",
		"app":         "fn",
		"handler":     "handler.sink",
	})
	if r.OK {
		t.Fatal("an unparseable deadline must fail the invocation")
	}
	if !strings.Contains(r.Err, "invalid handler context") {
		t.Errorf("error = %q, want the invalid-context rejection", r.Err)
	}
	// The container stays healthy: the next invocation succeeds.
	ok := p.invoke(t, "handler.sink", `{}`, nil)
	if !ok.OK || !strings.Contains(ok.Out, "ran") {
		t.Fatalf("post-context-error invoke = %+v, want a healthy container", ok)
	}
}

// TestPythonRuntimeInvokeMalformedFrameClosesChannel pins authenticated-phase
// framing: a malformed JSON payload on the authenticated channel is a protocol
// failure — the channel closes, the process exits (Relay discards the
// container), and no handler runs.
func TestPythonRuntimeInvokeMalformedFrameClosesChannel(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	marker := filepath.Join(dir, "RAN")
	writeHandler(t, dir, "handler", fmt.Sprintf(`
def mark(event, context):
    with open(%q, "w") as fh:
        fh.write("ran")
`, marker))
	p := startPython(t, dir)

	if err := channelWriteFrame(p.conn, []byte("this is not json")); err != nil {
		t.Fatalf("write malformed frame: %v", err)
	}
	expectFrameReadFails(t, p.conn, 5*time.Second)
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("a malformed frame must not run a handler")
	}
	_ = p.closeChannel()
	_ = p.waitExit(t, 10*time.Second)
}

// TestPythonRuntimeOversizeHelloRejected pins the handshake frame cap: a
// declared length over 4 KiB is refused before allocation, the peer is closed
// unauthenticated, and the listener survives.
func TestPythonRuntimeOversizeHelloRejected(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", "def f(event, context):\n    pass\n")
	p := startUnauthenticatedPython(t, dir)

	conn := p.dialChannel(t, 5*time.Second)
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(channelHandshakeCap+1))
	if _, err := conn.Write(header[:]); err != nil {
		t.Fatalf("write oversize header: %v", err)
	}
	expectFrameReadFails(t, conn, 5*time.Second)
	_ = conn.Close()

	conn2 := p.reconnect(t)
	defer conn2.Close()
	p.conn = conn2
	r := p.invoke(t, "handler.f", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke after the oversize rejection failed: %s", r.Err)
	}
}

// TestPythonRuntimePartialHelloFrameTimesOut pins the exact-read rule: a frame
// whose payload never completes gets the peer closed by the handshake deadline,
// with no handler side effect and the listener left standing.
func TestPythonRuntimePartialHelloFrameTimesOut(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", "def f(event, context):\n    pass\n")
	p := startUnauthenticatedPython(t, dir)

	conn := p.dialChannel(t, 5*time.Second)
	var header [4]byte
	header[3] = 100 // declares 100 bytes, delivers only a few
	if _, err := conn.Write(header[:]); err != nil {
		t.Fatalf("write partial header: %v", err)
	}
	if _, err := conn.Write([]byte(`{"type":"he`)); err != nil {
		t.Fatalf("write partial payload: %v", err)
	}
	// The bootstrap's handshake timeout tears the half-frame peer down.
	expectFrameReadFails(t, conn, 6*time.Second)
	_ = conn.Close()

	conn2 := p.reconnect(t)
	defer conn2.Close()
	p.conn = conn2
	r := p.invoke(t, "handler.f", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke after the partial-frame timeout failed: %s", r.Err)
	}
}

// TestPythonRuntimeProtocolFramesNeverOnStdout pins the channel/log
// separation: everything the protocol exchanges lives on the connection, while
// the child's stdout carries ONLY native application output — no result/auth/
// invoke frame JSON and no legacy sentinel ever appears there.
func TestPythonRuntimeProtocolFramesNeverOnStdout(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", "def f(event, context):\n    print('native-output-line')\n")
	p := startPython(t, dir)

	r := p.invoke(t, "handler.f", `{}`, nil)
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "native-output-line") {
		t.Fatalf("stdout out = %q, want the native print", r.Out)
	}
	stdout := p.stdoutString()
	for _, forbidden := range []string{
		`"type":"result"`,
		`"type":"invoke"`,
		`"type":"auth"`,
		`"type":"ready"`,
		`"mac"`,
		"@@RELAY@@",
	} {
		if strings.Contains(stdout, forbidden) {
			t.Errorf("protocol data %q leaked to stdout:\n%s", forbidden, stdout)
		}
	}
	if !utf8.ValidString(stdout) {
		t.Error("stdout must remain valid text (no binary frame headers)")
	}
}

// maxResultFrame is the contract's result payload cap (1 MiB), mirrored here to
// avoid an import cycle with the Go runtime package.
const maxResultFrame = 1 << 20

// maxErrorBytes is the contract's diagnostic error-string cap (64 KiB).
const maxErrorBytes = 64 << 10

// TestPythonRuntimeErrorFrameBound pins the error-frame bounds across ASCII,
// multibyte, emoji, accented, and control/escaped text: the emitted frame stays
// within the 1 MiB result cap in UTF-8 bytes, its error field within 64 KiB,
// it remains parseable JSON with the echoed id and ok:false, and the process
// stays healthy afterwards.
func TestPythonRuntimeErrorFrameBound(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
def short(event, context):
    raise ValueError("short ascii")

def long(event, context):
    raise ValueError("x" * 100000)

def multi(event, context):
    raise ValueError("é" * 100000)

def emoji(event, context):
    raise ValueError("\U0001F600" * 40000)

def accented(event, context):
    raise ValueError("àéîõü" * 30000)

def control(event, context):
    raise ValueError("\t\n\x00\x07" * 30000)
`)
	p := startPython(t, dir)

	for _, tc := range []struct {
		name    string
		handler string
	}{
		{"short ascii", "handler.short"},
		{"long ascii", "handler.long"},
		{"multibyte", "handler.multi"},
		{"emoji", "handler.emoji"},
		{"accented", "handler.accented"},
		{"control", "handler.control"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := p.invoke(t, tc.handler, `{}`, nil)
			if r.OK {
				t.Fatal("expected the raising handler to fail the invocation")
			}
			if len(r.Frame) > maxResultFrame {
				t.Fatalf("frame length %d exceeds the result cap %d", len(r.Frame), maxResultFrame)
			}
			if !utf8.ValidString(r.Frame) {
				t.Fatal("frame must be valid UTF-8")
			}
			var resp respFrame
			if err := json.Unmarshal([]byte(r.Frame), &resp); err != nil {
				t.Fatalf("frame must be valid parseable JSON: %v (%q)", err, r.Frame)
			}
			if resp.ID == "" || resp.OK {
				t.Fatalf("resp = %+v, want a non-empty id and ok:false for the handler error", resp)
			}
			if resp.Error == "" {
				t.Fatal("expected a non-empty bounded error string")
			}
			if len(resp.Error) > maxErrorBytes {
				t.Fatalf("error length %d exceeds the %d-byte diagnostic cap", len(resp.Error), maxErrorBytes)
			}
			// The process must stay healthy after an oversized error.
			ok := p.invoke(t, "handler.short", `{}`, nil)
			if ok.OK {
				t.Fatal("expected the short handler to also fail")
			}
		})
	}
}

// TestPythonRuntimeHandlerContextValues verifies the handler's second argument
// carries the frame's execution identity as snake_case fields: the stable
// request ID, the confirmed attempt and configured budget, an aware deadline
// parsed from the protocol's ISO-8601 string (nanosecond input truncated to
// microseconds), the optional message and trace IDs, and the app/handler —
// with context.handler MATCHING the invocation handler (a mismatch is a
// protocol violation). The event payload stays verbatim.
func TestPythonRuntimeHandlerContextValues(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
def show(event, context):
    print("event=%s" % event["msg"])
    print("request_id=" + context.request_id)
    print("attempt=%d" % context.attempt)
    print("max_attempts=%d" % context.max_attempts)
    print("deadline=" + context.deadline.isoformat())
    print("aware=" + str(context.deadline.tzinfo is not None))
    print("message_id=" + str(context.message_id))
    print("trace_id=" + str(context.trace_id))
    print("app=" + context.app)
    print("handler=" + context.handler)
`)
	p := startPython(t, dir)

	r := p.invokeFrame(t, "handler.show", `{"msg":"hi"}`, nil, nil, map[string]any{
		"requestId":   "event:m-7:app/events.created",
		"attempt":     2,
		"maxAttempts": 4,
		"deadline":    "2031-04-05T06:07:08.123456789Z",
		"messageId":   "m-7",
		"traceId":     "4bf92f3577b34da6a3ce929d0e0e4736",
		"app":         "app",
		"handler":     "handler.show", // must match the invocation handler
	})
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	for _, want := range []string{
		"event=hi",
		"request_id=event:m-7:app/events.created",
		"attempt=2",
		"max_attempts=4",
		"deadline=2031-04-05T06:07:08.123456+00:00",
		"aware=True",
		"message_id=m-7",
		"trace_id=4bf92f3577b34da6a3ce929d0e0e4736",
		"app=app",
		"handler=handler.show",
	} {
		if !strings.Contains(r.Out, want) {
			t.Errorf("out = %q, want it to contain %q", r.Out, want)
		}
	}
}

// TestPythonRuntimeHandlerContextOptionalMetadataAbsent verifies the optional
// broker and trace identities stay None when the frame omits them (a state-free
// execution): they are optional, never fabricated by the bootstrap.
func TestPythonRuntimeHandlerContextOptionalMetadataAbsent(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
def opt(event, context):
    print("message_id=" + str(context.message_id))
    print("trace_id=" + str(context.trace_id))
`)
	p := startPython(t, dir)

	r := p.invokeFrame(t, "handler.opt", `{}`, nil, nil, map[string]any{
		"requestId":   "manual-1",
		"attempt":     1,
		"maxAttempts": 1,
		"deadline":    "2031-01-01T00:00:00Z",
		"app":         "fn",
		"handler":     "handler.opt",
	})
	if !r.OK {
		t.Fatalf("invoke failed: %s", r.Err)
	}
	if !strings.Contains(r.Out, "message_id=None") {
		t.Errorf("out = %q, want message_id=None when the frame omits messageId", r.Out)
	}
	if !strings.Contains(r.Out, "trace_id=None") {
		t.Errorf("out = %q, want trace_id=None when the frame omits traceId", r.Out)
	}
}

// TestPythonRuntimeHandlerContextRemainingTime verifies get_remaining_time_ms()
// against a future deadline (positive and bounded by the configured span), an
// expired deadline (clamped to zero), and the zero instant the protocol emits
// for a context with no deadline (also zero, never negative).
func TestPythonRuntimeHandlerContextRemainingTime(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
def remain(event, context):
    print("ms=%d" % context.get_remaining_time_ms())
`)
	p := startPython(t, dir)

	ctxFor := func(deadline string) map[string]any {
		return map[string]any{
			"requestId":   "req-1",
			"attempt":     1,
			"maxAttempts": 1,
			"deadline":    deadline,
			"app":         "fn",
			"handler":     "handler.remain",
		}
	}
	remaining := func(t *testing.T, deadline string) int {
		t.Helper()
		r := p.invokeFrame(t, "handler.remain", `{}`, nil, nil, ctxFor(deadline))
		if !r.OK {
			t.Fatalf("invoke with deadline %q failed: %s", deadline, r.Err)
		}
		// The invocation window carries the START/END lifecycle lines beside
		// the handler's print; read the LAST ms line of the window.
		msText := ""
		for _, line := range strings.Split(r.Out, "\n") {
			if v, ok := strings.CutPrefix(strings.TrimSpace(line), "ms="); ok && v != "" {
				msText = v
			}
		}
		if msText == "" {
			t.Fatalf("out = %q, want an ms=<int> line", r.Out)
		}
		ms, err := strconv.Atoi(msText)
		if err != nil {
			t.Fatalf("remaining %q is not an integer: %v", msText, err)
		}
		return ms
	}

	futureDeadline := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	future := remaining(t, futureDeadline)
	if future <= 3_590_000 || future > 3_600_000 {
		t.Errorf("remaining for a +1h deadline = %d ms, want within (3590000, 3600000]", future)
	}
	if got := remaining(t, "2000-01-01T00:00:00Z"); got != 0 {
		t.Errorf("remaining for an expired deadline = %d, want 0 (clamped)", got)
	}
	if got := remaining(t, "0001-01-01T00:00:00Z"); got != 0 {
		t.Errorf("remaining for the zero instant = %d, want 0 (clamped)", got)
	}
}

// TestPythonRuntimeHandlerContextFreshPerInvocation proves the warm, reused
// process builds a fresh context per request: each invocation sees its own
// frame's request ID (no leakage from the previous one) and a distinct object
// identity even though the interpreter and module state persist.
func TestPythonRuntimeHandlerContextFreshPerInvocation(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
_prev = {"ctx": None}

def remember(event, context):
    print("fresh=%s" % (context is not _prev["ctx"]))
    print("request_id=" + context.request_id)
    _prev["ctx"] = context
`)
	p := startPython(t, dir)

	ctxFor := func(requestID string) map[string]any {
		return map[string]any{
			"requestId":   requestID,
			"attempt":     1,
			"maxAttempts": 3,
			"deadline":    "2031-01-01T00:00:00Z",
			"app":         "fn",
			"handler":     "handler.remember",
		}
	}
	r1 := p.invokeFrame(t, "handler.remember", `{}`, nil, nil, ctxFor("event:m-a:fn/remember"))
	if !r1.OK {
		t.Fatalf("first invoke failed: %s", r1.Err)
	}
	r2 := p.invokeFrame(t, "handler.remember", `{}`, nil, nil, ctxFor("event:m-b:fn/remember"))
	if !r2.OK {
		t.Fatalf("second invoke failed: %s", r2.Err)
	}
	for i, r := range []invokeResult{r1, r2} {
		if !strings.Contains(r.Out, "fresh=True") {
			t.Errorf("invoke %d out = %q, want fresh=True (a new context object per invocation)", i+1, r.Out)
		}
	}
	if !strings.Contains(r1.Out, "request_id=event:m-a:fn/remember") {
		t.Errorf("first out = %q, want its own request ID", r1.Out)
	}
	if !strings.Contains(r2.Out, "request_id=event:m-b:fn/remember") {
		t.Errorf("second out = %q, want its own request ID (no leakage from the first)", r2.Out)
	}
}

// TestPythonRuntimeSingleArgHandlerRejected pins the fixed two-argument
// calling convention: the bootstrap always calls handler(event, context), so a
// legacy single-argument handler fails its invocation with a TypeError instead
// of Relay adapting to it.
func TestPythonRuntimeSingleArgHandlerRejected(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", "def one(event):\n    pass\n")
	p := startPython(t, dir)

	r := p.invoke(t, "handler.one", `{}`, nil)
	if r.OK {
		t.Fatal("a single-argument handler must be rejected, not adapted to")
	}
	if !strings.Contains(r.Err, "positional argument") {
		t.Errorf("error = %q, want the two-argument TypeError", r.Err)
	}
}

// TestPythonRuntimeCancelledErrorIsNotNormalFailure pins the review fix in
// invoke(): an async handler raising asyncio.CancelledError must RE-RAISE
// (task cancellation is control flow, not a handler failure) so asyncio
// shutdown can join its tasks — the cancelled invocation must NOT receive a
// normal ok:false result frame, and the authenticated channel/process must
// terminate promptly under a bounded wait (no hang). The control half runs an
// ordinary Exception first on the SAME session: still a normal ok:false
// carrying the handler error, with the process kept alive.
func TestPythonRuntimeCancelledErrorIsNotNormalFailure(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import asyncio

async def boom(event, context):
    raise ValueError("ordinary-kaboom")

async def cancelled(event, context):
    raise asyncio.CancelledError()
`)
	p := startPython(t, dir)

	// Control: an ordinary Exception is still a NORMAL invocation failure —
	// a result frame with ok:false carrying the handler error — and the same
	// authenticated session keeps serving (two failures prove the process and
	// channel stayed alive between and after them). Both failures are COMPLETED
	// invocations: each carries its START/END pair under the LOGICAL requestId
	// its context frame declares, never the transport correlation id (t0, t1),
	// which the result frame echoes instead.
	for i := 1; i <= 2; i++ {
		transportID := fmt.Sprintf("t%d", p.seq)
		logical := fmt.Sprintf("handled-fail-%d", i)
		r := p.invokeFrame(t, "handler.boom", `{}`, nil, nil, logicalRequestContext("handler.boom", logical, i, 3))
		if r.OK {
			t.Fatalf("ordinary failure %d: an Exception must be a normal ok:false result", i)
		}
		if !strings.Contains(r.Err, "ordinary-kaboom") {
			t.Fatalf("ordinary failure %d error = %q, want the handler error text", i, r.Err)
		}
		if r.Frame == "" {
			t.Fatalf("ordinary failure %d must arrive as a result frame", i)
		}
		requireTransportEcho(t, r, transportID)
		// A handled failure completes its invocation: END follows START, both
		// under the logical id and never under the transport id.
		requireLogicalLifecycleLines(t, r.Out, logical, transportID, "")
	}
	select {
	case <-p.exitDone:
		t.Fatalf("process exited after ordinary handler errors: %v", p.waitErr)
	default:
	}

	// The regression: a CancelledError invocation must not be converted into a
	// normal handler failure. If invoke() swallows the cancellation, a result
	// frame arrives here and the assertion fails with the exact frame. Its
	// transport correlation id is the harness's next t<N>; the logical id is
	// deliberately different.
	cancelledTransport := fmt.Sprintf("t%d", p.seq)
	const cancelledLogical = "cancelled-logical"
	r := p.invokeFrame(t, "handler.cancelled", `{}`, nil, nil, logicalRequestContext("handler.cancelled", cancelledLogical, 1, 3))
	if r.Frame != "" {
		t.Fatalf("a CancelledError invocation must not receive a result frame, got: %s", r.Frame)
	}
	if !strings.Contains(r.Err, "read result frame") {
		t.Errorf("want the cancelled channel surfaced as a read-side close, got %q", r.Err)
	}

	// Prompt termination first: the shutdown path must join and exit under a
	// bounded wait — a hang fails here rather than stalling the suite. Because
	// cmd.Wait joins os/exec's output-copy goroutines, a completed exit also
	// means both pipes have drained, so the final stdout snapshot below is
	// taken against a complete, deterministic capture.
	if err := p.waitExit(t, 10*time.Second); err != nil {
		t.Logf("bootstrap exit status after cancellation: %v (termination is the contract; code not pinned)", err)
	}

	// The interrupted lifecycle contract: the cancelled invocation STARTed
	// under its logical requestId but its END must never be fabricated — a
	// task cancellation is not a completed invocation — and neither line may
	// carry the transport correlation id.
	stdout := p.stdoutString()
	if index := strings.Index(stdout, "START RequestId: "+cancelledLogical); index < 0 {
		t.Errorf("the cancelled invocation must have STARTed under its logical requestId, stdout:\n%s", stdout)
	}
	if strings.Contains(stdout, "END RequestId: "+cancelledLogical) {
		t.Errorf("END must never be fabricated for a cancelled invocation; stdout:\n%s", stdout)
	}
	for _, field := range []string{"START RequestId: ", "END RequestId: "} {
		if strings.Contains(stdout, field+cancelledTransport) {
			t.Errorf("lifecycle lines must not carry the transport correlation id %q; stdout:\n%s", cancelledTransport, stdout)
		}
	}
}
