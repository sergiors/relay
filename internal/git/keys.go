package git

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/transport"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"github.com/skeema/knownhosts"
	"golang.org/x/crypto/ssh"
)

// privateKeyComment is the SSH comment recorded in the generated key. It
// identifies the key's owner to the provider's deploy-key management screen.
const privateKeyComment = "relay"

// GenerateKey generates a new ed25519 private key pair and persists the private
// key atomically to dir/PrivateKeyFile (0700 dir, 0600 file), and returns the
// SSH-authorized-keys serialization of the public key for the operator to paste
// as a read-only Deploy Key. It refuses to overwrite an existing key file: a
// key, once generated, is a fixed identity the operator registers with a remote
// provider; silently regenerating it would orphan the public key registered
// there. To rotate, the operator must explicitly remove the old key and run
// keygen again.
//
// The implementation is provider-neutral: it never calls a GitHub/GitLab (or
// any) provider API. The operator completes registration out of band.
func GenerateKey(dir string) (publicAuthorized string, err error) {
	keyPath := filepath.Join(dir, PrivateKeyFile)
	// Guard against silent overwrite BEFORE generating anything.
	if _, statErr := os.Stat(keyPath); statErr == nil {
		return "", fmt.Errorf("git: SSH key already exists at %s; remove it first to regenerate", keyPath)
	} else if !os.IsNotExist(statErr) {
		return "", fmt.Errorf("git: stat key %s: %w", keyPath, statErr)
	}

	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", fmt.Errorf("git: generate ed25519 key: %w", err)
	}
	// ed25519.GenerateKey returns (publicKey, privateKey, err); pubKey is the
	// public half and privKey the private half. The private half implements the
	// crypto.Signer used to derive the SSH public key, so we pass it to
	// ssh.NewPublicKey and privKey to ssh.MarshalPrivateKey.
	sshPub, err := ssh.NewPublicKey(pubKey)
	if err != nil {
		return "", fmt.Errorf("git: serialize public key: %w", err)
	}

	block, err := ssh.MarshalPrivateKey(privKey, privateKeyComment)
	if err != nil {
		return "", fmt.Errorf("git: serialize private key: %w", err)
	}
	// MarshalPrivateKey returns a PEM block already carrying the OpenSSH
	// header; EncodeToMemory renders it as text.
	pemBytes := pem.EncodeToMemory(block)

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("git: create ssh dir: %w", err)
	}
	// Atomic write mirroring secrets/local.go: temp file in the same dir,
	// chmod 0600, fsync, then rename. A partial key must never be persisted.
	tmp, err := os.CreateTemp(dir, ".key-*")
	if err != nil {
		return "", fmt.Errorf("git: create key temp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("git: chmod key temp: %w", err)
	}
	if _, err := tmp.Write(pemBytes); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("git: write key temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("git: sync key temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("git: close key temp: %w", err)
	}
	if err := os.Rename(tmpName, keyPath); err != nil {
		return "", fmt.Errorf("git: rename key: %w", err)
	}

	// Authorized-keys form includes a trailing newline; trim it so printing
	// and tests get a clean single line.
	return strings.TrimSuffix(string(ssh.MarshalAuthorizedKey(sshPub)), "\n"), nil
}

// keyExists reports whether a private key file exists under dir. Status uses it
// to render a bool; it never exposes key material. A missing file returns false,
// as does a non-not-exist stat error (Status treats an unreadable key as absent
// rather than failing the whole status read).
func keyExists(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, PrivateKeyFile))
	return err == nil
}

// sshAuthFor builds the go-git SSH auth method from the persisted key under
// sshDir, wired for TOFU (Trust On First Use) host-key verification using
// Relay's OWN known_hosts file (<sshDir>/known_hosts). It no longer consults
// the system files (~/.ssh/known_hosts, /etc/ssh/ssh_known_hosts,
// SSH_KNOWN_HOSTS) and never needs ssh-keyscan: Relay is provider-neutral and
// works with GitHub, GitLab, or any SSH git server by trusting each host on its
// first contact and recording the fingerprint itself.
//
// The returned callback never returns a nil HostKeyCallback and never uses
// InsecureIgnoreHostKey. Verification semantics, per host:
//   - unknown host  -> persist its key (TOFU) and accept; the first-trust is
//     surfaced via out (a user-facing line) and log (Debug attrs).
//   - known, matching -> accept (normal verification).
//   - known, changed  -> fail loudly with an operator-actionable MITM warning
//     and never modify known_hosts.
//
// ep is the already-parsed SSH endpoint (so hostWithPort is consistent with the
// address go-git dials); out and log are both optional (nil is silent). out is
// the manual CLI sync's user-facing line; log is the background sync's
// structured diagnostic (see tofuHostKeyCallback).
func sshAuthFor(sshDir string, ep *transport.Endpoint, out io.Writer, log *slog.Logger) (gitssh.AuthMethod, error) {
	data, err := os.ReadFile(filepath.Join(sshDir, PrivateKeyFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("no SSH key found under %s; run relay git keygen first", sshDir)
		}
		return nil, fmt.Errorf("git: read key %s: %w", PrivateKeyFile, err)
	}

	// NewDB only ever sees a file that exists: ensureKnownHostsFile creates an
	// empty 0600 file (0700 dir) when absent, so "no hosts trusted yet" is a valid
	// state, not an error. Empty known_hosts yields a callback with an empty
	// trust set whose first contact takes the TOFU path below.
	if err := ensureKnownHostsFile(knownHostsPath(sshDir)); err != nil {
		return nil, err
	}
	db, err := knownhosts.NewDB(knownHostsPath(sshDir))
	if err != nil {
		return nil, fmt.Errorf("git: load known_hosts %s: %w", KnownHostsFile, err)
	}

	auth, err := gitssh.NewPublicKeys("git", data, "")
	if err != nil {
		return nil, fmt.Errorf("git: parse SSH key: %w", err)
	}
	auth.HostKeyCallback = tofuHostKeyCallback(sshDir, db, ep, out, log)
	// When the host is already trusted, restrict the algorithms the client will
	// offer to exactly those in known_hosts for that host. This mirrors what
	// go-git's own connect() does in its known_hosts branch (ssh/common.go:134),
	// and here it matters: go-git leaves HostKeyAlgorithms to the user when a
	// custom HostKeyCallback is set (ssh/common.go:135-140), so without this the
	// client would offer its default preference list. On FIRST contact
	// (hostWithPort not yet known) HostKeyAlgorithms(hostWithPort) is empty and
	// the field stays nil, letting golang ssh dial with default preferences and
	// the callback persist whatever key the server presents. On SUBSEQUENT
	// contacts the restricted list guarantees the callback only ever sees a key
	// type the pinned known_hosts entry records (a server offering only something
	// else is a mismatch). Setting nil when empty is required: an empty non-nil
	// slice would instruct ssh to offer NO host key algorithms.
	hostWithPort := net.JoinHostPort(ep.Host, strconv.Itoa(endpointPort(ep)))
	auth.HostKeyAlgorithms = db.HostKeyAlgorithms(hostWithPort)
	return auth, nil
}

// endpointPort returns the endpoint's port, defaulting to 22 (go-git's
// DefaultPort) when unset, matching getHostWithPort in go-git's ssh transport.
func endpointPort(ep *transport.Endpoint) int {
	if ep.Port <= 0 {
		return 22
	}
	return ep.Port
}

// knownHostsPath returns the Relay-owned known_hosts path under sshDir.
func knownHostsPath(sshDir string) string {
	return filepath.Join(sshDir, KnownHostsFile)
}

// ensureKnownHostsFile creates Relay's known_hosts file (and its 0700 parent)
// at path when missing, as an empty 0600 file. "File absent" is the "no hosts
// trusted yet" TOFU state, not an error; a present-but-unreadable file is a real
// error. Creating an empty file up front keeps knownhosts.NewDB (which errors on
// a nonexistent path) simple and always successful.
func ensureKnownHostsFile(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("git: create ssh dir: %w", err)
	}
	// O_CREATE|O_EXCL guards against racing a concurrent first-trust; a
	// successful open means some other goroutine already created it.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return nil
		}
		return fmt.Errorf("git: create known_hosts: %w", err)
	}
	return f.Close()
}

// tofuHostKeyCallback wraps the strict inner known_hosts callback with the TOFU
// policy. err == nil (host known and matching) is normal verification. A
// HostKeyChanged error is a loud, operator-actionable MITM warning that never
// touches known_hosts. A HostUnknown error triggers TOFU: the presented key is
// appended to Relay's known_hosts file and the connection proceeds, with the
// first-trust event surfaced on out (user line) and log (Debug attrs).
func tofuHostKeyCallback(sshDir string, db *knownhosts.HostKeyDB, ep *transport.Endpoint, out io.Writer, log *slog.Logger) ssh.HostKeyCallback {
	inner := db.HostKeyCallback()
	return ssh.HostKeyCallback(func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		err := inner(hostname, remote, key)
		switch {
		case err == nil:
			// Known and matching: normal OpenSSH-style verification.
			return nil
		case knownhosts.IsHostKeyChanged(err):
			// The host's key changed under us — the classic MITM signal. Fail
			// loudly and NEVER auto-update known_hosts: silently replacing the
			// key would let an attacker pin their own key. The operator resolves
			// a genuinely expected rotation by editing the file manually.
			return fmt.Errorf("git: host key for %s has changed! This may indicate a man-in-the-middle attack. If the change is expected, remove the host's line(s) from %s and sync again", hostname, knownHostsPath(sshDir))
		case knownhosts.IsHostUnknown(err):
			// First use of this host: TOFU — persist the key and continue. The
			// append is done with the file left O_APPEND (see the comment below),
			// deliberately NOT the temp+rename pattern used for config/key,
			// because known_hosts is append-only by design.
			if werr := appendKnownHost(knownHostsPath(sshDir), hostname, remote, key); werr != nil {
				return werr
			}
			// Surfacing the first-trust: a sentence-style user line on out plus a
			// distinct structured Debug record (never the raw key bytes). The
			// writer line serves the manual CLI sync; the Debug record is the
			// operational diagnostic and the ONLY record on the background
			// worker/webhook sync, where out is nil.
			fp := ssh.FingerprintSHA256(key)
			host := knownhosts.Normalize(hostname)
			if out != nil {
				fmt.Fprintf(out, "Trusted new host %s (fingerprint %s)\n", host, fp)
			}
			if log != nil {
				log.Debug("TOFU host key trusted", "host", host, "fingerprint", fp)
			}
			return nil
		default:
			// Any other error (corrupt line, unreadable entry): propagate.
			return err
		}
	})
}

// appendKnownHost appends a host-key line to Relay's known_hosts file, creating
// it (0700 dir, 0600 file) if absent, and returns a descriptive error on any
// failure so a first-trust that cannot be persisted fails the connection rather
// than silently proceeding on an unverified host.
func appendKnownHost(path, hostname string, remote net.Addr, key ssh.PublicKey) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("git: create ssh dir: %w", err)
	}
	// O_APPEND|O_CREATE: a fresh (possibly zero) file or an existing one, always
	// opened append-only with the 0600 mode applied to a newly created file. This
	// is a deliberate divergence from the config/key rename pattern (see
	// writeConfig/GenerateKey): known_hosts is a pure append log of distinct
	// host:port=>key pins, and re-opening with O_APPEND on every new host is
	// simplest and correct. Concurrent first-trusts of different hosts each get
	// an O_APPEND write that lands atomically at the end without overwriting a
	// sibling line. A changed/key mismatch path never reaches here (only the
	// TOFU HostUnknown branch does), so an existing pin is never replaced.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("git: open known_hosts: %w", err)
	}
	if err := knownhosts.WriteKnownHost(f, hostname, remote, key); err != nil {
		_ = f.Close()
		return fmt.Errorf("git: write known_hosts: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("git: sync known_hosts: %w", err)
	}
	return f.Close()
}
