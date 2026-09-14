package git

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
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
// ssdDir. It enforces host-key verification through the system known_hosts
// files via go-git's NewKnownHostsCallback (which reads ~/.ssh/known_hosts,
// /etc/ssh/ssh_known_hosts, and the SSH_KNOWN_HOSTS env var). Verification is
// NEVER disabled: when no known_hosts file can be found, sshAuthFor returns a
// descriptive error telling the operator to add the host, never an
// insecure-verification fallback.
func sshAuthFor(ssdDir string) (gitssh.AuthMethod, error) {
	data, err := os.ReadFile(filepath.Join(ssdDir, PrivateKeyFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("no SSH key found under %s; run relay git keygen first", ssdDir)
		}
		return nil, fmt.Errorf("git: read key %s: %w", PrivateKeyFile, err)
	}
	// This is the hard known_hosts requirement: the callback errors when it
	// cannot locate any known_hosts source, and we propagate that error with
	// operator guidance instead of skipping host verification.
	cb, err := gitssh.NewKnownHostsCallback()
	if err != nil {
		return nil, fmt.Errorf("git: cannot verify host key, no known_hosts file: %v; add the host first, e.g. 'ssh-keyscan <host> >> ~/.ssh/known_hosts'", err)
	}
	auth, err := gitssh.NewPublicKeys("git", data, "")
	if err != nil {
		return nil, fmt.Errorf("git: parse SSH key: %w", err)
	}
	auth.HostKeyCallback = cb
	return auth, nil
}
