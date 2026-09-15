// Package git provides the manual, operator-driven Git synchronization
// workflow for Relay. See doc.go for the full model, transport rules, and
// storage layout.
package git

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"

	"relay/internal/secrets"
)

// Fixed application-convention paths, mirroring state.DBPath and
// secrets.SecretsDir: they are constants, not env-configurable, so the compose
// volume mount at /var/lib/relay persists them across restarts. Every operation
// in this package takes directories as explicit parameters (so tests redirect
// them away from /var/lib/relay); these constants are only the production
// defaults the CLI wires in.
const (
	// SSHDir is the root that holds the SSH deploy key.
	SSHDir = "/var/lib/relay/ssh"
	// GitDir is the root that holds the sync config and checkout.
	GitDir = "/var/lib/relay/git"
	// ConfigPath is the persisted sync-config JSON file, under GitDir.
	ConfigPath = GitDir + "/source.json"
	// CheckoutDir is the managed clone/worktree of the source repository, under GitDir.
	CheckoutDir = GitDir + "/checkout"
	// PrivateKeyFile is the deploy key filename written under SSHDir.
	PrivateKeyFile = "id_ed25519"
	// KnownHostsFile is Relay's own OpenSSH known_hosts file under SSHDir,
	// maintained by the TOFU host-key callback (see hostkey.go). It is
	// deliberately NOT the operator's ~/.ssh/known_hosts: Relay trusts on first
	// use and records the fingerprint itself, so no ssh-keyscan step is needed.
	KnownHostsFile = "known_hosts"
	// DefaultRef is the ref used by `relay git set` when --ref is omitted.
	DefaultRef = "main"
)

// syncTimeout bounds the whole network sync (clone, fetch, resolve, checkout).
// It is a fixed constant because a sync cannot finish faster than the remote
// permits; a hung remote should fail loudly rather than stall the caller.
const syncTimeout = 10 * time.Minute

// errNoSource is the sentinel reported when no git source is configured yet.
//
// The value is exported-read-only through the sentinelErr package variable below
// so callers can detect the case with errors.Is. It is a plain error VALUE
// (never wrapped by other errors and never used as a wrapped cause's outer
// layer), so errors.Is(err, errNoSource) degenerates to an identity check — which
// is exactly the behavior we want and need to be uniform across every call site.
var errNoSource = errors.New("no git source configured; run relay git set <repository>")

// ErrConfigNotFound returns the "no git source configured" sentinel. All call
// sites MUST detect the "nothing configured" case via errors.Is(err,
// ErrConfigNotFound()) — never a bare == comparison — so that a wrapped sentinel
// (if LoadConfig ever starts wrapping) and a direct return behave identically.
// Status and Remove both go through this path; keep every check uniform.
func ErrConfigNotFound() error { return errNoSource }

// Config is the persisted sync configuration. It is small JSON written
// atomically to ConfigPath (see WriteConfig). Repository must be an SSH URL in
// production; Ref defaults to DefaultRef; Path is an optional monorepo subdir
// (empty = repo root). LastSyncedCommit/LastSyncedAt/Synced are the bookkeeping
// written by a successful Sync so Status can report when the source last
// produced /functions.
type Config struct {
	Repository       string `json:"repository"`
	Ref              string `json:"ref"`
	Path             string `json:"path,omitempty"`
	LastSyncedCommit string `json:"lastSyncedCommit,omitempty"`
	LastSyncedAt     string `json:"lastSyncedAt,omitempty"`
	Synced           bool   `json:"synced"`
	// WebhookSecretRef names the secret used to authenticate webhook requests for
	// this Git source. The value itself is resolved only when validating a request.
	WebhookSecretRef string `json:"webhookSecretRef,omitempty"`
}

// LoadConfig reads and decodes the persisted config at path. A missing file
// returns an error wrapping ErrConfigNotFound.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Missing file is the "nothing configured" state. It is wrapped (not
			// returned bare) so errors.Is(ErrConfigNotFound()) works through any
			// intermediate wrapper the caller may add, keeping detection uniform.
			return Config{}, fmt.Errorf("%w", ErrConfigNotFound())
		}
		return Config{}, fmt.Errorf("git: read config: %w", err)
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return Config{}, fmt.Errorf("git: parse config: %w", err)
	}
	if c.Ref == "" {
		// Configs written before the ref field existed, or hand-edited files,
		// fall back to the documented default so an old config still syncs.
		c.Ref = DefaultRef
	}
	return c, nil
}

// writeConfig persists c atomically to path (0700 dir, 0600 file), mirroring
// secrets/local.go's Set: the value is written to a temp file in the same
// directory, fsynced, then renamed over the target so a reader never observes a
// partial config. On any error the temp file is removed.
func writeConfig(c Config, path string) error {
	if c.Repository == "" {
		return fmt.Errorf("git: repository must not be empty")
	}
	if c.Ref == "" {
		return fmt.Errorf("git: ref must not be empty")
	}
	if err := validatePath(c.Path); err != nil {
		return err
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("git: encode config: %w", err)
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("git: create config dir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".source-*")
	if err != nil {
		return fmt.Errorf("git: create config temp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("git: chmod config temp: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("git: write config temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("git: sync config temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("git: close config temp: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("git: rename config: %w", err)
	}
	return nil
}

// SetSource validates and persists a sync source config: repository (an SSH URL),
// ref, an optional monorepo path, and an optional name of the secret holding the
// GitHub webhook secret (see Config.WebhookSecretRef). Calling it again
// overwrites (upsert). It enforces the SSH-URL rule and the monorepo-path safety
// rule so a bad value can never be persisted. The ref and path are stored exactly
// as given (the CLI defaults an omitted ref to DefaultRef; an empty path means
// repo root). Any prior last-synced bookkeeping is retained on update so an
// operator changing the ref/path keeps the last-success metadata until the next
// sync.
func SetSource(path, repository, ref, monorepoPath, webhookSecretRef string) error {
	// Preserve prior bookkeeping on an update (upsert) so status survives a
	// config change until the next sync records fresh values.
	var (
		priorSynced bool
		priorCommit string
		priorAt     string
	)
	if existing, err := LoadConfig(path); err == nil {
		priorSynced = existing.Synced
		priorCommit = existing.LastSyncedCommit
		priorAt = existing.LastSyncedAt
	}
	if err := ValidateRepositoryURL(repository); err != nil {
		return err
	}
	if err := validateRef(ref); err != nil {
		return err
	}
	if err := validatePath(monorepoPath); err != nil {
		return err
	}
	// An empty webhookSecretRef disables webhook triggering; a non-empty one
	// must be a legal secret name so the webhook server can resolve it later.
	// The value is never validated as the secret's VALUE — only its store name.
	if webhookSecretRef != "" {
		if err := secrets.ValidateName(webhookSecretRef); err != nil {
			return err
		}
	}
	if ref == "" {
		ref = DefaultRef
	}
	return writeConfig(Config{
		Repository:       repository,
		Ref:              ref,
		Path:             monorepoPath,
		Synced:           priorSynced,
		LastSyncedCommit: priorCommit,
		LastSyncedAt:     priorAt,
		WebhookSecretRef: webhookSecretRef,
	}, path)
}

// SyncOptions carries the injectable directories and transport inputs for a
// Sync. Tests redirect every path away from /var/lib/relay and /functions and
// drive the transport against a local filesystem path (CloneURL set, Auth nil).
type SyncOptions struct {
	// RepositoryURL overrides the configured repository as the remote source.
	// When set AND no Auth is provided, the SSH key is still used; it is how the
	// CLI threads the persisted URL through while tests use CloneURL instead.
	RepositoryURL string
	// CloneURL overrides the actual clone/fetch source (the remote). It is how
	// tests point sync at a local filesystem repo while the persisted config
	// still carries the configured SSH URL, and how production keeps the two
	// identical. An empty CloneURL uses RepositoryURL, then the configured value.
	CloneURL string
	// Ref overrides the configured ref. Empty uses the configured ref.
	Ref string
	// Path overrides the configured monorepo path in the same way as Ref.
	Path *string
	// Auth is the transport auth. nil is fine for local filesystem sources
	// (tests); production leaves it nil so the SSHDir key is loaded with TOFU
	// host-key verification over Relay's own known_hosts.
	Auth gitssh.AuthMethod
	// SSHDir is where the SSH deploy key lives. Defaulted by the CLI.
	SSHDir string
	// ConfigPath and CheckoutDir locate the config and checkout. Defaulted by
	// the CLI to /var/lib/relay/git/... .
	ConfigPath, CheckoutDir string
	// FunctionsDir is the root /functions must reflect. The CLI defaults it to
	// function.Dir; tests always inject a temp dir. Sync never hardcodes
	// "/functions" internally.
	FunctionsDir string
	// Log is the caller-injected logger (DI): production passes the CLI's process
	// logger (from cmd/main.go); tests pass a capturing logger or nil. nil means
	// the package is silent — it never constructs a fallback logger of its own
	// (the previous constructor fallback was removed). Step summaries still reach
	// Out independently, so Out alone works without a logger.
	Log *slog.Logger
	// Out, when non-nil, receives the tidy step summary.
	Out io.Writer
}

// NewSyncOptions returns a SyncOptions populated with the production default
// dirs (ConfigPath, CheckoutDir), which callers override the fields they need.
// SSHDir is intentionally left empty here — it is only read when sync targets an
// SSH host (production sets it); the local-test seam uses CloneURL and Auth
// instead and never needs it. Log is left nil: the package never constructs a
// logger, so callers that want logging must inject one (the CLI passes the
// process logger; tests pass a capturing logger or leave it nil for silence).
func NewSyncOptions() SyncOptions {
	return SyncOptions{
		ConfigPath:  ConfigPath,
		CheckoutDir: CheckoutDir,
	}
}
