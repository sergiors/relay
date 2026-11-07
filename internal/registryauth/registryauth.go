// Package registryauth resolves Docker registry credentials from the operator's
// Docker client configuration. It exists so `relay runtime update` can send the
// same X-Registry-Auth header to the Docker Engine that the Docker CLI would,
// without shelling out to Docker and without implementing a registry client.
//
// It supports the documented config.json fields auths (base64
// username:password), auths.identitytoken, credHelpers, and credsStore. A
// credential helper is invoked as a context-bound subprocess using the
// docker-credential-<helper> get protocol; stdout is parsed as JSON and never
// logged. No credential value is returned in an error or written to output.
//
// Relay does not read credentials from the Docker daemon or from a container's
// environment. When Relay runs in a container, the operator must mount the
// Docker client configuration and point DOCKER_CONFIG at its directory (or place
// it at $HOME/.docker/config.json); otherwise only anonymous registry access is
// available.
package registryauth

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/moby/moby/api/pkg/authconfig"
	"github.com/moby/moby/api/types/registry"
)

const (
	// dockerHubRegistry is the canonical host used by Docker image references
	// such as "node:24-alpine". Its client config key is the legacy index URL.
	dockerHubRegistry = "docker.io"
	// dockerHubConfigKey is the key Docker's client writes for Docker Hub in
	// config.json's auths map.
	dockerHubConfigKey = "https://index.docker.io/v1/"
	// dockerTokenUsername is the credential-helper sentinel that marks the
	// returned Secret as an identity token rather than a password.
	dockerTokenUsername = "<token>"
	// helperOutputLimit bounds what a credential helper can make Relay read.
	helperOutputLimit = 1 << 20
)

// Store is an immutable view of one Docker client config.json. A zero Store (or
// a nil *Store) is a valid anonymous configuration.
type Store struct {
	path        string
	auths       map[string]authEntry
	credsStore  string
	credHelpers map[string]string
}

type configFile struct {
	Auths       map[string]authEntry `json:"auths"`
	CredsStore  string               `json:"credsStore"`
	CredHelpers map[string]string    `json:"credHelpers"`
}

type authEntry struct {
	Auth          string `json:"auth"`
	IdentityToken string `json:"identitytoken"`
	Username      string `json:"username"`
	Password      string `json:"password"`
}

// Load reads the Docker client config selected by DOCKER_CONFIG (a directory),
// falling back to $HOME/.docker/config.json. A missing file is an anonymous
// configuration, not an error; a syntactically invalid or unreadable file is an
// error naming the path (never its contents).
func Load() (*Store, error) {
	path, err := defaultConfigPath()
	if err != nil {
		return nil, err
	}
	if path == "" {
		return &Store{}, nil
	}
	return LoadFile(path)
}

// LoadFile reads one Docker client config file. A missing file yields an
// anonymous Store; any other read or decode failure is returned with the path.
// Tests and embedding hosts use it to point at an isolated config.
func LoadFile(path string) (*Store, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &Store{path: path}, nil
		}
		return nil, fmt.Errorf("read Docker config %q: %w", path, err)
	}
	var parsed configFile
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("parse Docker config %q: %w", path, err)
	}
	return &Store{
		path:        path,
		auths:       parsed.Auths,
		credsStore:  parsed.CredsStore,
		credHelpers: parsed.CredHelpers,
	}, nil
}

// defaultConfigPath returns the Docker client config path, or "" when no home
// directory is available. DOCKER_CONFIG is interpreted as a directory, exactly
// like the Docker CLI (not as a file path).
func defaultConfigPath() (string, error) {
	if dir := os.Getenv("DOCKER_CONFIG"); dir != "" {
		return filepath.Join(dir, "config.json"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		// No resolvable home means no default client config. Anonymous access
		// remains available, so this is not an update failure.
		return "", nil
	}
	return filepath.Join(home, ".docker", "config.json"), nil
}

// EncodedAuthFor returns the X-Registry-Auth value for imageRef, or "" when no
// credentials are configured (anonymous access). A configured credential helper
// takes precedence over auths, matching the Docker CLI. The returned string
// contains a credential secret and must be treated as sensitive: Relay passes
// it only to the Docker Engine and never prints or logs it.
func (s *Store) EncodedAuthFor(ctx context.Context, imageRef string) (string, error) {
	if s == nil {
		return "", nil
	}
	host, serverAddress := registryForImage(imageRef)
	helper := s.helperFor(host)
	if helper != "" {
		username, secret, err := runCredentialHelper(ctx, helper, serverAddress)
		if err != nil {
			return "", fmt.Errorf("Docker credential helper %q for %q: %w", helper, host, err)
		}
		if username != "" || secret != "" {
			if username == dockerTokenUsername {
				return encode(registry.AuthConfig{
					IdentityToken: secret,
					ServerAddress: serverAddress,
				})
			}
			return encode(registry.AuthConfig{
				Username:      username,
				Password:      secret,
				ServerAddress: serverAddress,
			})
		}
	}
	entry, ok := s.authEntryFor(host)
	if !ok {
		return "", nil
	}
	if entry.IdentityToken != "" {
		return encode(registry.AuthConfig{
			IdentityToken: entry.IdentityToken,
			ServerAddress: serverAddress,
		})
	}
	if entry.Auth != "" {
		username, password, err := decodeAuth(entry.Auth)
		if err != nil {
			return "", fmt.Errorf("Docker config auth for %q: %w", host, err)
		}
		return encode(registry.AuthConfig{
			Username:      username,
			Password:      password,
			ServerAddress: serverAddress,
		})
	}
	if entry.Username != "" || entry.Password != "" {
		return encode(registry.AuthConfig{
			Username:      entry.Username,
			Password:      entry.Password,
			ServerAddress: serverAddress,
		})
	}
	return "", nil
}

func encode(auth registry.AuthConfig) (string, error) {
	encoded, err := authconfig.Encode(auth)
	if err != nil {
		return "", fmt.Errorf("encode Docker registry auth: %w", err)
	}
	return encoded, nil
}

func (s *Store) helperFor(host string) string {
	if host == "" || s == nil {
		return ""
	}
	if helper := strings.TrimSpace(s.credHelpers[host]); helper != "" {
		return helper
	}
	// Docker's config uses the canonical Docker Hub key in credHelpers. Accept
	// the legacy key too so a config written by an older client still works.
	if host == dockerHubRegistry {
		if helper := strings.TrimSpace(s.credHelpers[dockerHubConfigKey]); helper != "" {
			return helper
		}
	}
	return strings.TrimSpace(s.credsStore)
}

func (s *Store) authEntryFor(host string) (authEntry, bool) {
	if s == nil || len(s.auths) == 0 {
		return authEntry{}, false
	}
	for _, key := range authKeysFor(host) {
		if entry, ok := s.auths[key]; ok {
			return entry, true
		}
	}
	// Docker config keys are usually exact; a case-insensitive fallback makes a
	// host written in a different case still usable.
	lower := strings.ToLower(host)
	for key, entry := range s.auths {
		if strings.ToLower(strings.TrimSuffix(key, "/")) == lower {
			return entry, true
		}
	}
	return authEntry{}, false
}

// authKeysFor returns the config.json auths keys that may hold credentials for
// host, most specific first. Docker Hub has the historical index URL key; other
// registries normally use the bare host, with URL forms accepted as a courtesy.
func authKeysFor(host string) []string {
	if host == dockerHubRegistry {
		return []string{
			dockerHubConfigKey,
			"index.docker.io",
			"registry-1.docker.io",
			dockerHubRegistry,
			"https://" + dockerHubRegistry + "/v1/",
		}
	}
	return []string{
		host,
		"https://" + host,
		"https://" + host + "/v1/",
	}
}

// registryForImage derives the registry host and the server address used for
// auth lookup from an image reference. An unqualified reference (including one
// with a namespace such as "acme/app") is Docker Hub. A first path component
// containing a dot, a colon, or equal to localhost is a registry host.
func registryForImage(imageRef string) (host, serverAddress string) {
	first, _, hasSlash := strings.Cut(imageRef, "/")
	switch {
	case hasSlash && (strings.ContainsAny(first, ".:") || first == "localhost"):
		host = first
	default:
		host = dockerHubRegistry
	}
	if host == dockerHubRegistry {
		return dockerHubRegistry, dockerHubConfigKey
	}
	return host, host
}

func decodeAuth(encoded string) (string, string, error) {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		// Some clients omit padding; Docker accepts the unpadded form too.
		decoded, err = base64.RawStdEncoding.DecodeString(encoded)
		if err != nil {
			return "", "", fmt.Errorf("auth is not valid base64")
		}
	}
	username, password, ok := strings.Cut(string(decoded), ":")
	if !ok {
		return "", "", fmt.Errorf("auth is not username:password")
	}
	return username, password, nil
}

// helperResult is the response of `docker-credential-<helper> get`.
type helperResult struct {
	Username string `json:"Username"`
	Secret   string `json:"Secret"`
}

// boundedOutput keeps at most limit bytes of helper stdout while reporting every
// write as successful, so os/exec keeps draining the pipe instead of blocking
// the child. Bytes beyond the limit are discarded rather than buffered; overflow
// is reported only after the helper exits.
type boundedOutput struct {
	buf      bytes.Buffer
	limit    int
	overflow bool
}

func (w *boundedOutput) Write(p []byte) (int, error) {
	if !w.overflow {
		remaining := w.limit - w.buf.Len()
		if len(p) > remaining {
			if remaining > 0 {
				_, _ = w.buf.Write(p[:remaining])
			}
			w.overflow = true
		} else {
			_, _ = w.buf.Write(p)
		}
	}
	return len(p), nil
}

func runCredentialHelper(ctx context.Context, helper, serverAddress string) (string, string, error) {
	if strings.TrimSpace(helper) == "" {
		return "", "", nil
	}
	if strings.ContainsAny(helper, `/\`) {
		return "", "", fmt.Errorf("invalid credential helper name")
	}
	cmd := exec.CommandContext(ctx, "docker-credential-"+helper, "get")
	cmd.Stdin = strings.NewReader(serverAddress)
	stdout := &boundedOutput{limit: helperOutputLimit}
	cmd.Stdout = stdout
	// stderr is deliberately discarded: a helper may echo credentials or config
	// details, and this error is operator-facing. Exit status is enough.
	cmd.Stderr = nil
	runErr := cmd.Run()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return "", "", ctxErr
	}
	if stdout.overflow {
		return "", "", fmt.Errorf("helper output exceeded %d bytes", helperOutputLimit)
	}
	if runErr != nil {
		return "", "", fmt.Errorf("helper exited unsuccessfully")
	}
	var result helperResult
	if err := json.Unmarshal(stdout.buf.Bytes(), &result); err != nil {
		return "", "", fmt.Errorf("helper returned invalid JSON")
	}
	return result.Username, result.Secret, nil
}
