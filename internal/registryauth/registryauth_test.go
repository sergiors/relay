package registryauth

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/authconfig"
)

// This file pins the Docker client-config credential resolution in isolation:
// temp config.json files and fake docker-credential-<helper> shell scripts, so
// no real credential store, network, or Docker daemon is involved. Assertions
// about secrets always assert that they do NOT leak into an error.

// configFixture writes a config.json into a fresh directory and returns the
// file path. content must be valid or invalid JSON exactly as the scenario
// needs it.
func configFixture(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config fixture: %v", err)
	}
	return path
}

// basicAuthField renders the config.json "auth" field for user:secret.
func basicAuthField(username, secret string) string {
	return base64.StdEncoding.EncodeToString([]byte(username + ":" + secret))
}

// headerAuth decodes a returned X-Registry-Auth value through the production
// decoder, so assertions observe exactly what the Docker Engine would see.
func headerAuth(
	t *testing.T, encoded string,
) (username, password, identityToken, serverAddress string) {
	t.Helper()
	auth, err := authconfig.Decode(encoded)
	if err != nil {
		t.Fatalf("decode returned X-Registry-Auth header: %v", err)
	}
	return auth.Username, auth.Password, auth.IdentityToken, auth.ServerAddress
}

// mustLoad is LoadFile with a hard failure, for tests whose config must parse.
func mustLoad(t *testing.T, path string) *Store {
	t.Helper()
	store, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile(%s): %v", path, err)
	}
	return store
}

// helperFixture writes a fake docker-credential-<name> script into a unique
// directory, prepends that directory to PATH for the test, and returns the file
// path the helper can write its stdin (the server address) to. A body may
// reference it as @RECORD@; helpers that do not echo stdin simply ignore it.
func helperFixture(t *testing.T, name, body string) string {
	t.Helper()
	record := filepath.Join(t.TempDir(), "helper-stdin")
	body = strings.ReplaceAll(body, "@RECORD@", record)
	bin := filepath.Join(t.TempDir(), "docker-credential-"+name)
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatalf("write helper script: %v", err)
	}
	t.Setenv("PATH", filepath.Dir(bin)+string(os.PathListSeparator)+os.Getenv("PATH"))
	return record
}

// helperCredsBody answers get with fixed helper credentials and echoes its
// stdin (the server address) to the record file the fixture supplies.
func helperCredsBody() string {
	body := "cat > '@RECORD@'\n"
	body += `printf '{"Username":"helper-user","Secret":"helper-secret"}'`
	return body
}

// TestMissingConfigFileIsAnonymous pins the honest default: no config file (the
// common case for an anonymous Docker setup) is a valid anonymous store whose
// EncodedAuthFor resolves to NO header at all.
func TestMissingConfigFileIsAnonymous(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile(missing): %v", err)
	}
	encoded, err := store.EncodedAuthFor(context.Background(), "node:24-alpine")
	if err != nil {
		t.Fatalf("EncodedAuthFor: %v", err)
	}
	if encoded != "" {
		t.Fatalf("anonymous store returned header %q, want none", encoded)
	}
}

// TestMalformedConfigFailsNamingThePath pins the invalid-config failure: a
// syntactically invalid file is an error naming the path, never a silent
// anonymous fallback and never a leak of the file's bytes.
func TestMalformedConfigFailsNamingThePath(t *testing.T) {
	path := configFixture(t, `{"auths": {`)
	_, err := LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), "parse Docker config") {
		t.Fatalf("malformed config err = %v, want a parse error", err)
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("error %v does not name the config path %q", err, path)
	}
}

// TestUnreadableConfigFailsNamingThePath pins the unreadable-config failure:
// pointing LoadFile at a directory surfaces a read error naming the path.
func TestUnreadableConfigFailsNamingThePath(t *testing.T) {
	path := t.TempDir()
	_, err := LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), "read Docker config") {
		t.Fatalf("dir path err = %v, want a read error", err)
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("error %v does not name the path %q", err, path)
	}
}

// TestNilStoreIsAnonymous pins the zero-value contract of the package: a nil
// *Store is an anonymous configuration and never panics.
func TestNilStoreIsAnonymous(t *testing.T) {
	var store *Store
	encoded, err := store.EncodedAuthFor(context.Background(), "python:3.14-slim")
	if err != nil {
		t.Fatalf("nil store EncodedAuthFor: %v", err)
	}
	if encoded != "" {
		t.Fatalf("nil store returned header %q, want none", encoded)
	}
}

// TestHubAuthsUnderLegacyIndexKey pins Docker Hub key resolution: credentials
// written under the historical "https://index.docker.io/v1/" key authenticate
// unqualified references such as "node:24-alpine" (namespace or not), and the
// encoded header carries the decoded username/password for the legacy server
// address the Docker CLI itself uses.
func TestHubAuthsUnderLegacyIndexKey(t *testing.T) {
	store := mustLoad(t, configFixture(t, `{
		"auths": {
			"https://index.docker.io/v1/": {"auth": "`+basicAuthField("hub-user", "hub-secret")+`"}
		}
	}`))

	for _, ref := range []string{"node:24-alpine", "acme/app:v1"} {
		encoded, err := store.EncodedAuthFor(context.Background(), ref)
		if err != nil {
			t.Fatalf("%s: EncodedAuthFor: %v", ref, err)
		}
		if encoded == "" {
			t.Fatalf("%s: no header for the configured Docker Hub auths entry", ref)
		}
		user, pass, _, server := headerAuth(t, encoded)
		if user != "hub-user" || pass != "hub-secret" {
			t.Fatalf("%s: decoded credentials = %q/%q, want the config entry", ref, user, pass)
		}
		if server != "https://index.docker.io/v1/" {
			t.Fatalf("%s: server address = %q, want the legacy Docker Hub key", ref, server)
		}
	}
}

// TestOtherRegistryKeysFormsAndCase pins auths-key resolution for non-Hub
// hosts: bare host, URL forms, localhost:port, and (as the documented
// tolerance) a differently-cased key are all found by host lookup.
func TestOtherRegistryKeysFormsAndCase(t *testing.T) {
	cases := []struct {
		name       string
		configKey  string
		imageRef   string
		wantServer string
	}{
		{
			name:       "bare host key",
			configKey:  "registry.example.com",
			imageRef:   "registry.example.com/app:v1",
			wantServer: "registry.example.com",
		},
		{
			name:       "https URL key",
			configKey:  "https://registry.example.com",
			imageRef:   "registry.example.com/app:v1",
			wantServer: "registry.example.com",
		},
		{
			name:       "https URL with v1 suffix key",
			configKey:  "https://registry.example.com/v1/",
			imageRef:   "registry.example.com/app:v1",
			wantServer: "registry.example.com",
		},
		{
			name:       "localhost with port is a registry host",
			configKey:  "localhost:5001",
			imageRef:   "localhost:5001/app:v1",
			wantServer: "localhost:5001",
		},
		{
			name:       "differently-cased key matches the host",
			configKey:  "REGISTRY.Example.COM",
			imageRef:   "registry.example.com/app:v1",
			wantServer: "registry.example.com",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := mustLoad(t, configFixture(t, `{
				"auths": {
					"`+tc.configKey+`": {"auth": "`+basicAuthField("key-user", "key-secret")+`"}
				}
			}`))
			encoded, err := store.EncodedAuthFor(context.Background(), tc.imageRef)
			if err != nil || encoded == "" {
				t.Fatalf("EncodedAuthFor = (%q, %v), want a header", encoded, err)
			}
			user, pass, _, server := headerAuth(t, encoded)
			if user != "key-user" || pass != "key-secret" {
				t.Fatalf("decoded credentials = %q/%q, want the config entry", user, pass)
			}
			if server != tc.wantServer {
				t.Fatalf("server address = %q, want %q", server, tc.wantServer)
			}
		})
	}
}

// TestIdentityTokenTakesPrecedenceOverAuth pins the token contract: an entry
// carrying both an identitytoken and an auth field encodes ONLY the identity
// token — the Engine receives an identity-token header, not basic credentials.
func TestIdentityTokenTakesPrecedenceOverAuth(t *testing.T) {
	store := mustLoad(t, configFixture(t, `{
		"auths": {
			"registry.example.com": {
				"auth": "`+basicAuthField("auth-user", "auth-secret")+`",
				"identitytoken": "identity-token-value"
			}
		}
	}`))
	encoded, err := store.EncodedAuthFor(context.Background(), "registry.example.com/app:v1")
	if err != nil || encoded == "" {
		t.Fatalf("identity token resolution = (%q, %v)", encoded, err)
	}
	_, pass, token, server := headerAuth(t, encoded)
	if token != "identity-token-value" {
		t.Fatalf("identity token = %q, want the config token", token)
	}
	if pass != "" {
		t.Fatalf("header carries password %q alongside an identity token", pass)
	}
	if server != "registry.example.com" {
		t.Fatalf("server address = %q, want the host", server)
	}
}

// TestPlaintextUsernamePasswordEntry pins the unencoded entry form newer
// Docker clients write: username/password fields are encoded directly.
func TestPlaintextUsernamePasswordEntry(t *testing.T) {
	store := mustLoad(t, configFixture(t, `{
		"auths": {
			"registry.example.com": {"username": "plain-user", "password": "plain-secret"}
		}
	}`))
	encoded, err := store.EncodedAuthFor(context.Background(), "registry.example.com/app:v1")
	if err != nil || encoded == "" {
		t.Fatalf("plaintext entry resolution = (%q, %v)", encoded, err)
	}
	user, pass, _, _ := headerAuth(t, encoded)
	if user != "plain-user" || pass != "plain-secret" {
		t.Fatalf("decoded credentials = %q/%q, want the plaintext entry", user, pass)
	}
}

// TestInvalidAuthFieldErrorDoesNotLeakTheEntry pins redaction for the decode
// failure path: an auth field that is not valid base64 produces an error that
// names the host but never echoes the stored value.
func TestInvalidAuthFieldErrorDoesNotLeakTheEntry(t *testing.T) {
	store := mustLoad(t, configFixture(t, `{
		"auths": {
			"registry.example.com": {"auth": "!@not base64@!"}
		}
	}`))
	_, err := store.EncodedAuthFor(context.Background(), "registry.example.com/app:v1")
	if err == nil {
		t.Fatal("invalid auth field was accepted")
	}
	if !strings.Contains(err.Error(), `Docker config auth for "registry.example.com"`) {
		t.Fatalf("error %v does not name the host context", err)
	}
	if strings.Contains(err.Error(), "!@not base64@!") {
		t.Fatalf("error %v leaks the stored config value", err)
	}
}

// TestCredentialHelperTakesPrecedenceOverAuths pins the documented Docker
// precedence: with BOTH a credHelpers entry and an auths entry for the host,
// the helper runs, receives the server address on stdin, and wins.
func TestCredentialHelperTakesPrecedenceOverAuths(t *testing.T) {
	record := helperFixture(t, "precedence", helperCredsBody())

	store := mustLoad(t, configFixture(t, `{
		"auths": {
			"registry.example.com": {"auth": "`+basicAuthField("auth-user", "auth-secret")+`"}
		},
		"credHelpers": {"registry.example.com": "precedence"}
	}`))
	encoded, err := store.EncodedAuthFor(context.Background(), "registry.example.com/app:v1")
	if err != nil || encoded == "" {
		t.Fatalf("helper resolution = (%q, %v)", encoded, err)
	}
	user, pass, _, server := headerAuth(t, encoded)
	if user != "helper-user" || pass != "helper-secret" {
		t.Fatalf("decoded credentials = %q/%q, want the helper's answer", user, pass)
	}
	if server != "registry.example.com" {
		t.Fatalf("server address = %q, want the host", server)
	}
	echo, readErr := os.ReadFile(record)
	if readErr != nil {
		t.Fatalf("read helper stdin echo: %v", readErr)
	}
	if string(echo) != "registry.example.com" {
		t.Fatalf("helper stdin = %q, want the server address", string(echo))
	}
}

// TestHubCredHelperReceivesLegacyServerAddress pins the helper protocol for
// Docker Hub: the helper is invoked with the same legacy index URL server
// address the Docker CLI would use, for a credHelpers key written under the
// modern "docker.io" host.
func TestHubCredHelperReceivesLegacyServerAddress(t *testing.T) {
	record := helperFixture(t, "hubhelper", helperCredsBody())

	store := mustLoad(t, configFixture(t, `{
		"credHelpers": {"docker.io": "hubhelper"}
	}`))
	encoded, err := store.EncodedAuthFor(context.Background(), "node:24-alpine")
	if err != nil || encoded == "" {
		t.Fatalf("hub helper resolution = (%q, %v)", encoded, err)
	}
	user, _, _, _ := headerAuth(t, encoded)
	if user != "helper-user" {
		t.Fatalf("helper credentials not used: user %q", user)
	}
	echo, readErr := os.ReadFile(record)
	if readErr != nil {
		t.Fatalf("read helper stdin echo: %v", readErr)
	}
	if string(echo) != "https://index.docker.io/v1/" {
		t.Fatalf("helper stdin = %q, want the legacy Docker Hub server address", string(echo))
	}
}

// TestLegacyHubKeyInCredHelpersStillRoutes pins the tolerance for configs
// written by an older client: a credHelpers entry under the legacy
// "https://index.docker.io/v1/" key serves Docker Hub references too.
func TestLegacyHubKeyInCredHelpersStillRoutes(t *testing.T) {
	record := helperFixture(t, "legacyhelper", helperCredsBody())

	store := mustLoad(t, configFixture(t, `{
		"credHelpers": {"https://index.docker.io/v1/": "legacyhelper"}
	}`))
	encoded, err := store.EncodedAuthFor(context.Background(), "acme/app:v1")
	if err != nil || encoded == "" {
		t.Fatalf("legacy hub helper resolution = (%q, %v)", encoded, err)
	}
	user, _, _, _ := headerAuth(t, encoded)
	if user != "helper-user" {
		t.Fatalf("legacy-key helper not used for a Hub ref: user %q", user)
	}
	echo, readErr := os.ReadFile(record)
	if readErr != nil || string(echo) != "https://index.docker.io/v1/" {
		t.Fatalf("legacy helper stdin echo = (%q, %v), want the hub server address", echo, readErr)
	}
}

// TestHelperIdentityTokenSentinelEncodesAsIdentityToken pins the Docker
// credential-helper identity-token convention: a helper answering with the
// "<token>" sentinel username marks its Secret as an IDENTITY TOKEN, so the
// encoded X-Registry-Auth value carries IdentityToken (NO Username/Password
// fields and no legacy auth field) and preserves the server address. The
// encoded value is base64, so the assertion also proves the raw token never
// appears in plaintext within the header value.
func TestHelperIdentityTokenSentinelEncodesAsIdentityToken(t *testing.T) {
	const secretToken = "identity-token-secret-value"
	record := helperFixture(t, "tokenhelper",
		`cat > '@RECORD@'
printf '{"Username":"`+dockerTokenUsername+`","Secret":"`+secretToken+`"}'`)
	_ = record

	store := mustLoad(t, configFixture(t, `{
		"credHelpers": {"registry.example.com": "tokenhelper"}
	}`))
	encoded, err := store.EncodedAuthFor(context.Background(), "registry.example.com/app:v1")
	if err != nil || encoded == "" {
		t.Fatalf("identity-token helper resolution = (%q, %v), want a header", encoded, err)
	}

	// The encoded header value must never carry the token in plaintext; the
	// marker's hyphens cannot occur in base64, so this is a hard guarantee.
	if strings.Contains(encoded, secretToken) {
		t.Fatalf("encoded header %q contains the plaintext identity token", encoded)
	}

	user, pass, token, server := headerAuth(t, encoded)
	if token != secretToken {
		t.Fatalf("decoded identity token = %q, want the helper's Secret", token)
	}
	if user != "" || pass != "" {
		t.Fatalf("sentinel helper output encoded as Username=%q Password=%q; the Secret must be an IdentityToken", user, pass)
	}
	if server != "registry.example.com" {
		t.Fatalf("server address = %q, want the registry host preserved", server)
	}
}

// TestCredHelpersOverrideGlobalStore pins per-host routing when a global
// credsStore is configured: the host-specific helper serves that host, the
// store serves Docker Hub, and the store helper receives the hub server
// address.
func TestCredHelpersOverrideGlobalStore(t *testing.T) {
	record := helperFixture(t, "hosthelper", helperCredsBody())
	storeRecord := helperFixture(t, "storehelper", helperCredsBody())

	store := mustLoad(t, configFixture(t, `{
		"credsStore": "storehelper",
		"credHelpers": {"registry.example.com": "hosthelper"}
	}`))

	// The host helper answers for its host, not the global store.
	encoded, err := store.EncodedAuthFor(context.Background(), "registry.example.com/app:v1")
	if err != nil || encoded == "" {
		t.Fatalf("host helper resolution = (%q, %v)", encoded, err)
	}
	echo, readErr := os.ReadFile(record)
	if readErr != nil {
		t.Fatalf("read host helper stdin echo: %v", readErr)
	}
	if string(echo) != "registry.example.com" {
		t.Fatalf("host helper stdin = %q, want the host server address", string(echo))
	}

	// Docker Hub goes through the global store.
	encoded, err = store.EncodedAuthFor(context.Background(), "node:24-alpine")
	if err != nil || encoded == "" {
		t.Fatalf("store resolution for hub = (%q, %v)", encoded, err)
	}
	storeEcho, storeErr := os.ReadFile(storeRecord)
	if storeErr != nil {
		t.Fatalf("read store helper stdin echo: %v", storeErr)
	}
	if string(storeEcho) != "https://index.docker.io/v1/" {
		t.Fatalf("store helper stdin = %q, want the hub server address", string(storeEcho))
	}
}

// TestHelperFailureIsRedacted pins the helper failure path: the subprocess
// exits nonzero, its stderr (which in this scenario prints a credential) is
// discarded, and the returned error names the helper and host but never the
// secret.
func TestHelperFailureIsRedacted(t *testing.T) {
	record := helperFixture(t, "broken", `
printf 'SUPER-SECRET-VALUE' >&2
exit 1`)
	_ = record

	store := mustLoad(t, configFixture(t, `{
		"credHelpers": {"registry.example.com": "broken"}
	}`))
	_, err := store.EncodedAuthFor(context.Background(), "registry.example.com/app:v1")
	if err == nil {
		t.Fatal("failing helper was treated as success")
	}
	if !strings.Contains(err.Error(), `Docker credential helper "broken"`) ||
		!strings.Contains(err.Error(), `"registry.example.com"`) {
		t.Fatalf("error %v does not name the helper and host", err)
	}
	if !strings.Contains(err.Error(), "helper exited unsuccessfully") {
		t.Fatalf("error %v does not report the helper's exit failure", err)
	}
	if strings.Contains(err.Error(), "SUPER-SECRET-VALUE") {
		t.Fatalf("error %v leaks the helper's stderr", err)
	}
}

// TestHelperInvalidJSONIsRedacted pins the helper-output contract: output that
// is not the get-protocol JSON fails resolution with a message that never
// includes the raw helper stdout (which carries the "secret" here).
func TestHelperInvalidJSONIsRedacted(t *testing.T) {
	record := helperFixture(t, "noisy", `
printf 'SUPER-SECRET-VALUE'`)
	_ = record

	store := mustLoad(t, configFixture(t, `{
		"credHelpers": {"registry.example.com": "noisy"}
	}`))
	_, err := store.EncodedAuthFor(context.Background(), "registry.example.com/app:v1")
	if err == nil {
		t.Fatal("non-JSON helper output was accepted")
	}
	if !strings.Contains(err.Error(), "helper returned invalid JSON") {
		t.Fatalf("error %v does not report the invalid JSON", err)
	}
	if strings.Contains(err.Error(), "SUPER-SECRET-VALUE") {
		t.Fatalf("error %v leaks the helper's stdout", err)
	}
}

// TestHelperOversizeOutputIsDrainedAndRedacted pins the bounded-stdout
// contract: a helper that emits well over helperOutputLimit bytes has its
// output Drained to completion (the exec pipe consumer never blocks the child),
// the overflow becomes a SANITIZED size-limit error naming only the helper and
// host, and no byte of the helper's stdout — the stored bounded prefix, the
// discarded overflow region, or anything else — is echoed back. The helper's
// completion marker proves the subprocess ran to normal exit rather than being
// starved or killed mid-write.
func TestHelperOversizeOutputIsDrainedAndRedacted(t *testing.T) {
	doneRecord := helperFixture(t, "flood", `cat > /dev/null
printf 'HEAD-SECRET-MARKER'
dd if=/dev/zero bs=1048576 count=2 2>/dev/null
printf 'TAIL-SECRET-MARKER'
echo done > '@RECORD@'`)

	store := mustLoad(t, configFixture(t, `{
		"credHelpers": {"registry.example.com": "flood"}
	}`))
	encoded, err := store.EncodedAuthFor(context.Background(), "registry.example.com/app:v1")
	if encoded != "" {
		t.Fatalf("oversize helper resolution returned header %q alongside an error", encoded)
	}
	if err == nil {
		t.Fatal("oversize helper output was accepted as credentials")
	}
	wantLimit := fmt.Sprintf("helper output exceeded %d bytes", helperOutputLimit)
	if !strings.Contains(err.Error(), wantLimit) {
		t.Fatalf("oversize err = %v, want the size-limit error %q", err, wantLimit)
	}
	if !strings.Contains(err.Error(), `Docker credential helper "flood"`) ||
		!strings.Contains(err.Error(), `"registry.example.com"`) {
		t.Fatalf("error %v does not name the helper and host", err)
	}
	if strings.Contains(err.Error(), "helper exited unsuccessfully") ||
		strings.Contains(err.Error(), "invalid JSON") {
		t.Fatalf("err = %v, want the overflow path, not an exit/parse failure", err)
	}
	for _, leaked := range []string{"HEAD-SECRET-MARKER", "TAIL-SECRET-MARKER"} {
		if strings.Contains(err.Error(), leaked) {
			t.Fatalf("error %v echoes helper output (%q)", err, leaked)
		}
	}
	// Guard the whole surface, not just the markers: the sanitized error is a
	// short fixed-format message, so any echo of a megabyte of helper output
	// (markers or filler) would blow past this bound with ease.
	if len(err.Error()) > 512 {
		t.Fatalf("error is %d bytes long; the sanitized overflow message must stay short", len(err.Error()))
	}
	// The parent drained every byte: the helper reached its completion marker
	// (a broken or missing drain would have starved or killed the child).
	completed, readErr := os.ReadFile(doneRecord)
	if readErr != nil {
		t.Fatalf("the helper never completed after the oversize output: %v", readErr)
	}
	if !strings.Contains(string(completed), "done") {
		t.Fatalf("helper completion marker = %q, want \"done\"", string(completed))
	}
}

// TestMissingHelperBinaryFailsClosed pins the missing-helper path: a helper
// name configured but not installed fails resolution (naming the helper) — it
// never silently falls back to anonymous or other credentials.
func TestMissingHelperBinaryFailsClosed(t *testing.T) {
	record := helperFixture(t, "unused", "")
	_ = record

	store := mustLoad(t, configFixture(t, `{
		"credHelpers": {"registry.example.com": "notinstalled"}
	}`))
	_, err := store.EncodedAuthFor(context.Background(), "registry.example.com/app:v1")
	if err == nil {
		t.Fatal("missing helper was treated as success")
	}
	if !strings.Contains(err.Error(), `"notinstalled"`) {
		t.Fatalf("error %v does not name the missing helper", err)
	}
}

// TestHelperNameWithPathIsRejected pins the helper-name safety rule: a helper
// name containing a slash (which would escape the docker-credential- prefix
// convention) is rejected before any subprocess runs.
func TestHelperNameWithPathIsRejected(t *testing.T) {
	store := mustLoad(t, configFixture(t, `{
		"credHelpers": {"registry.example.com": "sub/dir"}
	}`))
	_, err := store.EncodedAuthFor(context.Background(), "registry.example.com/app:v1")
	if err == nil || !strings.Contains(err.Error(), "invalid credential helper name") {
		t.Fatalf("path-like helper name err = %v, want the invalid-name rejection", err)
	}
}

// TestHelperEmptyResultFallsBackToAuths pins the fallback contract: a helper
// that answers with no credentials (an empty JSON object) does not shadow the
// configured auths entry for the same host.
func TestHelperEmptyResultFallsBackToAuths(t *testing.T) {
	record := helperFixture(t, "empty", `
cat > '@RECORD@'
printf '{}'`)
	_ = record

	store := mustLoad(t, configFixture(t, `{
		"auths": {
			"registry.example.com": {"auth": "`+basicAuthField("auth-user", "auth-secret")+`"}
		},
		"credHelpers": {"registry.example.com": "empty"}
	}`))
	encoded, err := store.EncodedAuthFor(context.Background(), "registry.example.com/app:v1")
	if err != nil || encoded == "" {
		t.Fatalf("fallback resolution = (%q, %v)", encoded, err)
	}
	user, pass, _, _ := headerAuth(t, encoded)
	if user != "auth-user" || pass != "auth-secret" {
		t.Fatalf("fallback credentials = %q/%q, want the auths entry", user, pass)
	}
}

// TestHelperRespectsContextCancellation pins that the helper subprocess is
// context-bound: a helper that never finishes is abandoned when the caller's
// deadline expires, and the failure is the cancellation. The fixture uses
// `exec sleep 30` so the helper PROCESS ITSELF is replaced by the sleeper: the
// command kills the actual sleeper at cancellation, no grandchild can hold the
// inherited stdout/stderr pipes open, and Run returns promptly at the deadline.
func TestHelperRespectsContextCancellation(t *testing.T) {
	record := helperFixture(t, "hung", "exec sleep 30")
	_ = record

	store := mustLoad(t, configFixture(t, `{
		"credHelpers": {"registry.example.com": "hung"}
	}`))
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := store.EncodedAuthFor(ctx, "registry.example.com/app:v1")
	if err == nil {
		t.Fatal("hung helper was treated as success")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("helper cancellation error = %v, want the context cancellation", err)
	}
	// The replaced process never emitted the late credentials: the failure
	// name must not carry them either.
	if strings.Contains(err.Error(), "late") {
		t.Fatalf("cancellation error echoes the helper's late credentials: %v", err)
	}
}

// TestLoadUsesDOCKERConfigDir pins the documented DOCKER_CONFIG form: it names
// a DIRECTORY containing config.json, exactly like the Docker CLI.
func TestLoadUsesDOCKERConfigDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{
		"auths": {"registry.example.com": {"username": "env-user", "password": "env-secret"}}
	}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("DOCKER_CONFIG", dir)

	store, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	encoded, err := store.EncodedAuthFor(context.Background(), "registry.example.com/app:v1")
	if err != nil || encoded == "" {
		t.Fatalf("DOCKER_CONFIG resolution = (%q, %v)", encoded, err)
	}
	user, _, _, _ := headerAuth(t, encoded)
	if user != "env-user" {
		t.Fatalf("user = %q, want the DOCKER_CONFIG entry", user)
	}
}

// TestLoadWithoutAHomeIsAnonymous pins the no-home tolerance: without
// DOCKER_CONFIG or a resolvable home directory, Load returns an anonymous
// configuration instead of an error.
func TestLoadWithoutAHomeIsAnonymous(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", "")
	t.Setenv("HOME", filepath.Join(t.TempDir(), "missing-home"))

	store, err := Load()
	if err != nil {
		t.Fatalf("Load with no resolvable home: %v", err)
	}
	encoded, err := store.EncodedAuthFor(context.Background(), "node:24-alpine")
	if err != nil || encoded != "" {
		t.Fatalf("anonymous home resolution = (%q, %v), want none", encoded, err)
	}
}

// TestLoadReadsHomeDockerConfig pins the default discovery: without
// DOCKER_CONFIG, $HOME/.docker/config.json is used.
func TestLoadReadsHomeDockerConfig(t *testing.T) {
	home := t.TempDir()
	if err := os.MkdirAll(filepath.Join(home, ".docker"), 0o755); err != nil {
		t.Fatalf("mkdir .docker: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".docker", "config.json"), []byte(`{
		"auths": {"registry.example.com": {"username": "home-user", "password": "home-secret"}}
	}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	t.Setenv("DOCKER_CONFIG", "")
	t.Setenv("HOME", home)

	store, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	encoded, err := store.EncodedAuthFor(context.Background(), "registry.example.com/app:v1")
	if err != nil || encoded == "" {
		t.Fatalf("home config resolution = (%q, %v)", encoded, err)
	}
	user, _, _, _ := headerAuth(t, encoded)
	if user != "home-user" {
		t.Fatalf("user = %q, want the $HOME/.docker entry", user)
	}
}
