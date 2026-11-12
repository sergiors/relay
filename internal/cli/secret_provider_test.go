package cli

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"relay/internal/secrets"
)

// clearSecretsEnv forces every provider-selection variable to a truly-UNSET
// state so ambient operator configuration on the test machine can never change
// what a provider-aware secret test observes (t.Setenv registers restoration,
// mirroring the config package's helper of the same purpose).
func clearSecretsEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"SECRETS_PROVIDER",
		"INFISICAL_SITE_URL",
		"INFISICAL_PROJECT_ID",
		"INFISICAL_ENVIRONMENT",
		"INFISICAL_SECRET_PATH",
		"INFISICAL_CLIENT_ID",
		"INFISICAL_CLIENT_SECRET",
	} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}
}

// pinLocalSecretsProvider pins the documented local default so the shared
// secret fixtures stay hermetic against an ambient SECRETS_PROVIDER. Tests
// that exercise another provider call their fixture first, then set their own
// provider environment afterwards.
func pinLocalSecretsProvider(t *testing.T) {
	t.Helper()
	t.Setenv("SECRETS_PROVIDER", "")
}

// setInfisicalEnv pins a complete, valid Infisical configuration; the site
// URL and secret path stay unset so they resolve to their defaults, leaving
// each test free to vary exactly the variable it exercises.
func setInfisicalEnv(t *testing.T) {
	t.Helper()
	clearSecretsEnv(t)
	t.Setenv("SECRETS_PROVIDER", "infisical")
	t.Setenv("INFISICAL_PROJECT_ID", "proj-relay-tests")
	t.Setenv("INFISICAL_ENVIRONMENT", "staging")
	t.Setenv("INFISICAL_CLIENT_ID", "universal-auth-client-id")
	t.Setenv("INFISICAL_CLIENT_SECRET", "universal-auth-client-secret")
}

// unsetSecretsEnvVar clears one provider variable after setInfisicalEnv has
// pinned the rest, for incomplete-configuration scenarios.
func unsetSecretsEnvVar(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	os.Unsetenv(key)
}

// fakeListingProvider implements both provider seams the CLI sees: List
// returns the scripted names or error, and Resolve is never exercised by
// `secret ls`.
type fakeListingProvider struct {
	names []string
	err   error
}

func (f *fakeListingProvider) Resolve(context.Context, string) (string, error) {
	return "", errors.New("fake provider: Resolve is not used by secret ls")
}

func (f *fakeListingProvider) List(context.Context) ([]string, error) {
	return f.names, f.err
}

// resolveOnlyProvider implements secrets.Provider without the optional Lister
// capability.
type resolveOnlyProvider struct{}

func (resolveOnlyProvider) Resolve(context.Context, string) (string, error) {
	return "", errors.New("resolve-only provider")
}

// TestSecretLsSortsProviderNamesIntoSingleNameColumn pins the CLI rendering
// contract for any provider: whatever order a Lister returns, `secret ls`
// prints one sorted NAME column and nothing else, and the opener receives the
// injected local directory.
func TestSecretLsSortsProviderNamesIntoSingleNameColumn(t *testing.T) {
	deps, _, _ := newSecretDeps(t)
	var openedDir string
	deps.OpenSecretProvider = func(localDir string) (secrets.Provider, error) {
		openedDir = localDir
		return &fakeListingProvider{names: []string{"zeta", "alpha", "mid"}}, nil
	}

	out, stderr, err := runSecretCLI(t, deps, "", "secret", "ls")
	if err != nil {
		t.Fatalf("secret ls: err = %v, want nil", err)
	}
	if openedDir != deps.SecretsDir {
		t.Fatalf("opener received dir %q, want the injected %q", openedDir, deps.SecretsDir)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	want := []string{"NAME", "alpha", "mid", "zeta"}
	if len(lines) != len(want) {
		t.Fatalf("rows = %v, want %v (output %q)", lines, want, out)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Fatalf("row %d = %q, want %q (output %q)", i, lines[i], want[i], out)
		}
	}
}

// TestSecretLsEmptyProviderListPrintsHeaderOnly pins that an empty scope is a
// successful header-only listing rather than an error.
func TestSecretLsEmptyProviderListPrintsHeaderOnly(t *testing.T) {
	deps := newSecretDepsOnly(t)
	deps.OpenSecretProvider = func(string) (secrets.Provider, error) {
		return &fakeListingProvider{}, nil
	}

	out, _, err := runSecretCLI(t, deps, "", "secret", "ls")
	if err != nil {
		t.Fatalf("secret ls: err = %v, want nil", err)
	}
	if got := strings.TrimSpace(out); got != "NAME" {
		t.Fatalf("output = %q, want only the NAME header", out)
	}
}

// TestSecretLsRejectsProviderWithoutListingCapability pins the narrow-capability
// contract: a Provider that does not opt into Lister fails with a clear
// message instead of panicking or printing a partial table.
func TestSecretLsRejectsProviderWithoutListingCapability(t *testing.T) {
	deps := newSecretDepsOnly(t)
	deps.OpenSecretProvider = func(string) (secrets.Provider, error) {
		return resolveOnlyProvider{}, nil
	}

	out, _, err := runSecretCLI(t, deps, "", "secret", "ls")
	if err == nil || !strings.Contains(err.Error(), "does not support listing") {
		t.Fatalf("err = %v, want a missing-listing-capability error", err)
	}
	if out != "" {
		t.Fatalf("stdout = %q, want empty", out)
	}
}

// TestSecretLsPropagatesProviderOpenError pins that a provider-construction
// failure (invalid or incomplete configuration) reaches the operator verbatim
// with no output, exactly as returned by the opener.
func TestSecretLsPropagatesProviderOpenError(t *testing.T) {
	deps := newSecretDepsOnly(t)
	openErr := errors.New("secret provider: invalid SECRETS_PROVIDER \"vault\"")
	deps.OpenSecretProvider = func(string) (secrets.Provider, error) {
		return nil, openErr
	}

	out, _, err := runSecretCLI(t, deps, "", "secret", "ls")
	if !errors.Is(err, openErr) {
		t.Fatalf("err = %v, want it to wrap the opener's error", err)
	}
	if out != "" {
		t.Fatalf("stdout = %q, want empty", out)
	}
}

// TestSecretLsInvalidConfigFailsWithoutLocalFallback pins that a broken
// provider configuration fails `secret ls` even though local secret files
// exist: the command must never silently fall back to listing local files
// when the selected provider cannot be opened.
func TestSecretLsInvalidConfigFailsWithoutLocalFallback(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T)
		want  string
	}{
		{
			"unknown provider",
			func(t *testing.T) {
				clearSecretsEnv(t)
				t.Setenv("SECRETS_PROVIDER", "vault")
			},
			`invalid SECRETS_PROVIDER "vault"`,
		},
		{
			"incomplete infisical config",
			func(t *testing.T) {
				setInfisicalEnv(t)
				unsetSecretsEnvVar(t, "INFISICAL_PROJECT_ID")
			},
			"INFISICAL_PROJECT_ID",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps, store, _ := newSecretDeps(t)
			if err := store.Set(context.Background(), "local-only", "v"); err != nil {
				t.Fatalf("seed local secret: %v", err)
			}
			tt.setup(t)

			out, _, err := runSecretCLI(t, deps, "", "secret", "ls")
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tt.want)
			}
			if out != "" {
				t.Fatalf("stdout = %q, want empty (no local fallback listing)", out)
			}
			if strings.Contains(err.Error(), "local-only") {
				t.Fatalf("error exposed local secret names: %v", err)
			}
		})
	}
}

// TestSecretSetRejectsExternalProviderBeforeReadingValue pins the ordering
// contract of `secret set`: with Infisical selected the command fails with
// the provider rejection before any value is read (stdin or prompt), before
// any prompt is shown, and before the local secrets directory is created —
// and the piped value never appears in any output.
func TestSecretSetRejectsExternalProviderBeforeReadingValue(t *testing.T) {
	const piped = "piped-super-secret"
	rejection := `secret set: not supported with SECRETS_PROVIDER="infisical"`

	t.Run("stdin value is never read", func(t *testing.T) {
		deps, _, _ := newSecretDeps(t)
		setInfisicalEnv(t)

		out, stderr, err := runSecretCLI(t, deps, piped+"\n",
			"secret", "set", "db", "-")
		if err == nil || !strings.Contains(err.Error(), rejection) {
			t.Fatalf("err = %v, want the provider rejection %q", err, rejection)
		}
		if out != "" {
			t.Fatalf("stdout = %q, want empty", out)
		}
		if stderr != "" {
			t.Fatalf("stderr = %q, want empty (no prompt)", stderr)
		}
		if strings.Contains(err.Error(), piped) {
			t.Fatalf("error leaked the piped value: %v", err)
		}
		if _, statErr := os.Stat(deps.SecretsDir); !os.IsNotExist(statErr) {
			t.Fatalf("secrets dir stat = %v, want it never created", statErr)
		}
	})

	t.Run("prompt is never reached", func(t *testing.T) {
		deps, _, _ := newSecretDeps(t)
		setInfisicalEnv(t)

		out, stderr, err := runSecretCLI(t, deps, piped+"\n", "secret", "set", "db")
		if err == nil || !strings.Contains(err.Error(), rejection) {
			t.Fatalf("err = %v, want the provider rejection %q", err, rejection)
		}
		// The terminal guard would fire instead if the value read were
		// attempted first: the provider check must run before it.
		if strings.Contains(err.Error(), "interactive terminal") {
			t.Fatalf("value read ran before the provider check: %v", err)
		}
		if out != "" || stderr != "" {
			t.Fatalf("output = (%q, %q), want both empty", out, stderr)
		}
		if _, statErr := os.Stat(deps.SecretsDir); !os.IsNotExist(statErr) {
			t.Fatalf("secrets dir stat = %v, want it never created", statErr)
		}
	})
}

// TestSecretRmRejectsExternalProviderBeforeTouchingLocalFiles pins the same
// ordering for `secret rm`: with Infisical selected the command fails before
// the local secrets directory is created and without modifying an existing
// secret.
func TestSecretRmRejectsExternalProviderBeforeTouchingLocalFiles(t *testing.T) {
	rejection := `secret rm: not supported with SECRETS_PROVIDER="infisical"`

	t.Run("existing secret is not removed", func(t *testing.T) {
		deps, store, _ := newSecretDeps(t)
		if err := store.Set(context.Background(), "db", "local-value"); err != nil {
			t.Fatalf("seed local secret: %v", err)
		}
		setInfisicalEnv(t)

		out, _, err := runSecretCLI(t, deps, "", "secret", "rm", "db")
		if err == nil || !strings.Contains(err.Error(), rejection) {
			t.Fatalf("err = %v, want the provider rejection %q", err, rejection)
		}
		if out != "" {
			t.Fatalf("stdout = %q, want empty", out)
		}
		got, resolveErr := store.Resolve(context.Background(), "db")
		if resolveErr != nil || got != "local-value" {
			t.Fatalf("local secret after rejected rm = (%q, %v), want it untouched",
				got, resolveErr)
		}
	})

	t.Run("directory is not created", func(t *testing.T) {
		deps, _, _ := newSecretDeps(t)
		setInfisicalEnv(t)

		_, _, err := runSecretCLI(t, deps, "", "secret", "rm", "db")
		if err == nil || !strings.Contains(err.Error(), rejection) {
			t.Fatalf("err = %v, want the provider rejection %q", err, rejection)
		}
		if _, statErr := os.Stat(deps.SecretsDir); !os.IsNotExist(statErr) {
			t.Fatalf("secrets dir stat = %v, want it never created", statErr)
		}
	})
}

// TestSecretSetRmRejectInvalidSecretsConfigBeforeTouchingDisk pins that a
// broken provider configuration rejects both write commands with the config
// error — naming the offending variable or value — before any value is read
// and before any local file or directory is touched.
func TestSecretSetRmRejectInvalidSecretsConfigBeforeTouchingDisk(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(t *testing.T)
		args   []string
		prefix string
		want   string
	}{
		{
			"set with unknown provider",
			func(t *testing.T) {
				clearSecretsEnv(t)
				t.Setenv("SECRETS_PROVIDER", "vault")
			},
			[]string{"secret", "set", "db", "-"},
			"secret set:",
			`invalid SECRETS_PROVIDER "vault"`,
		},
		{
			"rm with unknown provider",
			func(t *testing.T) {
				clearSecretsEnv(t)
				t.Setenv("SECRETS_PROVIDER", "vault")
			},
			[]string{"secret", "rm", "db"},
			"secret rm:",
			`invalid SECRETS_PROVIDER "vault"`,
		},
		{
			"set with incomplete infisical config",
			func(t *testing.T) {
				setInfisicalEnv(t)
				unsetSecretsEnvVar(t, "INFISICAL_CLIENT_SECRET")
			},
			[]string{"secret", "set", "db", "-"},
			"secret set:",
			"INFISICAL_CLIENT_SECRET",
		},
		{
			"rm with incomplete infisical config",
			func(t *testing.T) {
				setInfisicalEnv(t)
				unsetSecretsEnvVar(t, "INFISICAL_CLIENT_SECRET")
			},
			[]string{"secret", "rm", "db"},
			"secret rm:",
			"INFISICAL_CLIENT_SECRET",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps, _, _ := newSecretDeps(t)
			tt.setup(t)

			out, _, err := runSecretCLI(t, deps, "piped-super-secret\n", tt.args...)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tt.want)
			}
			if !strings.Contains(err.Error(), tt.prefix) {
				t.Fatalf("err = %v, want the %q command prefix", err, tt.prefix)
			}
			if strings.Contains(err.Error(), "piped-super-secret") {
				t.Fatalf("error leaked the piped value: %v", err)
			}
			if out != "" {
				t.Fatalf("stdout = %q, want empty", out)
			}
			if _, statErr := os.Stat(deps.SecretsDir); !os.IsNotExist(statErr) {
				t.Fatalf("secrets dir stat = %v, want it never created", statErr)
			}
		})
	}
}

// TestSecretExplicitLocalProviderAllowsReadsAndWrites pins that selecting the
// local provider explicitly (including a spelling variant) preserves the
// historical behavior end to end: `secret set` writes the file and `secret
// ls` lists it.
func TestSecretExplicitLocalProviderAllowsReadsAndWrites(t *testing.T) {
	deps, store, _ := newSecretDeps(t)
	clearSecretsEnv(t)
	t.Setenv("SECRETS_PROVIDER", " LOCAL ")

	out, _, err := runSecretCLI(t, deps, "v1\n", "secret", "set", "db", "-")
	if err != nil {
		t.Fatalf("secret set: err = %v, want nil", err)
	}
	if !strings.Contains(out, `Successfully set secret "db"`) {
		t.Fatalf("stdout missing the success message: %q", out)
	}
	if got, err := store.Resolve(context.Background(), "db"); err != nil || got != "v1" {
		t.Fatalf("resolved = %q, err = %v, want v1", got, err)
	}

	out, _, err = runSecretCLI(t, deps, "", "secret", "ls")
	if err != nil {
		t.Fatalf("secret ls: err = %v, want nil", err)
	}
	if !strings.Contains(out, "db") {
		t.Fatalf("stdout = %q, want it to list db", out)
	}
}

// TestDefaultDependenciesWireSecretProviderOpener pins the production wiring:
// the built-in command tree constructs the configured provider for `secret
// ls` rather than requiring per-command injection.
func TestDefaultDependenciesWireSecretProviderOpener(t *testing.T) {
	if DefaultDependencies().OpenSecretProvider == nil {
		t.Fatal("OpenSecretProvider must be wired in production Dependencies")
	}
}
