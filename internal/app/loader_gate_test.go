package app

import (
	"bytes"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// validGateTemplate is a template that parses and validates.
const validGateTemplate = `
runtime: python3.14
events:
  - handler: handler.main
    pattern:
      event_name: [MODIFY]
`

// invalidGateTemplate is a deterministic parse/validation failure: the bytes are
// readable and hashable, but no parse can ever succeed for them.
const invalidGateTemplate = "runtime: python9.9\n"

// fakeParseGate records the loader's callbacks so a test can assert the gate
// contract (Observe before the read, SkipParse only after a successful read) and
// decide which identities are already recorded.
type fakeParseGate struct {
	observed []string
	skipped  []string
	skip     map[string]string
	skipErr  error
}

func (g *fakeParseGate) Observe(name string) {
	g.observed = append(g.observed, name)
}

func (g *fakeParseGate) SkipParse(name, identity string) (bool, error) {
	g.skipped = append(g.skipped, name+":"+identity)
	want, ok := g.skip[name]
	if !ok || want != identity {
		return false, nil
	}
	return true, g.skipErr
}

// TestTemplateIdentityIsStableContentDigest pins the identity contract: it is a
// deterministic SHA-256 digest of exactly the bytes read, so the same invalid
// input is recognized again and a one-byte change is not.
func TestTemplateIdentityIsStableContentDigest(t *testing.T) {
	a := TemplateIdentity([]byte(invalidGateTemplate))
	b := TemplateIdentity([]byte(invalidGateTemplate))
	if a != b {
		t.Fatalf("identity is not deterministic: %q != %q", a, b)
	}
	// Known SHA-256 of the empty input pins the encoding and the prefix.
	if got, want := TemplateIdentity(nil), "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; got != want {
		t.Fatalf("TemplateIdentity(nil) = %q, want %q", got, want)
	}
	if !strings.HasPrefix(a, "sha256:") || len(a) != len("sha256:")+64 {
		t.Fatalf("identity = %q, want sha256:<64 hex chars>", a)
	}
	changed := TemplateIdentity([]byte(invalidGateTemplate + "# changed\n"))
	if changed == a {
		t.Fatal("different bytes must yield a different identity")
	}
}

// TestReadTemplateSourceCarriesIdentityOnlyForReadBytes pins the read/parse
// split: successfully read bytes expose their identity, a deterministic parse
// failure carries that identity, and operational failures never carry one (a
// transient read must stay retryable instead of looking like the same invalid
// definition).
func TestReadTemplateSourceCarriesIdentityOnlyForReadBytes(t *testing.T) {
	t.Run("deterministic invalid parse", func(t *testing.T) {
		root := t.TempDir()
		writeTemplate(t, root, "broken", invalidGateTemplate)

		src, err := ReadTemplateSource(root, "broken")
		if err != nil {
			t.Fatalf("ReadTemplateSource: %v", err)
		}
		if string(src.Data) != invalidGateTemplate {
			t.Fatalf("data = %q, want the exact bytes read", string(src.Data))
		}
		if want := TemplateIdentity([]byte(invalidGateTemplate)); src.Identity != want {
			t.Fatalf("identity = %q, want %q", src.Identity, want)
		}

		_, parseErr := src.Parse()
		if parseErr == nil {
			t.Fatal("invalid template must fail to parse")
		}
		identity, ok := InvalidTemplateIdentity(parseErr)
		if !ok {
			t.Fatalf("parse error %v does not carry an invalid-template identity", parseErr)
		}
		if identity != src.Identity {
			t.Fatalf("invalid identity = %q, want the source identity %q", identity, src.Identity)
		}
		if !strings.Contains(parseErr.Error(), `app "broken"`) {
			t.Fatalf("parse error = %q, want it to name the app", parseErr)
		}
	})

	t.Run("valid parse", func(t *testing.T) {
		root := t.TempDir()
		writeTemplate(t, root, "good", validGateTemplate)

		src, err := ReadTemplateSource(root, "good")
		if err != nil {
			t.Fatalf("ReadTemplateSource: %v", err)
		}
		fn, err := src.Parse()
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if fn.Name != "good" || fn.Template == nil {
			t.Fatalf("parsed app = %+v, want the valid app", fn)
		}
	})

	t.Run("unreadable template has no identity", func(t *testing.T) {
		root := t.TempDir()
		dir := filepath.Join(root, "flaky")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		// A directory named template.yaml is present, so this is an app
		// candidate, but reading it fails operationally.
		if err := os.MkdirAll(filepath.Join(dir, TemplateFile), 0o755); err != nil {
			t.Fatalf("mkdir template path: %v", err)
		}

		if _, err := ReadTemplateSource(root, "flaky"); err == nil {
			t.Fatal("reading a template that is a directory must fail")
		} else if identity, ok := InvalidTemplateIdentity(err); ok {
			t.Fatalf("operational read error must not carry an identity, got %q", identity)
		}
	})

	t.Run("missing template is not ready", func(t *testing.T) {
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, "empty"), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if _, err := ReadTemplateSource(root, "empty"); !errors.Is(err, ErrNotReady) {
			t.Fatalf("err = %v, want ErrNotReady", err)
		}
	})

	t.Run("missing directory is not exist", func(t *testing.T) {
		root := t.TempDir()
		if _, err := ReadTemplateSource(root, "gone"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("err = %v, want fs.ErrNotExist", err)
		}
	})
}

// TestLoadWithDiagnosticsUsingGateSkipsRecordedIdentity pins the loader fast
// path: when the gate recognizes the exact persisted invalid identity, the
// template is neither parsed nor warned about again, but it is still reported as
// a Recorded issue so the caller can track the app for removal.
func TestLoadWithDiagnosticsUsingGateSkipsRecordedIdentity(t *testing.T) {
	root := t.TempDir()
	writeTemplate(t, root, "good", validGateTemplate)
	writeTemplate(t, root, "broken", invalidGateTemplate)
	if err := os.MkdirAll(filepath.Join(root, "empty"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	identity := TemplateIdentity([]byte(invalidGateTemplate))
	recordedErr := errors.New(`function "broken": unknown runtime "python9.9"`)
	gate := &fakeParseGate{
		skip:    map[string]string{"broken": identity},
		skipErr: recordedErr,
	}

	var logs bytes.Buffer
	loader := NewLoader(root, slog.New(slog.NewTextHandler(&logs, nil)))
	apps, issues, err := loader.LoadWithDiagnosticsUsing(gate)
	if err != nil {
		t.Fatalf("LoadWithDiagnosticsUsing: %v", err)
	}

	if len(apps) != 1 || apps[0].Name != "good" {
		t.Fatalf("valid apps = %+v, want only good", apps)
	}
	if len(issues) != 1 {
		t.Fatalf("issues = %+v, want one for broken", issues)
	}
	issue := issues[0]
	if issue.Name != "broken" || !issue.Recorded {
		t.Fatalf("issue = %+v, want a Recorded issue for broken", issue)
	}
	if issue.Identity != identity {
		t.Fatalf("issue identity = %q, want %q", issue.Identity, identity)
	}
	if !errors.Is(issue.Err, recordedErr) {
		t.Fatalf("issue error = %v, want the already-persisted diagnostic", issue.Err)
	}

	// Observe precedes the read for every directory; SkipParse is only reached
	// for readable templates (not the template-less directory).
	for _, want := range []string{"broken", "empty", "good"} {
		found := false
		for _, got := range gate.observed {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Observe calls = %v, want %q", gate.observed, want)
		}
	}
	if len(gate.skipped) != 2 {
		t.Fatalf("SkipParse calls = %v, want one per readable template", gate.skipped)
	}
	if len(gate.observed) < len(gate.skipped) {
		t.Fatalf("SkipParse must follow Observe, observed=%v skipped=%v", gate.observed, gate.skipped)
	}

	if logs.Len() != 0 {
		t.Fatalf("a Recorded issue must not be logged again, logs = %q", logs.String())
	}
}

// TestLoadWithDiagnosticsUsingGateReparsesUnrecordedIdentity pins that a gate
// which does not recognize the read bytes does not suppress the parse: the
// deterministic failure is reported with its identity and Recorded=false so the
// caller records the transition.
func TestLoadWithDiagnosticsUsingGateReparsesUnrecordedIdentity(t *testing.T) {
	root := t.TempDir()
	writeTemplate(t, root, "broken", invalidGateTemplate)

	gate := &fakeParseGate{} // recognizes nothing
	loader := NewLoader(root, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	_, issues, err := loader.LoadWithDiagnosticsUsing(gate)
	if err != nil {
		t.Fatalf("LoadWithDiagnosticsUsing: %v", err)
	}
	if len(issues) != 1 {
		t.Fatalf("issues = %+v, want one", issues)
	}
	issue := issues[0]
	if issue.Recorded {
		t.Fatal("an unrecognized identity must not be reported as Recorded")
	}
	if want := TemplateIdentity([]byte(invalidGateTemplate)); issue.Identity != want {
		t.Fatalf("issue identity = %q, want %q", issue.Identity, want)
	}
	if issue.Err == nil || !strings.Contains(issue.Err.Error(), `app "broken"`) {
		t.Fatalf("issue error = %v, want the parse failure naming the app", issue.Err)
	}
}

// TestLoadWithDiagnosticsUsingNilGateMatchesDiagnosticLoader pins the optional
// gate: nil leaves the historical loader behavior exactly intact.
func TestLoadWithDiagnosticsUsingNilGateMatchesDiagnosticLoader(t *testing.T) {
	root := t.TempDir()
	writeTemplate(t, root, "good", validGateTemplate)
	writeTemplate(t, root, "broken", invalidGateTemplate)

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	plainApps, plainIssues, err := NewLoader(root, logger).LoadWithDiagnostics()
	if err != nil {
		t.Fatalf("LoadWithDiagnostics: %v", err)
	}
	gateApps, gateIssues, err := NewLoader(root, logger).LoadWithDiagnosticsUsing(nil)
	if err != nil {
		t.Fatalf("LoadWithDiagnosticsUsing(nil): %v", err)
	}
	if len(plainApps) != len(gateApps) || len(gateApps) != 1 || plainApps[0].Name != gateApps[0].Name {
		t.Fatalf("apps = %+v / %+v, want the same single valid app", plainApps, gateApps)
	}
	if len(plainIssues) != len(gateIssues) || len(gateIssues) != 1 {
		t.Fatalf("issues = %+v / %+v, want the same single issue", plainIssues, gateIssues)
	}
	if gateIssues[0].Recorded || gateIssues[0].Identity == "" {
		t.Fatalf("nil gate issue = %+v, want a parsed (unrecorded) issue with identity", gateIssues[0])
	}
}
