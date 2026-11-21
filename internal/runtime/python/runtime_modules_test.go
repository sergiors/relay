package python

import (
	"encoding/json"
	"io/fs"
	"path"
	"strings"
	"testing"

	"relay/internal/runtime/plan"
)

// TestPlanShipsEmbeddedRuntimeModules pins the embedded module packaging: the
// plan writes the RESOLVED bootstrap (the embedded standard variant or its
// zuvloop form) plus EVERY embedded .py sibling module, each at /relay/<name>
// with the exact embedded bytes, so the bootstrap's relative _relay_* imports
// resolve inside the image. The service health probe is not among them: it
// belongs to Relay's own healthcheck, never the image. It must also carry the
// runtime metadata document.
func TestPlanShipsEmbeddedRuntimeModules(t *testing.T) {
	for _, tc := range []struct {
		name      string
		spec      plan.Spec
		handlers  []string
		bootstrap []byte
	}{
		{name: "standard asyncio bootstrap", spec: testSpecs[0], handlers: []string{"handler"}, bootstrap: Bootstrap},
		{name: "zuvloop-enabled bootstrap", spec: zuvloopSpec(true), handlers: []string{"handler"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantBootstrap := tc.bootstrap
			if wantBootstrap == nil {
				var err error
				wantBootstrap, err = bootstrapFor(true)
				if err != nil {
					t.Fatalf("bootstrapFor(true): %v", err)
				}
			}
			p, err := Engine{}.Plan(tc.spec, t.TempDir(), tc.handlers)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}

			written := map[string]plan.File{}
			for _, f := range p.Files {
				if strings.HasPrefix(f.Path, relayDir+"/") {
					written[strings.TrimPrefix(f.Path, relayDir+"/")] = f
				}
			}
			embedded, err := runtimeFiles(wantBootstrap)
			if err != nil {
				t.Fatalf("runtimeFiles: %v", err)
			}
			for _, want := range embedded {
				name := path.Base(want.Path)
				got, ok := written[name]
				if !ok {
					t.Errorf("plan must write the embedded module %s, wrote %v", name, moduleList(written))
					continue
				}
				if string(got.Content) != string(want.Content) {
					t.Errorf("planned %s content differs from the embedded module", name)
				}
				if got.Mode != fs.FileMode(0o644) {
					t.Errorf("%s mode = %v, want 0644", name, got.Mode)
				}
			}
			if _, ok := written["bootstrap.py"]; !ok {
				t.Errorf("plan must write /relay/bootstrap.py, wrote %v", moduleList(written))
			} else if string(written["bootstrap.py"].Content) != string(wantBootstrap) {
				t.Errorf("/relay/bootstrap.py must carry the RESOLVED bootstrap variant")
			}
			if _, ok := written["runtime-metadata.json"]; !ok {
				t.Errorf("plan must write /relay/runtime-metadata.json, wrote %v", moduleList(written))
			}
			// The plan must not omit any embedded module.
			for _, embeddedName := range []string{"_relay_context.py", "_relay_server.py", "_relay_lifecycle.py", "_relay_config.py"} {
				if _, ok := written[embeddedName]; !ok {
					t.Errorf("plan must write the sibling module %s, wrote %v", embeddedName, moduleList(written))
				}
			}
			// The embed glob picks up the health probe too; it belongs to
			// Relay's own healthcheck, never the image module set.
			if _, ok := written[healthcheckFile]; ok {
				t.Errorf("plan must not write the service"+
					" health probe (it belongs to Relay's"+
					" healthcheck, never the image),"+
					" wrote %v", moduleList(written))
			}
		})
	}
}

// moduleList returns the plan's /relay file names for diagnostics.
func moduleList(written map[string]plan.File) []string {
	names := make([]string, 0, len(written))
	for name := range written {
		names = append(names, name)
	}
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	return names
}

// assertPlanOmitsHealthcheck pins that the embedded service health probe never
// enters a planned image: the embed glob picks it up for Relay's own use, but
// the Docker healthcheck runs its source directly (see
// runtime.serviceHealthConfig), so no plan may write it under /relay.
func assertPlanOmitsHealthcheck(t *testing.T, p plan.BuildPlan) {
	t.Helper()
	probe := path.Join(relayDir, healthcheckFile)
	for _, f := range p.Files {
		if f.Path == probe {
			t.Errorf("plan must not write the service health probe"+
				" %s (it belongs to Relay's healthcheck,"+
				" never the image)", probe)
		}
	}
}

// TestPlanShipsSameModulesForBothZuvloopVariants pins that only the bootstrap
// module differs between the loop modes: every _relay_* sibling is
// byte-identical, so a mode flip is a bootstrap-label change (rebuild) and the
// sibling modules never become a second copy of the mode distinction.
func TestPlanShipsSameModulesForBothZuvloopVariants(t *testing.T) {
	disabled, err := Engine{}.Plan(zuvloopSpec(false), t.TempDir(), []string{"handler"})
	if err != nil {
		t.Fatalf("disabled plan: %v", err)
	}
	enabled, err := Engine{}.Plan(zuvloopSpec(true), t.TempDir(), []string{"handler"})
	if err != nil {
		t.Fatalf("enabled plan: %v", err)
	}
	assertPlanOmitsHealthcheck(t, disabled)
	assertPlanOmitsHealthcheck(t, enabled)
	index := func(files []plan.File) map[string]string {
		out := make(map[string]string, len(files))
		for _, f := range files {
			if f.Path == "/relay/bootstrap.py" {
				continue
			}
			out[f.Path] = string(f.Content)
		}
		return out
	}
	di, ei := index(disabled.Files), index(enabled.Files)
	if len(di) != len(ei) {
		t.Fatalf("file sets differ: %d vs %d", len(di), len(ei))
	}
	for file, content := range di {
		e, ok := ei[file]
		if !ok {
			t.Fatalf("enabled plan lost %s", file)
		}
		if e != content {
			t.Errorf("sibling module %s differs between loop modes", file)
		}
	}
}

// TestPlanMetadataRecordsEffectiveBaseImage pins the metadata SOURCE accuracy:
// the generated document records the effective base reference verbatim (the
// configured tag when unpinned, configured-ref@digest when pinned) and is the
// only plan file the change touches. The caller must always pass the
// reference the image builds FROM.
func TestPlanMetadataRecordsEffectiveBaseImage(t *testing.T) {
	digest := "sha256:" + strings.Repeat("d", 64)
	cases := []struct {
		name string
		base string
	}{
		{"unpinned keeps the configured tag", "python:3.14-slim"},
		{"pinned records the selected digest", "python:3.14-slim@" + digest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := plan.Spec{Name: "python3.14", Engine: plan.EnginePython, BaseImage: tc.base}
			spec.RuntimeTools = testSpecs[0].RuntimeTools
			p, err := Engine{}.Plan(spec, t.TempDir(), nil)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			assertPlanOmitsHealthcheck(t, p)
			var metadata *plan.File
			for i := range p.Files {
				if p.Files[i].Path == plan.RuntimeMetadataPath {
					metadata = &p.Files[i]
					break
				}
			}
			if metadata == nil {
				t.Fatalf("plan has no %s", plan.RuntimeMetadataPath)
			}
			want, err := plan.RuntimeMetadataFile(spec)
			if err != nil {
				t.Fatalf("RuntimeMetadataFile: %v", err)
			}
			if string(metadata.Content) != string(want.Content) {
				t.Errorf("metadata = %q, want %q", metadata.Content, want.Content)
			}
			var doc plan.RuntimeMetadata
			if err := json.Unmarshal(metadata.Content, &doc); err != nil {
				t.Fatalf("decode metadata: %v", err)
			}
			if doc.Runtime != "python3.14" || doc.BaseImage != tc.base {
				t.Errorf("metadata = %+v, want runtime python3.14 base %q", doc, tc.base)
			}
			if p.BaseImage != tc.base {
				t.Errorf("plan base image = %q, want the effective %q", p.BaseImage, tc.base)
			}
		})
	}
}
