package git

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"relay/internal/app"
)

// TestMaterializeOnlyChangedAppIsExchanged pins the incremental contract of one
// sync pass over several apps: every candidate is staged and compared, but only
// the app whose materialized representation actually changed is committed. The
// unchanged app keeps its live directory inode (and therefore its reconciler
// watches), while the changed app is replaced atomically.
//
// The follow-up pass is identical to the source and injects an unsupported
// exchange. Because nothing needs replacement, the capability probe is skipped:
// the sync succeeds without calling the exchange primitive even once and both
// live inodes stay put. This is the deterministic basis for "no unnecessary
// reconcile callback": /apps was not mutated, so no fsnotify event can be
// produced for either live app (reserved .sync-* staging events are filtered by
// the reconciler; see internal/reconciler/staging_test.go).
func TestMaterializeOnlyChangedAppIsExchanged(t *testing.T) {
	requireAtomicExchange(t)

	src := t.TempDir()
	writeFile(t, filepath.Join(src, "alpha", "template.yaml"),
		hardeningTemplate("alpha-v1"))
	writeFile(t, filepath.Join(src, "alpha", "old-only.txt"), "alpha old\n")
	writeFile(t, filepath.Join(src, "beta", "template.yaml"),
		hardeningTemplate("beta"))
	writeFile(t, filepath.Join(src, "beta", "handler.js"),
		"export function beta(){}\n")

	apps := t.TempDir()
	selection := mustSelect(t, src, src)
	first, err := materializeApps(selection, apps)
	if err != nil {
		t.Fatalf("initial materialize: %v", err)
	}
	if first.desired != 2 || len(first.materialized) != 2 ||
		len(first.removed) != 0 {
		t.Fatalf("initial result = %+v, want desired=2 "+
			"materialized=[alpha beta] removed=[]", first)
	}

	alphaBefore, err := os.Stat(filepath.Join(apps, "alpha"))
	if err != nil {
		t.Fatalf("stat alpha before update: %v", err)
	}
	betaBefore, err := os.Stat(filepath.Join(apps, "beta"))
	if err != nil {
		t.Fatalf("stat beta before update: %v", err)
	}

	// Change alpha only: a new template generation, a delete, and an add. beta
	// stays byte-for-byte identical to its live tree.
	writeFile(t, filepath.Join(src, "alpha", "template.yaml"),
		hardeningTemplate("alpha-v2"))
	writeFile(t, filepath.Join(src, "alpha", "new-only.txt"), "alpha new\n")
	if err := os.Remove(filepath.Join(src, "alpha", "old-only.txt")); err != nil {
		t.Fatalf("remove alpha old-only: %v", err)
	}
	selection = mustSelect(t, src, src)

	var liveTargets []string
	swapExchange(t, func(a, b string) error {
		if app.IsReservedDir(filepath.Base(b)) {
			return platformExchange(a, b)
		}
		liveTargets = append(liveTargets, filepath.Base(b))
		return platformExchange(a, b)
	})

	second, err := materializeApps(selection, apps)
	if err != nil {
		t.Fatalf("incremental materialize: %v", err)
	}
	if second.desired != 2 {
		t.Fatalf("desired = %d, want 2", second.desired)
	}
	if len(second.materialized) != 1 || second.materialized[0] != "alpha" {
		t.Fatalf("materialized = %v, want only [alpha]", second.materialized)
	}
	if len(second.removed) != 0 {
		t.Fatalf("removed = %v, want none", second.removed)
	}
	if len(liveTargets) != 1 || liveTargets[0] != "alpha" {
		t.Fatalf("live exchange targets = %v, want exactly [alpha]", liveTargets)
	}

	alphaAfter, err := os.Stat(filepath.Join(apps, "alpha"))
	if err != nil {
		t.Fatalf("stat alpha after update: %v", err)
	}
	betaAfter, err := os.Stat(filepath.Join(apps, "beta"))
	if err != nil {
		t.Fatalf("stat beta after update: %v", err)
	}
	if os.SameFile(alphaBefore, alphaAfter) {
		t.Fatal("alpha inode did not change despite the atomic exchange")
	}
	if !os.SameFile(betaBefore, betaAfter) {
		t.Fatal("beta inode changed although its tree was unchanged")
	}
	if got := mustReadFile(t,
		filepath.Join(apps, "alpha", "new-only.txt")); got != "alpha new\n" {
		t.Fatalf("alpha new content = %q", got)
	}
	if _, err := os.Stat(
		filepath.Join(apps, "alpha", "old-only.txt")); !os.IsNotExist(err) {
		t.Fatalf("alpha old file survived the update: %v", err)
	}
	betaHandler := mustReadFile(t, filepath.Join(apps, "beta", "handler.js"))
	if betaHandler != "export function beta(){}\n" {
		t.Fatalf("unchanged beta content = %q", betaHandler)
	}

	// A second, identical sync must not exchange anything. Inject a hard failure
	// into the seam to prove the probe is not even attempted when no existing app
	// changed.
	swapExchange(t, func(a, b string) error {
		t.Fatalf("exchange called for an unchanged app: %q -> %q", a, b)
		return errors.New("unreachable")
	})
	third, err := materializeApps(selection, apps)
	if err != nil {
		t.Fatalf("identical materialize: %v", err)
	}
	if third.desired != 2 {
		t.Fatalf("identical pass desired = %d, want 2", third.desired)
	}
	if len(third.materialized) != 0 || len(third.removed) != 0 {
		t.Fatalf("identical pass = %+v, want no materialized or removed apps", third)
	}
	alphaFinal, err := os.Stat(filepath.Join(apps, "alpha"))
	if err != nil {
		t.Fatalf("stat alpha after identical pass: %v", err)
	}
	betaFinal, err := os.Stat(filepath.Join(apps, "beta"))
	if err != nil {
		t.Fatalf("stat beta after identical pass: %v", err)
	}
	if !os.SameFile(alphaAfter, alphaFinal) {
		t.Fatal("identical pass replaced the alpha directory")
	}
	if !os.SameFile(betaAfter, betaFinal) {
		t.Fatal("identical pass replaced the beta directory")
	}
	assertNoReservedEntries(t, apps)
}

// TestMaterializeDetectsResourceOnlyChangeWithoutFingerprintChange pins the
// interaction between materialization and the image fingerprint. A template
// edit confined to the top-level `resources` mapping is a real materialized
// change (the live template must expose the new limits), so the app directory is
// exchanged and reported. It is NOT an image change: the runtime fingerprint
// deliberately strips `resources`, so the reconciler's unchanged-fingerprint
// path publishes the new limits to the warm pool without rebuilding (covered in
// internal/reconciler/reconciler_resources_test.go).
func TestMaterializeDetectsResourceOnlyChangeWithoutFingerprintChange(t *testing.T) {
	requireAtomicExchange(t)

	src := t.TempDir()
	const resourceTemplate = `runtime: node24
resources:
  memory: 128MiB
events:
  - handler: index.hi
    pattern:
      event_name: [INSERT]
`
	writeFile(t, filepath.Join(src, "res", "template.yaml"), resourceTemplate)
	writeFile(t, filepath.Join(src, "res", "index.js"), "export function hi(e){}\n")

	apps := t.TempDir()
	if _, err := materializeApps(mustSelect(t, src, src), apps); err != nil {
		t.Fatalf("initial materialize: %v", err)
	}
	before, err := app.LoadSingle(apps, "res")
	if err != nil {
		t.Fatalf("load initial app: %v", err)
	}
	beforeFP, err := app.FingerprintApp(before.Dir, before.Template)
	if err != nil {
		t.Fatalf("fingerprint initial app: %v", err)
	}

	writeFile(t, filepath.Join(src, "res", "template.yaml"),
		strings.Replace(resourceTemplate, "128MiB", "1GiB", 1))
	second, err := materializeApps(mustSelect(t, src, src), apps)
	if err != nil {
		t.Fatalf("resource-only materialize: %v", err)
	}
	if len(second.materialized) != 1 || second.materialized[0] != "res" {
		t.Fatalf("materialized = %v, want [res] for a resource-only template edit", second.materialized)
	}

	after, err := app.LoadSingle(apps, "res")
	if err != nil {
		t.Fatalf("load updated app: %v", err)
	}
	if got, want := after.Template.Resources.MemoryBytes, int64(1<<30); got != want {
		t.Fatalf("materialized memory limit = %d, want %d", got, want)
	}
	afterFP, err := app.FingerprintApp(after.Dir, after.Template)
	if err != nil {
		t.Fatalf("fingerprint updated app: %v", err)
	}
	if afterFP != beforeFP {
		t.Fatalf("resource-only edit changed the image fingerprint: %s -> %s", beforeFP, afterFP)
	}
	assertNoReservedEntries(t, apps)
}

// TestSameStagedTreeMatchesMaterializedRepresentation exercises the comparison
// against the representation stageAppDir actually produces: directories and
// regular files with their permission bits, empty directories included, and no
// symlinks (copyTree skips them). Every discrepancy is "changed" so the live
// tree is replaced; only an identical materialized tree is left alone.
func TestSameStagedTreeMatchesMaterializedRepresentation(t *testing.T) {
	type treePair struct{ staged, live string }

	newPair := func(t *testing.T) treePair {
		t.Helper()
		pair := treePair{
			staged: filepath.Join(t.TempDir(), "staged"),
			live:   filepath.Join(t.TempDir(), "live"),
		}
		for _, root := range []string{pair.staged, pair.live} {
			writeFile(t, filepath.Join(root, "template.yaml"), "runtime: node24\n")
			writeFile(t, filepath.Join(root, "nested", "keep.txt"), "same bytes\n")
			if err := os.MkdirAll(filepath.Join(root, "empty"), 0o755); err != nil {
				t.Fatalf("mkdir empty: %v", err)
			}
			for _, dir := range []string{root, filepath.Join(root, "nested"), filepath.Join(root, "empty")} {
				if err := os.Chmod(dir, 0o755); err != nil {
					t.Fatalf("chmod %q: %v", dir, err)
				}
			}
			for _, file := range []string{
				filepath.Join(root, "template.yaml"),
				filepath.Join(root, "nested", "keep.txt"),
			} {
				if err := os.Chmod(file, 0o644); err != nil {
					t.Fatalf("chmod %q: %v", file, err)
				}
			}
		}
		return pair
	}

	cases := []struct {
		name   string
		mutate func(t *testing.T, pair treePair)
		want   bool
	}{
		{
			name:   "identical including empty directory",
			mutate: func(*testing.T, treePair) {},
			want:   true,
		},
		{
			name: "same-size different bytes",
			mutate: func(t *testing.T, pair treePair) {
				writeFile(t, filepath.Join(pair.live, "nested", "keep.txt"), "same bytez\n")
			},
			want: false,
		},
		{
			name: "file permission differs",
			mutate: func(t *testing.T, pair treePair) {
				if err := os.Chmod(filepath.Join(pair.live, "nested", "keep.txt"), 0o600); err != nil {
					t.Fatalf("chmod file: %v", err)
				}
			},
			want: false,
		},
		{
			name: "directory permission differs",
			mutate: func(t *testing.T, pair treePair) {
				if err := os.Chmod(filepath.Join(pair.live, "nested"), 0o750); err != nil {
					t.Fatalf("chmod dir: %v", err)
				}
			},
			want: false,
		},
		{
			name: "root permission differs",
			mutate: func(t *testing.T, pair treePair) {
				if err := os.Chmod(pair.live, 0o700); err != nil {
					t.Fatalf("chmod root: %v", err)
				}
			},
			want: false,
		},
		{
			name: "empty directory only in staged tree",
			mutate: func(t *testing.T, pair treePair) {
				if err := os.Remove(filepath.Join(pair.live, "empty")); err != nil {
					t.Fatalf("remove live empty dir: %v", err)
				}
			},
			want: false,
		},
		{
			name: "empty directory only in live tree",
			mutate: func(t *testing.T, pair treePair) {
				if err := os.Mkdir(filepath.Join(pair.live, "extra-empty"), 0o755); err != nil {
					t.Fatalf("mkdir extra empty dir: %v", err)
				}
			},
			want: false,
		},
		{
			name: "file added in staged tree",
			mutate: func(t *testing.T, pair treePair) {
				writeFile(t, filepath.Join(pair.staged, "new.txt"), "new\n")
			},
			want: false,
		},
		{
			name: "file added in live tree",
			mutate: func(t *testing.T, pair treePair) {
				writeFile(t, filepath.Join(pair.live, "extra.txt"), "extra\n")
			},
			want: false,
		},
		{
			name: "live entry is a symlink",
			mutate: func(t *testing.T, pair treePair) {
				path := filepath.Join(pair.live, "nested", "keep.txt")
				if err := os.Remove(path); err != nil {
					t.Fatalf("remove live file: %v", err)
				}
				if err := os.Symlink("template.yaml", path); err != nil {
					t.Fatalf("symlink live entry: %v", err)
				}
			},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pair := newPair(t)
			tc.mutate(t, pair)
			got, err := sameStagedTree(pair.staged, pair.live)
			if err != nil {
				t.Fatalf("sameStagedTree: %v", err)
			}
			if got != tc.want {
				t.Fatalf("sameStagedTree = %v, want %v", got, tc.want)
			}
		})
	}
}
