package app

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// Dir is the fixed application-convention root the loader reads apps from.
const Dir = "/apps"

// StagingPrefix prefixes Relay's own transient staging directories created
// directly under the apps root by git materialization
// (internal/git/materialize.go: os.MkdirTemp(dst, StagingPrefix+"*")). Such a
// directory holds an app tree only while it is being copied into place and is
// then moved into the app's real name (an atomic rename for a new app, an
// atomic directory exchange for a replacement) before materialization returns,
// so it is never an app and never a desired definition. A staging directory can
// also hold a replaced app's old tree briefly before disposal and any crash
// leftovers are reclaimed by the next serialized sync. The constant lives here,
// at the discovery boundary, so the creator (internal/git) and every
// discovery/reload path (internal/app, internal/reconciler, internal/state)
// share one definition and cannot drift apart.
const StagingPrefix = ".sync-"

// IsReservedDir reports whether name is one of Relay's internal, transient
// directories that discovery and live/periodic reload must ignore ENTIRELY: a
// staging directory (StagingPrefix) is not a user app, so it is neither a
// valid app nor an invalid desired definition, and while it briefly exists
// it must never produce a warning, a state write, or a degraded/unavailable
// transition. It is deliberately narrow — the exact Relay-owned prefix, not
// "any directory starting with '.'" — so a genuinely invalid user directory
// (including a hidden one) keeps its existing invalid-name diagnostics.
func IsReservedDir(name string) bool {
	return strings.HasPrefix(name, StagingPrefix)
}

// TemplateFile is the per-app definition file directly inside an app directory.
// A direct child directory is an app candidate only when it contains this
// entry; a directory without it is not an app (it may hold only unrelated
// files) and must never be treated as an invalid desired definition.
const TemplateFile = "template.yaml"

// IsRealAppDir reports whether path currently names a real directory, without
// following a symlink. A missing path or an unsupported entry (a symlink or a
// non-directory) returns (false, nil); other Lstat failures are returned so a
// caller does not mistake an ambiguous observation for removal.
func IsRealAppDir(path string) (bool, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return info.Mode()&os.ModeSymlink == 0 && info.IsDir(), nil
}

// HasTemplate reports whether dir contains a template.yaml candidate. It only
// inspects the entry and never parses it: a malformed or unreadable regular
// template is still present. Symlinked or non-regular template entries are
// present invalid candidates (true, ErrInvalidPath), never followed. A path
// that definitely does not exist is (false, nil); other stat errors are
// returned so callers can avoid treating an ambiguous read as removal.
func HasTemplate(dir string) (bool, error) {
	info, err := os.Lstat(filepath.Join(dir, TemplateFile))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return true, fmt.Errorf("%w: %q is not a regular non-symlink template", ErrInvalidPath, filepath.Join(dir, TemplateFile))
	}
	return true, nil
}

// App is a loaded app: its name (from the directory name), directory
// path (used later for image building), and parsed template.
type App struct {
	Name     string
	Dir      string
	Template *Template
}

// Loader discovers apps from a directory: each direct subdirectory is one
// app and must contain a template.yaml.
type Loader struct {
	dir string
	log *slog.Logger
}

func NewLoader(dir string, logger *slog.Logger) *Loader {
	return &Loader{dir: dir, log: logger}
}

// ErrNotReady reports that an app directory is present but has no
// template.yaml, so the directory is not an app candidate. LoadSingle returns it
// so a live reconcile can treat a previously-known app losing its template as a
// removal while ignoring a never-app directory entirely. Loader.Load and
// Loader.LoadWithDiagnostics skip it without an issue or a warning.
var ErrNotReady = errors.New("template.yaml not present")

// ErrInvalidPath reports a path that can never be an app directory: an
// illegal app name, a path that is not a DIRECT child of the apps
// root, a symlink, or a non-directory. It is deliberately distinct from
// ErrNotReady so a caller can tell "not an app candidate" (template absent) from
// "never loadable" (retain any previously-loaded version and do not replace it).
var ErrInvalidPath = errors.New("not a valid app directory")

// LoadIssue is one PRESENT app candidate that could not be loaded: its name and
// the load/validation error (an invalid path, an unreadable template, or an
// invalid template). It is reported by LoadWithDiagnostics so a caller (the
// worker's startup state phase) can record the invalid desired definition
// instead of silently leaving a stale view. A directory without template.yaml
// is not an app candidate and is never reported, and a directory that VANISHED
// between the directory read and the load is not a desired definition at all.
//
// Identity is the stable content identity of the successfully read
// template.yaml bytes when the failure is a DETERMINISTIC parse/validation
// failure of those bytes. It is empty for every other failure (invalid path, a
// read/stat error, or any operational failure), which must stay retryable and
// must never be treated as the same invalid desired definition. Recorded means
// the caller already persisted this exact identity as invalid: the parse was
// deliberately skipped so a caller must not log or record it again.
type LoadIssue struct {
	Name     string
	Err      error
	Identity string
	Recorded bool
}

// TemplateIdentity returns the stable content identity of raw template.yaml
// bytes: the SHA-256 digest rendered as "sha256:<hex>". It is used only to
// recognize that the exact same invalid input was already diagnosed; it is
// deliberately NOT a fencing token and never participates in generation/claim
// ordering.
func TemplateIdentity(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// InvalidTemplateError wraps a DETERMINISTIC ParseTemplate failure with the
// identity of the exact bytes that failed. Errors from reading the file, path
// resolution, or any post-parse operational work are never wrapped: only a
// successfully read template whose parse/validation fails deterministically can
// be recognized again without re-parsing.
type InvalidTemplateError struct {
	Identity string
	Err      error
}

func (e *InvalidTemplateError) Error() string { return e.Err.Error() }
func (e *InvalidTemplateError) Unwrap() error { return e.Err }

// InvalidTemplateIdentity reports the identity carried by a deterministic
// template parse/validation failure. It is false for any other error.
func InvalidTemplateIdentity(err error) (string, bool) {
	var invalid *InvalidTemplateError
	if !errors.As(err, &invalid) || invalid.Identity == "" {
		return "", false
	}
	return invalid.Identity, true
}

// TemplateSource is a template.yaml that was read successfully but not parsed.
// Splitting the read from the parse lets a caller hash the exact bytes, consult
// persisted state, and skip a known-invalid parse without ever reusing an
// identity from an earlier, different read.
type TemplateSource struct {
	Name     string
	Dir      string
	Data     []byte
	Identity string
}

// Parse parses the source bytes into a loaded app. A deterministic
// parse/validation failure is returned as *InvalidTemplateError carrying the
// source identity; other errors are not wrapped that way.
func (s TemplateSource) Parse() (App, error) {
	tmpl, err := ParseTemplate(s.Data)
	if err != nil {
		return App{}, &InvalidTemplateError{
			Identity: s.Identity,
			Err:      fmt.Errorf("app %q: %w", s.Name, err),
		}
	}
	return App{Name: s.Name, Dir: s.Dir, Template: tmpl}, nil
}

// TemplateParseGate lets a caller consult persisted state around a loader scan.
// Observe is called with the app name immediately BEFORE its template.yaml is
// read, so the implementation can snapshot the persisted generation the
// observation is based on. SkipParse is called after a successful read and hash,
// with the exact content identity, and may return true only when that identity
// is still persisted as the already-recorded deterministic invalid desired
// definition. The loader then reports a Recorded issue without parsing or
// logging; the returned error is the already-persisted diagnostic.
//
// A gate is optional. Nil keeps the historical behavior: every readable
// template is parsed and every failure is reported.
type TemplateParseGate interface {
	Observe(name string)
	SkipParse(name, identity string) (skip bool, err error)
}

// resolveAppDir opens name beneath a handle to root, applying the discovery
// policy shared by startup discovery (Loader.Load) and live/periodic reload
// (LoadSingle). It rejects symlinked/non-directory direct children and verifies
// that the opened directory is the same entry observed through the root handle.
// The returned Root pins that directory across path replacement; all subsequent
// template access must use it rather than rebuilding a path. Invalid app names
// are rejected after checking whether the safely opened directory is a candidate.
//
// A missing directory wraps fs.ErrNotExist (so a caller can treat it as a
// removal); an illegal name, a non-child path, a symlink, or a non-directory is
// ErrInvalidPath; other filesystem failures remain operational errors so a
// flaky read is never mistaken for a removal.
func resolveAppDir(root, name string) (*os.Root, string, error) {
	// Reject names that could escape or fail to identify a single directory
	// before touching the filesystem. Other invalid names may still identify a
	// non-app directory, so their candidacy is decided after the directory check.
	if name == "" || name == "." || name == ".." || filepath.IsAbs(name) ||
		strings.ContainsAny(name, "/\\") || name != filepath.Base(name) {
		return nil, "", fmt.Errorf("%w: %q is not a single path element", ErrInvalidPath, name)
	}
	nameErr := ValidName(name)
	appsRoot, err := os.OpenRoot(root)
	if err != nil {
		if nameErr != nil {
			return nil, "", fmt.Errorf("%w: %v", ErrInvalidPath, nameErr)
		}
		return nil, "", fmt.Errorf("app %q: open apps root: %w", name, err)
	}
	defer appsRoot.Close()
	info, err := appsRoot.Lstat(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			if nameErr != nil {
				return nil, "", fmt.Errorf("%w: %v", ErrInvalidPath, nameErr)
			}
			return nil, "", fmt.Errorf("app %q: %w", name, fs.ErrNotExist)
		}
		if nameErr != nil {
			return nil, "", fmt.Errorf("%w: %v", ErrInvalidPath, nameErr)
		}
		return nil, "", fmt.Errorf("app %q: stat: %w", name, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, "", fmt.Errorf("%w: %q is a symlink", ErrInvalidPath, name)
	}
	if !info.IsDir() {
		return nil, "", fmt.Errorf("%w: %q is not a directory", ErrInvalidPath, name)
	}
	appRoot, err := appsRoot.OpenRoot(name)
	if err != nil {
		// The entry may have changed between Lstat and OpenRoot. Reclassify a
		// now-missing, symlinked, or non-directory child without following it.
		current, statErr := appsRoot.Lstat(name)
		if statErr == nil && (current.Mode()&os.ModeSymlink != 0 || !current.IsDir()) {
			return nil, "", fmt.Errorf("%w: %q changed to an unsupported app path", ErrInvalidPath, name)
		}
		if errors.Is(statErr, fs.ErrNotExist) && nameErr == nil {
			return nil, "", fmt.Errorf("app %q: %w", name, fs.ErrNotExist)
		}
		if nameErr != nil {
			return nil, "", fmt.Errorf("%w: %v", ErrInvalidPath, nameErr)
		}
		if statErr != nil {
			return nil, "", fmt.Errorf("app %q: stat: %w", name, statErr)
		}
		return nil, "", fmt.Errorf("app %q: open directory: %w", name, err)
	}
	keepAppRoot := false
	defer func() {
		if !keepAppRoot {
			_ = appRoot.Close()
		}
	}()
	openedInfo, err := appRoot.Stat(".")
	if err != nil {
		return nil, "", fmt.Errorf("app %q: stat opened directory: %w", name, err)
	}
	if !openedInfo.IsDir() || !os.SameFile(info, openedInfo) {
		return nil, "", fmt.Errorf("app %q: directory changed during open", name)
	}
	currentInfo, err := appsRoot.Lstat(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, "", fmt.Errorf("app %q: %w", name, fs.ErrNotExist)
		}
		return nil, "", fmt.Errorf("app %q: recheck directory: %w", name, err)
	}
	if currentInfo.Mode()&os.ModeSymlink != 0 {
		return nil, "", fmt.Errorf("%w: %q changed to a symlink", ErrInvalidPath, name)
	}
	if !currentInfo.IsDir() {
		return nil, "", fmt.Errorf("%w: %q changed to a non-directory", ErrInvalidPath, name)
	}
	if !os.SameFile(openedInfo, currentInfo) {
		return nil, "", fmt.Errorf("app %q: directory changed during open", name)
	}
	if nameErr != nil {
		_, statErr := appRoot.Lstat(TemplateFile)
		if errors.Is(statErr, fs.ErrNotExist) {
			return nil, "", ErrNotReady
		}
		return nil, "", fmt.Errorf("%w: %v", ErrInvalidPath, nameErr)
	}
	keepAppRoot = true
	return appRoot, filepath.Join(root, name), nil
}

// LoadSingle loads exactly one app from root by name, enforcing the same
// discovery policy as Loader.Load: a legal single-element name and a real, direct
// child directory of root that is not a symlink. It surfaces the cases
// distinctly so the reconciler can decide how to act: a missing directory wraps
// fs.ErrNotExist (a removal), an invalid name/symlink/non-directory is
// ErrInvalidPath (retain, never replace), a missing template.yaml is ErrNotReady
// (the directory is not an app candidate), an invalid template returns a parse
// error, and unreadable templates surface as an error too.
func LoadSingle(root, name string) (App, error) {
	source, err := ReadTemplateSource(root, name)
	if err != nil {
		return App{}, err
	}
	return source.Parse()
}

// ReadTemplateSource resolves and reads exactly one template.yaml without
// parsing it. It applies the same path policy as LoadSingle and returns the same
// error taxonomy (fs.ErrNotExist for a removal, ErrInvalidPath, ErrNotReady, and
// a wrapped read error), but it surfaces the successfully read bytes plus their
// stable identity so a caller can decide whether parsing is necessary. Only
// BYTES THAT WERE ACTUALLY READ can produce an identity: a failed or ambiguous
// read never yields one.
func ReadTemplateSource(root, name string) (TemplateSource, error) {
	appRoot, dir, err := resolveAppDir(root, name)
	if err != nil {
		return TemplateSource{}, err
	}
	defer appRoot.Close()
	info, err := appRoot.Lstat(TemplateFile)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return TemplateSource{}, ErrNotReady
		}
		return TemplateSource{}, fmt.Errorf("app %q: stat template: %w", name, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return TemplateSource{}, fmt.Errorf("%w: app %q template is not a regular non-symlink file", ErrInvalidPath, name)
	}
	f, err := appRoot.Open(TemplateFile)
	if err != nil {
		currentInfo, statErr := appRoot.Lstat(TemplateFile)
		if statErr == nil && (currentInfo.Mode()&os.ModeSymlink != 0 || !currentInfo.Mode().IsRegular()) {
			return TemplateSource{}, fmt.Errorf("%w: app %q template changed to a non-regular or symlinked file", ErrInvalidPath, name)
		}
		if errors.Is(statErr, fs.ErrNotExist) {
			return TemplateSource{}, ErrNotReady
		}
		if statErr != nil {
			return TemplateSource{}, fmt.Errorf("app %q: recheck template: %w", name, statErr)
		}
		if errors.Is(err, fs.ErrNotExist) {
			// The entry is regular now, but the open raced with a replacement.
			// Do not wrap ENOENT: this is retryable, not a removal or invalid input.
			return TemplateSource{}, fmt.Errorf("app %q: template changed during open (open error: %v)", name, err)
		}
		return TemplateSource{}, fmt.Errorf("app %q: read template: %w", name, err)
	}
	defer f.Close()
	openedInfo, err := f.Stat()
	if err != nil {
		return TemplateSource{}, fmt.Errorf("app %q: stat opened template: %w", name, err)
	}
	// Recheck the path before deciding whether an identity mismatch is invalid
	// input or a concurrent regular-file replacement. Symlinks and other
	// non-regular observations remain invalid; a different regular inode is an
	// operational race and must be retried without claiming an invalid identity.
	currentInfo, err := appRoot.Lstat(TemplateFile)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return TemplateSource{}, ErrNotReady
		}
		return TemplateSource{}, fmt.Errorf("app %q: recheck template: %w", name, err)
	}
	if currentInfo.Mode()&os.ModeSymlink != 0 || !currentInfo.Mode().IsRegular() || !openedInfo.Mode().IsRegular() {
		return TemplateSource{}, fmt.Errorf("%w: app %q template changed during read", ErrInvalidPath, name)
	}
	if !os.SameFile(info, openedInfo) || !os.SameFile(currentInfo, openedInfo) {
		return TemplateSource{}, fmt.Errorf("app %q: template changed during read", name)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return TemplateSource{}, fmt.Errorf("app %q: read template: %w", name, err)
	}
	return TemplateSource{
		Name:     name,
		Dir:      dir,
		Data:     data,
		Identity: TemplateIdentity(data),
	}, nil
}

// Load returns the successfully loaded apps. Direct-child files and symlinks
// are unsupported root entries and are silently skipped, as are real app
// directories without template.yaml and Relay-owned transient staging
// directories (IsReservedDir). Invalid names, unreadable or invalid templates,
// and symlinked or non-regular template.yaml entries inside real app directories
// are logged and skipped without aborting the load. Each app is loaded through
// LoadSingle, so startup discovery and live reload apply the same path policy.
// Callers that must observe invalid entries (rather than only skip them) use
// LoadWithDiagnostics.
func (l *Loader) Load() ([]App, error) {
	apps, issues, err := l.load(nil)
	if err != nil {
		return nil, err
	}
	l.logIssues(issues)
	return apps, nil
}

// logIssues emits the historical warning for every present-but-invalid entry.
// Directories without a template.yaml are not app candidates and never reach
// this list. Both Load and LoadWithDiagnostics share it so the diagnostic
// variant keeps exactly the logging Load has always produced. A Recorded issue
// is an already-persisted deterministic invalid identity whose parse was
// skipped: it must not produce a duplicate warning on every scan/restart.
func (l *Loader) logIssues(issues []LoadIssue) {
	for _, issue := range issues {
		if issue.Recorded {
			continue
		}
		l.log.Warn("App: invalid; skipping", "app", issue.Name, "error", issue.Err)
	}
}

// LoadWithDiagnostics loads exactly like Load — returning the same valid
// apps, with the same logging and the same root-read error semantics — but
// ALSO returns LoadIssues for real direct-child app directories that could not
// be loaded (for example, an invalid name, an unreadable or invalid template, or
// a symlinked/non-regular template.yaml). Direct-child files and symlinks are
// unsupported root entries and are skipped, not reported; a directory without
// template.yaml is likewise not an app candidate. Relay-owned transient staging
// directories (IsReservedDir) are ignored entirely. A directory that VANISHED
// between the directory read and its load is not a desired definition and is
// deliberately omitted, so a caller never mistakes a transient absence for an
// invalid desired state. An ambiguous directory-inspection failure is reported
// as an issue so it cannot be mistaken for absence.
//
// A root read error is returned as a fatal error (nil apps), exactly as
// Load does: the loader cannot even enumerate the entries, so there is nothing
// to report. The invalid entries stay OUT of the returned apps, so a caller
// that persists issues and prepares the valid set never executes or schedules an
// invalid definition.
func (l *Loader) LoadWithDiagnostics() ([]App, []LoadIssue, error) {
	apps, issues, err := l.load(nil)
	if err != nil {
		return nil, nil, err
	}
	l.logIssues(issues)
	return apps, issues, nil
}

// LoadWithDiagnosticsUsing is LoadWithDiagnostics with an optional
// TemplateParseGate. The gate is consulted once per readable template, AFTER the
// bytes are read and hashed but BEFORE they are parsed, so a caller can skip a
// deterministic parse/validation failure whose exact identity is already
// persisted. A skipped entry is still returned as a Recorded LoadIssue (so the
// caller can track the app for removal) but is not logged. A nil gate is
// exactly LoadWithDiagnostics.
func (l *Loader) LoadWithDiagnosticsUsing(gate TemplateParseGate) ([]App, []LoadIssue, error) {
	apps, issues, err := l.load(gate)
	if err != nil {
		return nil, nil, err
	}
	l.logIssues(issues)
	return apps, issues, nil
}

// load is the shared loader body: it skips unsupported direct-child files and
// symlinks, then reads each real direct-child directory through
// ReadTemplateSource (optionally consulting the caller's parse gate) and
// classifies the outcome. It returns loaded apps plus issues for invalid names,
// unreadable/invalid templates, and symlinked or non-regular template.yaml
// entries. Relay-owned transient staging directories (IsReservedDir) are skipped
// entirely; vanished entries (fs.ErrNotExist) and directories without
// template.yaml (ErrNotReady) are neither apps nor issues. Ambiguous directory
// inspection failures are issues rather than presumed removals. Callers own the
// logging and error semantics.
func (l *Loader) load(gate TemplateParseGate) ([]App, []LoadIssue, error) {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return nil, nil, fmt.Errorf("read apps dir %q: %w", l.dir, err)
	}

	var apps []App
	var issues []LoadIssue
	for _, entry := range entries {
		name := entry.Name()
		if IsReservedDir(name) {
			// A Relay-owned transient staging directory (git's ".sync-*") is
			// never an app and never a desired definition: it is skipped
			// entirely — no app, no issue, no warning — so a live sync
			// cannot surface a spurious invalid/unavailable state.
			continue
		}
		dir := filepath.Join(l.dir, name)
		realDir, dirErr := IsRealAppDir(dir)
		if dirErr != nil {
			// The entry was enumerated but its current type could not be checked.
			// Keep the ambiguity visible rather than silently turning it into a
			// missing desired app.
			issues = append(issues, LoadIssue{Name: name, Err: fmt.Errorf("app %q: inspect directory: %w", name, dirErr)})
			continue
		}
		if !realDir {
			// Files, symlinks, and broken links are unsupported root entries,
			// not app candidates.
			continue
		}
		// The gate snapshots persisted state BEFORE the read, so a later
		// invalid observation can be rejected if another worker advances the
		// app while this scan is still running.
		if gate != nil {
			gate.Observe(name)
		}
		source, err := ReadTemplateSource(l.dir, name)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// The directory vanished between the read and the load: it is not
				// a present desired definition, so it is neither an app nor an
				// issue (a removal, not an invalid state).
				continue
			}
			if errors.Is(err, ErrNotReady) {
				// No template.yaml: not an app candidate, so it is neither an app
				// nor an invalid desired definition. It may become one on a later
				// event (the watcher/reload path), but this pass has nothing to
				// report.
				continue
			}
			// An invalid name, invalid template entry (including a symlink), or
			// unreadable template is a candidate that cannot be loaded. Report it
			// as an issue; the caller decides how to record it.
			issues = append(issues, LoadIssue{Name: name, Err: err})
			continue
		}
		if gate != nil {
			if skip, gateErr := gate.SkipParse(name, source.Identity); skip {
				// The exact bytes are already persisted as a deterministic
				// invalid desired definition. Report the already-recorded issue
				// for tracking but do not parse, log, or re-record it.
				issues = append(issues, LoadIssue{
					Name:     name,
					Err:      gateErr,
					Identity: source.Identity,
					Recorded: true,
				})
				continue
			}
		}
		fn, err := source.Parse()
		if err != nil {
			identity, _ := InvalidTemplateIdentity(err)
			// An invalid template is a present desired definition that cannot be
			// loaded; Identity is non-empty only for a deterministic
			// parse/validation failure of the bytes that were actually read.
			issues = append(issues, LoadIssue{Name: name, Err: err, Identity: identity})
			continue
		}
		apps = append(apps, fn)
	}

	return apps, issues, nil
}
