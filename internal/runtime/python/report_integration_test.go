//go:build integration

// Per-invocation REPORT tests for the split Python bootstrap: the pure
// cgroup path/value/sampling seams of _relay_memory.py and the format and
// emission seams of _relay_report.py, driven with fixture inputs (never a
// host cgroup layout), plus the warm bootstrap's stdout contract — one
// REPORT line after the logical id's END line on success, handled failure,
// and warm reuse alike. See _relay_memory.py and _relay_report.py; the Node
// mirrors live in the node package.
package python

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// reportPaths mirrors the JSON form of _relay_memory.MemoryPaths/one Node
// candidate pair: the resolved file paths of the current and peak counters.
type reportPaths struct {
	Current string `json:"current"`
	Peak    string `json:"peak"`
}

func intValue(v int64) *int64 { return &v }

// mountinfoLine renders one mountinfo record with the given root, mount
// point, filesystem type, mount source, and super-block options — the fields
// the cgroup parsers read, in the kernel's field order.
func mountinfoLine(root, point, fstype, source, superOpts string) string {
	return fmt.Sprintf("105 99 0:31 %s %s rw,nosuid - %s %s %s",
		root, point, fstype, source, superOpts)
}

// mountinfoLineOptional is mountinfoLine with optional per-mount fields
// (shared/master tags) between the mount options and the "-" separator, the
// layout real kernels emit.
func mountinfoLineOptional(root, point, fstype, source, superOpts string) string {
	return fmt.Sprintf("105 99 0:31 %s %s rw,nosuid shared:7 master:1 - %s %s %s",
		root, point, fstype, source, superOpts)
}

// reportDriverSource is the fixture-only driver for _relay_memory and
// _relay_report: it reads one JSON op on stdin and answers with one JSON line
// on stdout (plus at most one REPORT line, when the op emits through the
// shared lifecycle writer). No op touches a host cgroup.
const reportDriverSource = `
import json
import sys

import _relay_lifecycle as lifecycle
import _relay_memory as memory
import _relay_report as report


def usage_of(spec):
    return memory.MemoryUsage(
        spec.get("current_bytes"), spec.get("peak_bytes")
    )


def main():
    spec = json.load(sys.stdin)
    op = spec["op"]
    out = {}
    if op == "resolve":
        paths = memory.resolve_memory_paths(spec["cgroup"], spec["mountinfo"])
        out["paths"] = [
            {"current": candidate.current, "peak": candidate.peak}
            for candidate in paths
        ]
    elif op == "read":
        out["value"] = memory.read_memory_value(spec["path"])
    elif op == "read_denied":
        import builtins

        real_open = builtins.open

        def deny(path, *args, **kwargs):
            if path == spec["path"]:
                raise PermissionError(13, "Permission denied", path)
            return real_open(path, *args, **kwargs)

        builtins.open = deny
        try:
            out["value"] = memory.read_memory_value(spec["path"])
        finally:
            builtins.open = real_open
    elif op == "sample":
        candidates = tuple(
            memory.MemoryPaths(p["current"], p["peak"])
            for p in (spec["paths"] or [])
        )
        usage = memory.sample_memory(
            candidates[0] if spec.get("single") else candidates
        )
        out["usage"] = {
            "current_bytes": usage.current_bytes,
            "peak_bytes": usage.peak_bytes,
        }
    elif op == "format":
        out["line"] = report.format_report(
            spec["request_id"], spec["duration_ms"], usage_of(spec)
        )
    elif op == "report":
        report.report_invocation(
            spec["request_id"], spec["duration_ms"], usage_of(spec)
        )
        out["emitted"] = True
    elif op == "report_guarded":
        class Broken:
            def write(self, line):
                raise OSError("stdout gone")

            def flush(self):
                raise OSError("stdout gone")

        lifecycle._STDOUT = Broken()
        report.report_invocation(
            spec["request_id"], None, memory.MemoryUsage(1048576, None)
        )
        report.report_invocation(
            spec["request_id"], 1.5, memory.MemoryUsage(None, None)
        )
        out["survived"] = True
    else:
        raise SystemExit("unknown op: " + op)
    print(json.dumps(out))


main()
`

// runReportDriver materializes the embedded runtime modules beside the
// driver, feeds it one JSON op on stdin, and returns the REPORT lines the
// module wrote plus the driver's final JSON payload line. Any other stdout
// line is a contract break and fails the test.
func runReportDriver(t *testing.T, spec map[string]any) ([]string, json.RawMessage) {
	t.Helper()
	dir := t.TempDir()
	writeEmbeddedRuntimeModules(t, dir)
	driver := filepath.Join(dir, "report_driver.py")
	if err := os.WriteFile(driver, []byte(reportDriverSource), 0o644); err != nil {
		t.Fatalf("write report driver: %v", err)
	}
	body, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal driver spec: %v", err)
	}
	cmd := exec.Command("python3", driver)
	cmd.Env = append(os.Environ(), "PYTHONPATH="+dir)
	cmd.Stdin = bytes.NewReader(body)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("run report driver %q: %v\nstderr:\n%s\nstdout:\n%s",
			spec["op"], err, stderr.String(), stdout.String())
	}
	return splitReportDriverOutput(t, stdout.String())
}

// splitReportDriverOutput splits the driver's stdout into the REPORT lines
// written before the single trailing JSON payload line.
func splitReportDriverOutput(t *testing.T, out string) ([]string, json.RawMessage) {
	t.Helper()
	trimmed := strings.TrimSuffix(out, "\n")
	if trimmed == "" {
		t.Fatal("report driver produced no output")
	}
	lines := strings.Split(trimmed, "\n")
	payload := json.RawMessage(lines[len(lines)-1])
	if !json.Valid(payload) {
		t.Fatalf("driver's last stdout line is not JSON: %q", payload)
	}
	var reports []string
	for _, line := range lines[:len(lines)-1] {
		if !strings.HasPrefix(line, "REPORT ") {
			t.Fatalf("driver stdout line is not a REPORT line: %q", line)
		}
		reports = append(reports, line)
	}
	return reports, payload
}

func decodeReportPayload(t *testing.T, payload json.RawMessage, dst any) {
	t.Helper()
	if err := json.Unmarshal(payload, dst); err != nil {
		t.Fatalf("decode driver payload %s: %v", payload, err)
	}
}

// countLinesWithPrefix counts the stdout lines starting with prefix.
func countLinesWithPrefix(out, prefix string) int {
	count := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, prefix) {
			count++
		}
	}
	return count
}

// requireInvocationReport pins one invocation's REPORT contract: exactly one
// REPORT line in the window, written AFTER the logical id's END line,
// carrying the LOGICAL requestId (never the transport id) and a two-decimal
// monotonic duration; each memory field appears only when measurable, as a
// whole-MiB value.
func requireInvocationReport(t *testing.T, window, logical, transportID string) {
	t.Helper()
	pattern := regexp.MustCompile(`^REPORT RequestId: ` +
		regexp.QuoteMeta(logical) + ` Duration: [0-9]+\.[0-9]{2} ms` +
		`( Memory Used: [0-9]+ MiB)?( Container Memory Peak: [0-9]+ MiB)?$`)
	end := lifecycleLineIndex(window, "END RequestId: "+logical)
	if end == -1 {
		t.Fatalf("window must carry END for %q before its REPORT:\n%s",
			logical, window)
	}
	reports := 0
	for i, line := range strings.Split(window, "\n") {
		if !strings.HasPrefix(line, "REPORT RequestId: ") {
			continue
		}
		reports++
		if !pattern.MatchString(line) {
			t.Errorf("REPORT line = %q, want the contract line for %q",
				line, logical)
		}
		if i <= end {
			t.Errorf("REPORT must follow END (report at %d, END at %d):\n%s",
				i, end, window)
		}
	}
	if reports != 1 {
		t.Errorf("REPORT lines in the window = %d, want exactly 1:\n%s",
			reports, window)
	}
	if n := countLinesWithPrefix(window, "REPORT RequestId: "+transportID); n != 0 {
		t.Errorf("REPORT must not carry the transport id %q (%d line(s)):\n%s",
			transportID, n, window)
	}
}

// reportPeakMiB extracts the lifetime-peak field from the window's REPORT
// line for the logical id, reporting whether the field was observed at all
// (a host without cgroup metrics omits it).
func reportPeakMiB(t *testing.T, window, logical string) (int64, bool) {
	t.Helper()
	pattern := regexp.MustCompile(`Container Memory Peak: ([0-9]+) MiB`)
	for _, line := range strings.Split(window, "\n") {
		if !strings.HasPrefix(line, "REPORT RequestId: "+logical+" ") {
			continue
		}
		m := pattern.FindStringSubmatch(line)
		if m == nil {
			return 0, false
		}
		peak, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			t.Fatalf("peak field %q is not a number: %v", m[1], err)
		}
		return peak, true
	}
	t.Fatalf("window carries no REPORT line for %q:\n%s", logical, window)
	return 0, false
}

// TestPythonCgroupMemoryPathFixtures pins the pure candidate resolution:
// the process text and mountinfo fixtures map to the exact current/peak file
// paths for cgroup v2 and the v1 memory controller — v2 candidates listed
// before v1 when both hierarchies are present — with optional mountinfo
// fields tolerated, mount-root rebasing, octal escapes decoded, and unsafe or
// malformed layouts yielding no candidates.
func TestPythonCgroupMemoryPathFixtures(t *testing.T) {
	requirePython(t)
	v2Mount := mountinfoLine("/", "/sys/fs/cgroup", "cgroup2", "cgroup", "rw")
	v1Mount := mountinfoLine("/", "/sys/fs/cgroup/memory",
		"cgroup", "cgroup", "rw,memory")
	v2Files := []reportPaths{{
		Current: "/sys/fs/cgroup/docker/app/memory.current",
		Peak:    "/sys/fs/cgroup/docker/app/memory.peak",
	}}
	v1Files := []reportPaths{{
		Current: "/sys/fs/cgroup/memory/docker/app/memory.usage_in_bytes",
		Peak:    "/sys/fs/cgroup/memory/docker/app/memory.max_usage_in_bytes",
	}}
	cases := []struct {
		name      string
		cgroup    string
		mountinfo string
		want      []reportPaths
	}{
		{
			name:      "cgroup v2 maps under the unified mount",
			cgroup:    "0::/docker/app\n",
			mountinfo: v2Mount,
			want:      v2Files,
		},
		{
			name:      "cgroup v1 maps through the memory mount",
			cgroup:    "11:pids:/foo\n2:memory:/docker/app\n",
			mountinfo: v1Mount,
			want:      v1Files,
		},
		{
			name:      "v2 candidates lead when both hierarchies are present",
			cgroup:    "2:memory:/docker/app\n0::/docker/app\n",
			mountinfo: v2Mount + "\n" + v1Mount,
			want:      append(append([]reportPaths{}, v2Files...), v1Files...),
		},
		{
			name:   "optional mountinfo fields before the separator are tolerated",
			cgroup: "0::/docker/app\n",
			mountinfo: mountinfoLineOptional("/", "/sys/fs/cgroup",
				"cgroup2", "cgroup", "rw"),
			want: v2Files,
		},
		{
			name:   "the mount root rebases the membership directory",
			cgroup: "0::/docker/app/sub\n",
			mountinfo: mountinfoLine("/docker/app", "/sys/fs/cgroup",
				"cgroup2", "cgroup", "rw"),
			want: []reportPaths{{
				Current: "/sys/fs/cgroup/sub/memory.current",
				Peak:    "/sys/fs/cgroup/sub/memory.peak",
			}},
		},
		{
			name:   "octal escapes in the mount point are decoded",
			cgroup: "0::/docker/app\n",
			mountinfo: mountinfoLine("/", "/sys/fs/cgroup/my\\040grp",
				"cgroup2", "cgroup", "rw"),
			want: []reportPaths{{
				Current: "/sys/fs/cgroup/my grp/docker/app/memory.current",
				Peak:    "/sys/fs/cgroup/my grp/docker/app/memory.peak",
			}},
		},
		{
			name:   "membership outside the mount root is rejected",
			cgroup: "0::/elsewhere\n",
			mountinfo: mountinfoLine("/docker/app", "/sys/fs/cgroup",
				"cgroup2", "cgroup", "rw"),
			want: nil,
		},
		{
			name:      "membership escaping the mount point resolves to nothing",
			cgroup:    "0::/../../../../etc\n",
			mountinfo: v2Mount,
			want:      nil,
		},
		{
			name:      "no memory controller membership resolves to nothing",
			cgroup:    "11:pids:/foo\n",
			mountinfo: v1Mount,
			want:      nil,
		},
		{
			name:   "a v1 mount without the memory controller is skipped",
			cgroup: "2:memory:/docker/app\n",
			mountinfo: mountinfoLine("/", "/sys/fs/cgroup/cpuset",
				"cgroup", "cgroup", "rw,cpuset"),
			want: nil,
		},
		{
			name:   "a v1 mount with source memory but no memory options is rejected",
			cgroup: "2:memory:/docker/app\n",
			mountinfo: mountinfoLine("/", "/sys/fs/cgroup/memory",
				"cgroup", "memory", "rw"),
			want: nil,
		},
		{
			name:      "malformed mountinfo lines are skipped",
			cgroup:    "0::/docker/app\n",
			mountinfo: "not a mountinfo line\ntoo few - fields",
			want:      nil,
		},
		{
			name:      "empty process text resolves to nothing",
			cgroup:    "",
			mountinfo: "",
			want:      nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reports, payload := runReportDriver(t, map[string]any{
				"op":        "resolve",
				"cgroup":    tc.cgroup,
				"mountinfo": tc.mountinfo,
			})
			if len(reports) != 0 {
				t.Fatalf("resolve must emit no REPORT lines, got %v", reports)
			}
			var got struct {
				Paths []reportPaths `json:"paths"`
			}
			decodeReportPayload(t, payload, &got)
			if len(got.Paths) != len(tc.want) {
				t.Fatalf("candidates = %+v, want %+v", got.Paths, tc.want)
			}
			for i := range tc.want {
				if got.Paths[i] != tc.want[i] {
					t.Errorf("candidate %d = %+v, want %+v",
						i, got.Paths[i], tc.want[i])
				}
			}
		})
	}
}

// TestPythonReadMemoryValueFixtureFiles pins the byte-value reader against
// fixture files: a decimal counter round-trips, while a missing file, an
// unreadable path (a directory standing in for the file — deterministic for
// every user, unlike permission bits), a deterministic PermissionError, and
// malformed or negative content all omit the value instead of fabricating
// one.
func TestPythonReadMemoryValueFixtureFiles(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	write := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write fixture %s: %v", name, err)
		}
		return path
	}
	unreadable := filepath.Join(dir, "directory")
	if err := os.Mkdir(unreadable, 0o755); err != nil {
		t.Fatalf("mkdir fixture: %v", err)
	}
	cases := []struct {
		name string
		path string
		want *int64
	}{
		{"a decimal byte counter", write("current", "5242880\n"), intValue(5242880)},
		{"a zero counter", write("zero", "0"), intValue(0)},
		{"a missing file", filepath.Join(dir, "missing"), nil},
		{"an unreadable path", unreadable, nil},
		{"a malformed counter", write("malformed", "not-a-number"), nil},
		{"a counter with embedded junk", write("junk", "12 34"), nil},
		{"a fractional counter", write("fraction", "1.5"), nil},
		{"a negative counter", write("negative", "-4096"), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reports, payload := runReportDriver(t, map[string]any{
				"op":   "read",
				"path": tc.path,
			})
			if len(reports) != 0 {
				t.Fatalf("read must emit no REPORT lines, got %v", reports)
			}
			var got struct {
				Value *int64 `json:"value"`
			}
			decodeReportPayload(t, payload, &got)
			switch {
			case tc.want == nil && got.Value != nil:
				t.Errorf("value = %d, want none", *got.Value)
			case tc.want != nil && got.Value == nil:
				t.Errorf("value = none, want %d", *tc.want)
			case tc.want != nil && *got.Value != *tc.want:
				t.Errorf("value = %d, want %d", *got.Value, *tc.want)
			}
		})
	}

	// A deterministic PermissionError: the fixture file exists and is
	// readable, but the driver's patched builtins.open denies exactly that
	// path — chmod bits alone would not be deterministic (a CI root can
	// read anything).
	denied := write("denied", "4096")
	reports, payload := runReportDriver(t, map[string]any{
		"op": "read_denied", "path": denied,
	})
	if len(reports) != 0 {
		t.Fatalf("a denied read must emit no REPORT lines, got %v", reports)
	}
	var deniedResult struct {
		Value *int64 `json:"value"`
	}
	decodeReportPayload(t, payload, &deniedResult)
	if deniedResult.Value != nil {
		t.Errorf("a PermissionError value = %d, want none", *deniedResult.Value)
	}
}

// TestPythonSampleMemoryFixtureFiles pins same-candidate sampling: both
// counters come from the first candidate (resolved in v2-first preference
// order) that yields either value, so the other field stays unavailable
// rather than mixed in from a different cgroup, and a candidate yielding
// neither falls through to the next — v1 only when v2 yields nothing. The
// two counters stay distinct raw byte values; unavailable paths omit
// everything.
func TestPythonSampleMemoryFixtureFiles(t *testing.T) {
	requirePython(t)
	const mib = int64(1024 * 1024)
	dir := t.TempDir()
	write := func(name string, body int64) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(fmt.Sprint(body)), 0o644); err != nil {
			t.Fatalf("write fixture %s: %v", name, err)
		}
		return path
	}
	current := write("current", 5*mib+100)
	peak := write("peak", 12*mib-1)
	missing := filepath.Join(dir, "missing")
	cases := []struct {
		name        string
		paths       []reportPaths
		single      bool
		wantCurrent *int64
		wantPeak    *int64
	}{
		{
			name:        "current and peak stay distinct",
			paths:       []reportPaths{{current, peak}},
			wantCurrent: intValue(5*mib + 100), wantPeak: intValue(12*mib - 1),
		},
		{
			name:  "a single MemoryPaths object is accepted",
			paths: []reportPaths{{current, peak}}, single: true,
			wantCurrent: intValue(5*mib + 100), wantPeak: intValue(12*mib - 1),
		},
		{
			name:        "a missing peak omits only the peak",
			paths:       []reportPaths{{current, missing}},
			wantCurrent: intValue(5*mib + 100),
		},
		{
			name:  "missing files omit every value",
			paths: []reportPaths{{missing, missing}},
		},
		{
			name: "no candidates omit every value",
		},
		{
			name: "the current stays unavailable rather than mixed in",
			paths: []reportPaths{
				{missing, peak},
				{current, missing},
			},
			wantPeak: intValue(12*mib - 1),
		},
		{
			name: "the peak stays unavailable rather than mixed in",
			paths: []reportPaths{
				{current, missing},
				{missing, peak},
			},
			wantCurrent: intValue(5*mib + 100),
		},
		{
			name: "the v1 candidate serves only when v2 yields neither value",
			paths: []reportPaths{
				{missing, missing},
				{current, peak},
			},
			wantCurrent: intValue(5*mib + 100), wantPeak: intValue(12*mib - 1),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reports, payload := runReportDriver(t, map[string]any{
				"op":     "sample",
				"paths":  tc.paths,
				"single": tc.single,
			})
			if len(reports) != 0 {
				t.Fatalf("sample must emit no REPORT lines, got %v", reports)
			}
			var got struct {
				Usage struct {
					CurrentBytes *int64 `json:"current_bytes"`
					PeakBytes    *int64 `json:"peak_bytes"`
				} `json:"usage"`
			}
			decodeReportPayload(t, payload, &got)
			assertOptionalInt64(t, "current_bytes", got.Usage.CurrentBytes, tc.wantCurrent)
			assertOptionalInt64(t, "peak_bytes", got.Usage.PeakBytes, tc.wantPeak)
		})
	}

	// Sampling is read-only: one or repeated samples never write or reset
	// the lifetime-peak counter file — the kernel owns that counter and the
	// report only reads it.
	before, err := os.ReadFile(peak)
	if err != nil {
		t.Fatalf("read peak fixture: %v", err)
	}
	for i := 0; i < 2; i++ {
		runReportDriver(t, map[string]any{
			"op":    "sample",
			"paths": []reportPaths{{current, peak}},
		})
	}
	after, err := os.ReadFile(peak)
	if err != nil {
		t.Fatalf("re-read peak fixture: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("sampling altered the lifetime-peak counter: %q -> %q",
			before, after)
	}
}

func assertOptionalInt64(t *testing.T, field string, got, want *int64) {
	t.Helper()
	switch {
	case want == nil && got != nil:
		t.Errorf("%s = %d, want none", field, *got)
	case want != nil && got == nil:
		t.Errorf("%s = none, want %d", field, *want)
	case want != nil && *got != *want:
		t.Errorf("%s = %d, want %d", field, *got, *want)
	}
}

// TestPythonReportFormatFixtureValues pins the rendered report line: the
// logical request id, a two-decimal duration, and byte counters floored to
// whole MiB in their current/peak slots — with each memory field omitted
// when its value is unavailable.
func TestPythonReportFormatFixtureValues(t *testing.T) {
	requirePython(t)
	const mib = 1024 * 1024
	cases := []struct {
		name      string
		requestID string
		duration  float64
		current   *int64
		peak      *int64
		want      string
	}{
		{
			name:      "id, duration, current and peak in one line",
			requestID: "req-42", duration: 12.5,
			current: intValue(5 * mib), peak: intValue(12 * mib),
			want: "REPORT RequestId: req-42 Duration: 12.50 ms" +
				" Memory Used: 5 MiB Container Memory Peak: 12 MiB",
		},
		{
			name:      "bytes round down to whole MiB",
			requestID: "req-1", duration: 1,
			current: intValue(2*mib - 1), peak: intValue(mib),
			want: "REPORT RequestId: req-1 Duration: 1.00 ms" +
				" Memory Used: 1 MiB Container Memory Peak: 1 MiB",
		},
		{
			name:      "sub-MiB usage reports zero",
			requestID: "req-2", duration: 0.5,
			current: intValue(1023), peak: intValue(0),
			want: "REPORT RequestId: req-2 Duration: 0.50 ms" +
				" Memory Used: 0 MiB Container Memory Peak: 0 MiB",
		},
		{
			name:      "an unavailable current omits only its field",
			requestID: "req-3", duration: 3, peak: intValue(4 * mib),
			want: "REPORT RequestId: req-3 Duration: 3.00 ms" +
				" Container Memory Peak: 4 MiB",
		},
		{
			name:      "unavailable memory omits both fields",
			requestID: "req-4", duration: 0,
			want: "REPORT RequestId: req-4 Duration: 0.00 ms",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, payload := runReportDriver(t, map[string]any{
				"op":            "format",
				"request_id":    tc.requestID,
				"duration_ms":   tc.duration,
				"current_bytes": tc.current,
				"peak_bytes":    tc.peak,
			})
			var got struct {
				Line string `json:"line"`
			}
			decodeReportPayload(t, payload, &got)
			if got.Line != tc.want {
				t.Errorf("line = %q, want %q", got.Line, tc.want)
			}
		})
	}
}

// TestPythonReportEmissionIsGuarded pins the emission seam: a report writes
// exactly one line through the shared lifecycle writer (whose captured native
// stdout the driver replaces to simulate a broken stream), while a failing
// format (a non-numeric duration) and a broken stream are both swallowed
// without raising into the invocation path.
func TestPythonReportEmissionIsGuarded(t *testing.T) {
	requirePython(t)
	const mib = 1024 * 1024
	reports, payload := runReportDriver(t, map[string]any{
		"op":            "report",
		"request_id":    "emit-1",
		"duration_ms":   7.25,
		"current_bytes": 2 * mib,
		"peak_bytes":    9 * mib,
	})
	want := "REPORT RequestId: emit-1 Duration: 7.25 ms" +
		" Memory Used: 2 MiB Container Memory Peak: 9 MiB"
	if len(reports) != 1 || reports[0] != want {
		t.Errorf("report lines = %q, want [%q]", reports, want)
	}
	var emitted struct {
		Emitted bool `json:"emitted"`
	}
	decodeReportPayload(t, payload, &emitted)
	if !emitted.Emitted {
		t.Error("driver must reach the end of the report op")
	}

	reports, payload = runReportDriver(t, map[string]any{
		"op": "report_guarded", "request_id": "guard-1",
	})
	if len(reports) != 0 {
		t.Errorf("a guarded report must write nothing, got %q", reports)
	}
	var guarded struct {
		Survived bool `json:"survived"`
	}
	decodeReportPayload(t, payload, &guarded)
	if !guarded.Survived {
		t.Error("report emission must never raise into the invocation path")
	}
}

// TestPythonRuntimeInvocationReportLine pins the warm bootstrap's stdout
// contract: every completed invocation — success, warm reuse, and handled
// failure alike — emits exactly one REPORT line after its logical END,
// under the logical requestId and never the transport id; no REPORT appears
// before the first request; and a handler that rebinds sys.stdout cannot
// swallow the report or change the result.
func TestPythonRuntimeInvocationReportLine(t *testing.T) {
	requirePython(t)
	dir := t.TempDir()
	writeHandler(t, dir, "handler", `
import io
import sys


def show(event, context):
    print("app out " + event["msg"])


def boom(event, context):
    raise ValueError("handler-kaboom")


def mute(event, context):
    sys.stdout = io.StringIO()
    print("hidden")
`)
	p := startPython(t, dir)
	p.awaitStdoutQuiet()
	if n := countLinesWithPrefix(p.stdoutString(), "REPORT "); n != 0 {
		t.Fatalf("REPORT lines before the first request = %d, want 0; stdout:\n%s",
			n, p.stdoutString())
	}

	// Success: the report follows END in the invocation's window.
	logical1 := "report-success-1"
	r1 := p.invokeFrame(t, "handler.show", `{"msg":"a"}`, nil, nil,
		logicalRequestContext("handler.show", logical1, 1, 3))
	if !r1.OK {
		t.Fatalf("invoke 1 failed: %s", r1.Err)
	}
	requireTransportEcho(t, r1, "t0")
	requireInvocationReport(t, r1.Out, logical1, "t0")

	// Warm reuse: the same process reports a second invocation under its
	// own logical id.
	logical2 := "report-success-2"
	r2 := p.invokeFrame(t, "handler.show", `{"msg":"b"}`, nil, nil,
		logicalRequestContext("handler.show", logical2, 1, 3))
	if !r2.OK {
		t.Fatalf("invoke 2 failed: %s", r2.Err)
	}
	requireTransportEcho(t, r2, "t1")
	requireInvocationReport(t, r2.Out, logical2, "t1")

	// Warm reuse must not reset the lifetime peak: when the first report
	// observed a peak, the reused process's next report still observes one
	// at least as large — memory.peak is lifetime, never per-invocation.
	peak1, hasPeak1 := reportPeakMiB(t, r1.Out, logical1)
	peak2, hasPeak2 := reportPeakMiB(t, r2.Out, logical2)
	if hasPeak1 && !hasPeak2 {
		t.Error("the warm reuse's report must still carry the lifetime peak")
	} else if hasPeak1 && peak2 < peak1 {
		t.Errorf("lifetime peak went from %d MiB to %d MiB across warm reuse",
			peak1, peak2)
	}

	// Handled failure: a caught error is still a completed invocation with
	// its own report, and the result stays ok:false.
	logical3 := "report-failure"
	r3 := p.invokeFrame(t, "handler.boom", `{}`, nil, nil,
		logicalRequestContext("handler.boom", logical3, 1, 3))
	if r3.OK {
		t.Fatal("expected the failing handler to fail its invocation")
	}
	requireTransportEcho(t, r3, "t2")
	requireInvocationReport(t, r3.Out, logical3, "t2")

	// A handler that rebinds sys.stdout hides its own print but must not
	// swallow the report or change the result: the report writer captured
	// the native stdout before any handler ran.
	logical4 := "report-muted-stdout"
	r4 := p.invokeFrame(t, "handler.mute", `{}`, nil, nil,
		logicalRequestContext("handler.mute", logical4, 1, 3))
	if !r4.OK {
		t.Fatalf("invoke 4 failed: %s", r4.Err)
	}
	requireInvocationReport(t, r4.Out, logical4, "t3")
	if strings.Contains(r4.Out, "hidden") {
		t.Errorf("the rebound stdout's print must not reach native stdout:\n%s",
			r4.Out)
	}

	// Whole-process view: one report per invocation, none under a
	// transport correlation id.
	out := p.stdoutString()
	if n := countLinesWithPrefix(out, "REPORT RequestId: "); n != 4 {
		t.Errorf("REPORT lines = %d, want one per invocation (4):\n%s", n, out)
	}
	for _, transport := range []string{"t0", "t1", "t2", "t3"} {
		if n := countLinesWithPrefix(out, "REPORT RequestId: "+transport); n != 0 {
			t.Errorf("REPORT lines carrying transport id %s = %d, want 0:\n%s",
				transport, n, out)
		}
	}
}
