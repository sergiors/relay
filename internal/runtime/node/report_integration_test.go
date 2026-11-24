//go:build integration

// Per-invocation REPORT tests for the split Node bootstrap: the pure cgroup
// path/value/format seams of cgroup-memory.mjs and lifecycle.mjs driven with
// fixture inputs (never a host cgroup layout), and the warm bootstrap's
// stdout contract — one REPORT line after the logical id's END line on
// success, handled failure, and warm reuse alike. See cgroup-memory.mjs and
// lifecycle.mjs; the Python mirrors live in the python package.
package node

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

// reportPaths mirrors one cgroup-memory.mjs discovery candidate: the
// resolved file paths of the current and peak counters.
type reportPaths struct {
	Current string `json:"current"`
	Peak    string `json:"peak"`
}

// memoryBytes mirrors the JSON form of a readCgroupMemory sample: raw byte
// counters as decimal strings, or null when a counter is unavailable. The
// lifecycle formatter converts them to whole MiB.
type memoryBytes struct {
	CurrentBytes *string `json:"currentBytes"`
	PeakBytes    *string `json:"peakBytes"`
}

func strValue(v string) *string { return &v }

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

// reportDriverSource is the fixture-only driver for cgroup-memory.mjs and
// lifecycle.mjs: it reads one JSON op on stdin and answers with one JSON line
// on stdout (plus at most one REPORT line, when the op emits through the
// lifecycle writer). No op touches a host cgroup.
const reportDriverSource = `
import {
  discoverCgroupMemoryPaths,
  initializeCgroupMemory,
  parseCgroupMemoryValue,
  readCgroupMemory,
} from "./cgroup-memory.mjs";
import {
  formatInvocationReport,
  writeInvocationReport,
} from "./lifecycle.mjs";

const chunks = [];
for await (const chunk of process.stdin) {
  chunks.push(chunk);
}
const spec = JSON.parse(Buffer.concat(chunks).toString("utf8"));

const bi = (value) => (value == null ? null : value.toString());
const bigint = (value) => (value == null ? null : BigInt(value));
const sample = (memory) => ({
  currentBytes: bi(memory.currentBytes),
  peakBytes: bi(memory.peakBytes),
});
const usage = (memory) =>
  memory == null
    ? memory
    : {
        currentBytes: bigint(memory.currentBytes),
        peakBytes: bigint(memory.peakBytes),
      };

let out;
switch (spec.op) {
  case "discover":
    out = { paths: discoverCgroupMemoryPaths(spec.cgroup, spec.mountinfo) };
    break;
  case "parse":
    out = { value: bi(parseCgroupMemoryValue(spec.contents)) };
    break;
  case "read":
    out = { memory: sample(readCgroupMemory(spec.paths ?? [])) };
    break;
  case "readDenied": {
    const files = spec.files ?? {};
    out = {
      memory: sample(
        readCgroupMemory(spec.paths ?? [], (file) => {
          if (!Object.prototype.hasOwnProperty.call(files, file)) {
            const err = new Error("EACCES: permission denied, open");
            err.code = "EACCES";
            throw err;
          }
          return files[file];
        }),
      ),
    };
    break;
  }
  case "format":
    out = {
      line: formatInvocationReport(
        spec.id,
        spec.durationMs,
        usage(spec.memory),
      ),
    };
    break;
  case "writeReport":
    writeInvocationReport(spec.id, spec.durationMs, usage(spec.memory));
    out = { emitted: true };
    break;
  case "writeReportGuarded":
    writeInvocationReport(spec.id, undefined, usage(spec.memory));
    out = { survived: true };
    break;
  case "writeReportThrowingMemory":
    writeInvocationReport(spec.id, spec.durationMs, {
      get currentBytes() {
        throw new Error("memory reader failed");
      },
    });
    out = { survived: true };
    break;
  case "readRuntimeMemory":
    initializeCgroupMemory();
    out = { memory: sample(readCgroupMemory()) };
    break;
  default:
    throw new Error("unknown op: " + spec.op);
}
process.stdout.write(JSON.stringify(out) + "\n");
`

// runReportDriver materializes the embedded runtime modules beside the
// driver, feeds it one JSON op on stdin, and returns the REPORT lines the
// lifecycle writer produced plus the driver's final JSON payload line. Any
// other stdout line is a contract break and fails the test.
func runReportDriver(t *testing.T, spec map[string]any) ([]string, json.RawMessage) {
	t.Helper()
	root := t.TempDir()
	writeRuntimeModules(t, root, false)
	driver := filepath.Join(root, "relay", "report_driver.mjs")
	if err := os.WriteFile(driver, []byte(reportDriverSource), 0o644); err != nil {
		t.Fatalf("write report driver: %v", err)
	}
	body, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal driver spec: %v", err)
	}
	cmd := exec.Command("node", driver)
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

// TestNodeCgroupMemoryPathFixtures pins the pure discovery: the process text
// and mountinfo fixtures map to the exact current/peak file paths for cgroup
// v2 (preferred, listed first) and the v1 memory controller, with mount-root
// rebasing, octal escapes decoded, and unsafe or malformed layouts yielding
// no candidates.
func TestNodeCgroupMemoryPathFixtures(t *testing.T) {
	requireNode(t)
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
			name:   "optional mountinfo fields before the separator are tolerated",
			cgroup: "0::/docker/app\n",
			mountinfo: mountinfoLineOptional("/", "/sys/fs/cgroup",
				"cgroup2", "cgroup", "rw"),
			want: v2Files,
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
			mountinfo: "not a mountinfo line",
			want:      nil,
		},
		{
			name:      "empty process text discovers nothing",
			cgroup:    "",
			mountinfo: "",
			want:      nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reports, payload := runReportDriver(t, map[string]any{
				"op":        "discover",
				"cgroup":    tc.cgroup,
				"mountinfo": tc.mountinfo,
			})
			if len(reports) != 0 {
				t.Fatalf("discover must emit no REPORT lines, got %v", reports)
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

// TestNodeParseCgroupMemoryValue pins the counter parser: a decimal byte
// counter round-trips (whitespace trimmed), while negative, fractional,
// empty, or junk content yields no value instead of fabricating one.
func TestNodeParseCgroupMemoryValue(t *testing.T) {
	requireNode(t)
	cases := []struct {
		name     string
		contents string
		want     *string
	}{
		{"a decimal byte counter", "5242880\n", strValue("5242880")},
		{"surrounding whitespace", "  42 \n", strValue("42")},
		{"a zero counter", "0", strValue("0")},
		{"a negative counter", "-4096", nil},
		{"a fractional counter", "1.5", nil},
		{"an empty counter", "", nil},
		{"a malformed counter", "not-a-number", nil},
		{"a counter with embedded junk", "12 34", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, payload := runReportDriver(t, map[string]any{
				"op": "parse", "contents": tc.contents,
			})
			var got struct {
				Value *string `json:"value"`
			}
			decodeReportPayload(t, payload, &got)
			switch {
			case tc.want == nil && got.Value != nil:
				t.Errorf("value = %q, want none", *got.Value)
			case tc.want != nil && got.Value == nil:
				t.Errorf("value = none, want %q", *tc.want)
			case tc.want != nil && *got.Value != *tc.want:
				t.Errorf("value = %q, want %q", *got.Value, *tc.want)
			}
		})
	}
}

// TestNodeReadMemoryFixtureFiles pins the fixture-path reader: byte counters
// stay distinct raw values (no MiB conversion at read time), the first
// candidate yielding either counter decides the sample — the other field
// stays unavailable rather than mixed in from a different cgroup — and a
// candidate yielding neither falls through to the next (v1 only when v2
// yields nothing). Unavailable candidates omit the value, never fabricate one.
func TestNodeReadMemoryFixtureFiles(t *testing.T) {
	requireNode(t)
	const mib = 1024 * 1024
	dir := t.TempDir()
	write := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write fixture %s: %v", name, err)
		}
		return path
	}
	current5 := write("current", strconv.Itoa(5*mib+100))
	peak12 := write("peak", strconv.Itoa(12*mib-1))
	subMiB := write("tiny", "1023")
	malformed := write("malformed", "oops")
	negative := write("negative", "-4096")
	missing := filepath.Join(dir, "missing")
	cases := []struct {
		name        string
		paths       []reportPaths
		wantCurrent *string
		wantPeak    *string
	}{
		{
			name:        "byte counters stay distinct raw values",
			paths:       []reportPaths{{current5, peak12}},
			wantCurrent: strValue(strconv.Itoa(5*mib + 100)),
			wantPeak:    strValue(strconv.Itoa(12*mib - 1)),
		},
		{
			name:        "a missing peak omits only the peak",
			paths:       []reportPaths{{current5, missing}},
			wantCurrent: strValue(strconv.Itoa(5*mib + 100)),
		},
		{
			name:  "missing files omit every value",
			paths: []reportPaths{{missing, missing}},
		},
		{
			name:  "malformed files omit every value",
			paths: []reportPaths{{malformed, malformed}},
		},
		{
			name:  "negative counters omit every value",
			paths: []reportPaths{{negative, negative}},
		},
		{
			name:        "a sub-MiB counter stays a raw byte value",
			paths:       []reportPaths{{subMiB, subMiB}},
			wantCurrent: strValue("1023"), wantPeak: strValue("1023"),
		},
		{
			name: "the current stays unavailable rather than mixed in",
			paths: []reportPaths{
				{current5, missing},
				{missing, peak12},
			},
			wantCurrent: strValue(strconv.Itoa(5*mib + 100)),
		},
		{
			name: "the peak stays unavailable rather than mixed in",
			paths: []reportPaths{
				{missing, peak12},
				{current5, missing},
			},
			wantPeak: strValue(strconv.Itoa(12*mib - 1)),
		},
		{
			name: "a malformed first candidate falls through",
			paths: []reportPaths{
				{malformed, malformed},
				{current5, peak12},
			},
			wantCurrent: strValue(strconv.Itoa(5*mib + 100)),
			wantPeak:    strValue(strconv.Itoa(12*mib - 1)),
		},
		{
			name: "the v1 candidate serves only when v2 yields neither value",
			paths: []reportPaths{
				{missing, missing},
				{current5, peak12},
			},
			wantCurrent: strValue(strconv.Itoa(5*mib + 100)),
			wantPeak:    strValue(strconv.Itoa(12*mib - 1)),
		},
		{
			name: "no candidates omit every value",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reports, payload := runReportDriver(t, map[string]any{
				"op": "read", "paths": tc.paths,
			})
			if len(reports) != 0 {
				t.Fatalf("read must emit no REPORT lines, got %v", reports)
			}
			var got struct {
				Memory memoryBytes `json:"memory"`
			}
			decodeReportPayload(t, payload, &got)
			assertOptionalString(t, "currentBytes", got.Memory.CurrentBytes, tc.wantCurrent)
			assertOptionalString(t, "peakBytes", got.Memory.PeakBytes, tc.wantPeak)
		})
	}

	// Sampling is read-only: one or repeated samples never write or reset
	// the lifetime-peak counter file — the kernel owns that counter and the
	// report only reads it.
	before, err := os.ReadFile(peak12)
	if err != nil {
		t.Fatalf("read peak fixture: %v", err)
	}
	for i := 0; i < 2; i++ {
		runReportDriver(t, map[string]any{
			"op":    "read",
			"paths": []reportPaths{{current5, peak12}},
		})
	}
	after, err := os.ReadFile(peak12)
	if err != nil {
		t.Fatalf("re-read peak fixture: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("sampling altered the lifetime-peak counter: %q -> %q",
			before, after)
	}

	// A deterministic EACCES for every candidate: both fields are omitted
	// without touching host permission bits (unreliable under a CI root).
	reports, payload := runReportDriver(t, map[string]any{
		"op":    "readDenied",
		"paths": []reportPaths{{current5, peak12}},
	})
	if len(reports) != 0 {
		t.Fatalf("a denied read must emit no REPORT lines, got %v", reports)
	}
	var deniedAll struct {
		Memory memoryBytes `json:"memory"`
	}
	decodeReportPayload(t, payload, &deniedAll)
	if deniedAll.Memory.CurrentBytes != nil || deniedAll.Memory.PeakBytes != nil {
		t.Errorf("an EACCES read must omit every field, got currentBytes=%v peakBytes=%v",
			deniedAll.Memory.CurrentBytes, deniedAll.Memory.PeakBytes)
	}

	// EACCES on one counter only: the readable field still reports while
	// the denied one is omitted — the denial is per candidate file.
	reports, payload = runReportDriver(t, map[string]any{
		"op":    "readDenied",
		"paths": []reportPaths{{current5, peak12}},
		"files": map[string]string{current5: strconv.Itoa(5*mib + 100)},
	})
	if len(reports) != 0 {
		t.Fatalf("a denied read must emit no REPORT lines, got %v", reports)
	}
	var deniedPeak struct {
		Memory memoryBytes `json:"memory"`
	}
	decodeReportPayload(t, payload, &deniedPeak)
	wantCurrent := strconv.Itoa(5*mib + 100)
	switch {
	case deniedPeak.Memory.CurrentBytes == nil:
		t.Errorf("currentBytes = none, want %q (its file was readable)", wantCurrent)
	case *deniedPeak.Memory.CurrentBytes != wantCurrent:
		t.Errorf("currentBytes = %q, want %q (its file was readable)",
			*deniedPeak.Memory.CurrentBytes, wantCurrent)
	}
	if deniedPeak.Memory.PeakBytes != nil {
		t.Errorf("peakBytes = %q, want none (its file raised EACCES)",
			*deniedPeak.Memory.PeakBytes)
	}
}

func assertOptionalString(t *testing.T, field string, got, want *string) {
	t.Helper()
	switch {
	case want == nil && got != nil:
		t.Errorf("%s = %q, want none", field, *got)
	case want != nil && got == nil:
		t.Errorf("%s = none, want %q", field, *want)
	case want != nil && *got != *want:
		t.Errorf("%s = %q, want %q", field, *got, *want)
	}
}

// TestNodeReportFormatFixtureValues pins the lifecycle formatter: the
// logical request id, a two-decimal duration, and raw byte counters floored
// to whole MiB in their current/peak slots — with each memory field omitted
// when its value is unavailable.
func TestNodeReportFormatFixtureValues(t *testing.T) {
	requireNode(t)
	const mib = 1024 * 1024
	cases := []struct {
		name     string
		id       string
		duration float64
		memory   memoryBytes
		want     string
	}{
		{
			name: "id, duration, current and peak in one line",
			id:   "req-42", duration: 12.5,
			memory: memoryBytes{
				CurrentBytes: strValue(strconv.Itoa(5 * mib)),
				PeakBytes:    strValue(strconv.Itoa(12 * mib)),
			},
			want: "REPORT RequestId: req-42 Duration: 12.50 ms" +
				" Memory Used: 5 MiB Container Memory Peak: 12 MiB",
		},
		{
			name: "byte counters floor to whole MiB",
			id:   "req-5", duration: 1,
			memory: memoryBytes{
				CurrentBytes: strValue(strconv.Itoa(2*mib - 1)),
				PeakBytes:    strValue(strconv.Itoa(mib - 1)),
			},
			want: "REPORT RequestId: req-5 Duration: 1.00 ms" +
				" Memory Used: 1 MiB Container Memory Peak: 0 MiB",
		},
		{
			name: "an unavailable current omits only its field",
			id:   "req-2", duration: 3,
			memory: memoryBytes{PeakBytes: strValue(strconv.Itoa(4 * mib))},
			want: "REPORT RequestId: req-2 Duration: 3.00 ms" +
				" Container Memory Peak: 4 MiB",
		},
		{
			name: "unavailable memory omits both fields",
			id:   "req-3", duration: 0.5,
			want: "REPORT RequestId: req-3 Duration: 0.50 ms",
		},
		{
			name: "a long duration still renders two decimals",
			id:   "req-4", duration: 999.999,
			memory: memoryBytes{
				CurrentBytes: strValue("0"), PeakBytes: strValue("0"),
			},
			want: "REPORT RequestId: req-4 Duration: 1000.00 ms" +
				" Memory Used: 0 MiB Container Memory Peak: 0 MiB",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, payload := runReportDriver(t, map[string]any{
				"op":         "format",
				"id":         tc.id,
				"durationMs": tc.duration,
				"memory":     tc.memory,
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

// TestNodeReportEmissionIsGuarded pins the emission seam: a report writes
// exactly one plain line through the pre-bound native logger — the exact
// duration-only line when memory is unavailable — while a broken duration or
// a failure raised while formatting the memory fields is swallowed without
// raising into the invocation path or writing anything.
func TestNodeReportEmissionIsGuarded(t *testing.T) {
	requireNode(t)
	const mib = 1024 * 1024
	reports, payload := runReportDriver(t, map[string]any{
		"op": "writeReport", "id": "emit-1", "durationMs": 7.25,
		"memory": memoryBytes{
			CurrentBytes: strValue(strconv.Itoa(2 * mib)),
			PeakBytes:    strValue(strconv.Itoa(9 * mib)),
		},
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
		"op": "writeReportGuarded", "id": "guard-1",
		"memory": memoryBytes{
			CurrentBytes: strValue(strconv.Itoa(mib)),
			PeakBytes:    strValue(strconv.Itoa(2 * mib)),
		},
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

	// Missing memory (an absent object, as the runtime reader may hand back
	// after a swallowed failure) still emits the exact duration-only line.
	reports, payload = runReportDriver(t, map[string]any{
		"op": "writeReport", "id": "emit-nomem", "durationMs": 5,
	})
	wantNoMemory := "REPORT RequestId: emit-nomem Duration: 5.00 ms"
	if len(reports) != 1 || reports[0] != wantNoMemory {
		t.Errorf("report lines = %q, want [%q]", reports, wantNoMemory)
	}
	var noMemory struct {
		Emitted bool `json:"emitted"`
	}
	decodeReportPayload(t, payload, &noMemory)
	if !noMemory.Emitted {
		t.Error("driver must reach the end of the report op")
	}

	// A failure raised mid-format (while reading the memory fields) is
	// swallowed: nothing is written and nothing propagates.
	reports, payload = runReportDriver(t, map[string]any{
		"op": "writeReportThrowingMemory", "id": "guard-2", "durationMs": 2.5,
	})
	if len(reports) != 0 {
		t.Errorf("a failing report must write nothing, got %q", reports)
	}
	var throwing struct {
		Survived bool `json:"survived"`
	}
	decodeReportPayload(t, payload, &throwing)
	if !throwing.Survived {
		t.Error("report emission must never raise into the invocation path")
	}
}

// TestNodeReadRuntimeMemoryShape pins the runtime sampling entry point: the
// bootstrap's initialize-then-read sequence never throws (a host without
// /proc, such as macOS, yields nulls) and always returns raw byte counters as
// decimal digit strings or null — any other shape is a contract break. The
// concrete values are host-dependent and deliberately not pinned here; the
// fixture reader covers the value semantics.
func TestNodeReadRuntimeMemoryShape(t *testing.T) {
	requireNode(t)
	_, payload := runReportDriver(t, map[string]any{
		"op": "readRuntimeMemory",
	})
	var got struct {
		Memory memoryBytes `json:"memory"`
	}
	decodeReportPayload(t, payload, &got)
	digits := regexp.MustCompile(`^\d+$`)
	for _, field := range []struct {
		name  string
		value *string
	}{
		{"currentBytes", got.Memory.CurrentBytes},
		{"peakBytes", got.Memory.PeakBytes},
	} {
		if field.value != nil && !digits.MatchString(*field.value) {
			t.Errorf("%s = %q, want null or a decimal byte count",
				field.name, *field.value)
		}
	}
}

// TestNodeRuntimeInvocationReportLine pins the warm bootstrap's stdout
// contract: every completed invocation — success, warm reuse, and handled
// failure alike — emits exactly one REPORT line after its logical END,
// under the logical requestId and never the transport id; no REPORT appears
// before the first request; and a handler that replaces console.log cannot
// swallow the report or change the result.
func TestNodeRuntimeInvocationReportLine(t *testing.T) {
	requireNode(t)
	root := appRoot(t)
	writeModule(t, root, "index.mjs", `
export function show(event, context) {
  console.log("app out " + event.msg);
}
export function boom(event, context) {
  throw new Error("handler-kaboom");
}
export function mute(event, context) {
  console.log = () => {};
  console.log("hidden");
}
`)
	p := startNode(t, root)
	p.awaitStdoutQuiet()
	if n := countLinesWithPrefix(p.stdoutString(), "REPORT "); n != 0 {
		t.Fatalf("REPORT lines before the first request = %d, want 0; stdout:\n%s",
			n, p.stdoutString())
	}

	// Success: the report follows END in the invocation's window.
	logical1 := "report-success-1"
	r1 := p.invokeFrame(t, "index.show", `{"msg":"a"}`, nil, nil,
		logicalRequestContext("index.show", logical1, 1, 3))
	if !r1.OK {
		t.Fatalf("invoke 1 failed: %s", r1.Err)
	}
	requireTransportEcho(t, r1, "t0")
	requireInvocationReport(t, r1.Out, logical1, "t0")

	// Warm reuse: the same process reports a second invocation under its
	// own logical id.
	logical2 := "report-success-2"
	r2 := p.invokeFrame(t, "index.show", `{"msg":"b"}`, nil, nil,
		logicalRequestContext("index.show", logical2, 1, 3))
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
	r3 := p.invokeFrame(t, "index.boom", `{}`, nil, nil,
		logicalRequestContext("index.boom", logical3, 1, 3))
	if r3.OK {
		t.Fatal("expected the failing handler to fail its invocation")
	}
	requireTransportEcho(t, r3, "t2")
	requireInvocationReport(t, r3.Out, logical3, "t2")

	// A handler that replaces console.log must not swallow the report or
	// change the result: the lifecycle writer bound the native method
	// before any handler could be imported.
	logical4 := "report-muted-console"
	r4 := p.invokeFrame(t, "index.mute", `{}`, nil, nil,
		logicalRequestContext("index.mute", logical4, 1, 3))
	if !r4.OK {
		t.Fatalf("invoke 4 failed: %s", r4.Err)
	}
	requireInvocationReport(t, r4.Out, logical4, "t3")

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
