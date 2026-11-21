package app

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// healthcheckTemplate wraps one service healthcheck block in an otherwise
// valid entrypoint-service template.
func healthcheckTemplate(healthcheck string) string {
	body := "runtime: nodejs24.x\nservices:\n  - name: web\n    entrypoint: service.js\n"
	if healthcheck != "" {
		body += "    healthcheck:\n" + healthcheck
	}
	return body
}

// A declared but empty healthcheck resolves every documented default, and an
// omitted healthcheck stays nil so the historical no-probe behavior is
// byte-for-byte unchanged.
func TestParseServiceHealthcheckDefaults(t *testing.T) {
	t.Run("empty mapping", func(t *testing.T) {
		tmpl := mustParse(t, healthcheckTemplate("      {}\n"))
		hc := tmpl.Services[0].Healthcheck
		if hc == nil {
			t.Fatal("declared healthcheck = nil, want the resolved defaults")
		}
		if hc.Path != DefaultServiceHealthcheckPath {
			t.Errorf("path = %q, want %q", hc.Path, DefaultServiceHealthcheckPath)
		}
		if hc.Interval != DefaultServiceHealthcheckInterval {
			t.Errorf("interval = %s, want %s", hc.Interval, DefaultServiceHealthcheckInterval)
		}
		if hc.Timeout != DefaultServiceHealthcheckTimeout {
			t.Errorf("timeout = %s, want %s", hc.Timeout, DefaultServiceHealthcheckTimeout)
		}
		if hc.Retries != DefaultServiceHealthcheckRetries {
			t.Errorf("retries = %d, want %d", hc.Retries, DefaultServiceHealthcheckRetries)
		}
		if hc.StartPeriod != DefaultServiceHealthcheckStartPeriod {
			t.Errorf("start_period = %s, want %s", hc.StartPeriod, DefaultServiceHealthcheckStartPeriod)
		}
	})
	t.Run("omitted key", func(t *testing.T) {
		tmpl := mustParse(t, healthcheckTemplate(""))
		if got := tmpl.Services[0].Healthcheck; got != nil {
			t.Fatalf("omitted healthcheck = %+v, want nil", got)
		}
	})
	t.Run("null value", func(t *testing.T) {
		tmpl := mustParse(t, healthcheckTemplate("      ~\n"))
		if got := tmpl.Services[0].Healthcheck; got != nil {
			t.Fatalf("null healthcheck = %+v, want nil", got)
		}
	})
}

// Every present field resolves independently: an explicit value wins and the
// omitted ones keep their defaults.
func TestParseServiceHealthcheckExplicitValues(t *testing.T) {
	tmpl := mustParse(t, healthcheckTemplate(
		"      path: /ready\n"+
			"      interval: 2s\n"+
			"      timeout: 1s\n"+
			"      retries: 5\n"+
			"      start_period: 30s\n"))
	hc := tmpl.Services[0].Healthcheck
	if hc == nil {
		t.Fatal("healthcheck = nil, want the parsed probe")
	}
	if hc.Path != "/ready" || hc.Interval != 2*time.Second ||
		hc.Timeout != time.Second || hc.Retries != 5 ||
		hc.StartPeriod != 30*time.Second {
		t.Fatalf("healthcheck = %+v, want the explicit values", hc)
	}
}

// The probe path is used EXACTLY as configured: a trailing slash is
// significant for HTTP endpoints, so it survives parsing untouched.
func TestParseServiceHealthcheckPathUsedExactly(t *testing.T) {
	for _, path := range []string{"/", "/health", "/ready/", "/api/v2/status"} {
		tmpl := mustParse(t, healthcheckTemplate("      path: "+path+"\n"))
		if got := tmpl.Services[0].Healthcheck.Path; got != path {
			t.Errorf("path %q parsed as %q, want it verbatim", path, got)
		}
	}
}

// A path that is not a safe absolute URL path is rejected, naming the field.
func TestParseServiceHealthcheckRejectsUnsafePaths(t *testing.T) {
	cases := []struct {
		name, path string
	}{
		{"relative", "health"},
		{"no leading slash", "v1/health"},
		{"whitespace", `"/a b"`},
		{"query", `"/health?full=1"`},
		{"fragment", `"/health#top"`},
		{"backslash", `'/a\b'`},
		{"empty segment", `"/a//b"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(healthcheckTemplate("      path: " + tc.path + "\n")))
			if err == nil || !strings.Contains(err.Error(), "path") {
				t.Fatalf("parse error = %v, want a path rejection", err)
			}
		})
	}
}

// Durations are Go duration strings with documented bounds: unparseable,
// out-of-range, and inverted (timeout over interval) values are rejected, while
// an omitted timeout is clamped to a sub-default interval so the probe stays
// usable.
func TestParseServiceHealthcheckDurations(t *testing.T) {
	rejected := map[string]string{
		"unparseable interval": "      interval: soon\n",
		"interval below min":   "      interval: 500ms\n",
		"interval above max":   "      interval: 2h\n",
		"unparseable timeout":  "      timeout: later\n",
		"timeout below min":    "      interval: 5s\n      timeout: 500ms\n",
		"timeout over interval": "      interval: 2s\n" +
			"      timeout: 3s\n",
		"unparseable start_period": "      start_period: soon\n",
		"start_period above max":   "      start_period: 11m\n",
	}
	for name, block := range rejected {
		t.Run(name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(healthcheckTemplate(block)))
			if err == nil {
				t.Fatalf("parse of %q unexpectedly succeeded", block)
			}
			if !strings.Contains(err.Error(), "healthcheck") {
				t.Fatalf("error = %v, want it to name the healthcheck", err)
			}
		})
	}

	t.Run("zero start_period accepted", func(t *testing.T) {
		tmpl := mustParse(t, healthcheckTemplate("      start_period: 0s\n"))
		if got := tmpl.Services[0].Healthcheck.StartPeriod; got != 0 {
			t.Fatalf("start_period = %s, want 0", got)
		}
	})
	t.Run("default timeout clamped to interval", func(t *testing.T) {
		tmpl := mustParse(t, healthcheckTemplate("      interval: 2s\n"))
		hc := tmpl.Services[0].Healthcheck
		if hc.Interval != 2*time.Second || hc.Timeout != 2*time.Second {
			t.Fatalf("interval/timeout = %s/%s, want 2s/2s", hc.Interval, hc.Timeout)
		}
	})
}

// retries is an integer within [1, 10]; a non-integer or out-of-range value is
// rejected instead of silently coerced.
func TestParseServiceHealthcheckRetriesBounds(t *testing.T) {
	rejected := map[string]string{
		"zero":     "      retries: 0\n",
		"above":    "      retries: 11\n",
		"string":   "      retries: \"5\"\n",
		"float":    "      retries: 1.5\n",
		"negative": "      retries: -1\n",
	}
	for name, block := range rejected {
		t.Run(name, func(t *testing.T) {
			_, err := ParseTemplate([]byte(healthcheckTemplate(block)))
			if err == nil || !strings.Contains(err.Error(), "retries") {
				t.Fatalf("parse error = %v, want a retries rejection", err)
			}
		})
	}
	for _, n := range []int{1, 10} {
		tmpl := mustParse(t, healthcheckTemplate(
			"      retries: "+strconv.Itoa(n)+"\n"))
		if got := tmpl.Services[0].Healthcheck.Retries; got != n {
			t.Errorf("retries %d parsed as %d", n, got)
		}
	}
}

// A healthcheck on an external `image` service is rejected at parse time: Relay
// cannot assume a probe tool or shell inside an image it does not build.
func TestParseServiceHealthcheckRejectedForImageSource(t *testing.T) {
	const withProbe = `runtime: nodejs24.x
services:
  - name: web
    image: nginx:1.27
    healthcheck:
      path: /health
`
	_, err := ParseTemplate([]byte(withProbe))
	if err == nil || !strings.Contains(err.Error(), "healthcheck is not supported for an image source") {
		t.Fatalf("parse error = %v, want the image-source healthcheck rejection", err)
	}

	const withoutProbe = `runtime: nodejs24.x
services:
  - name: web
    image: nginx:1.27
`
	tmpl := mustParse(t, withoutProbe)
	if got := tmpl.Services[0].Healthcheck; got != nil {
		t.Fatalf("image service without a healthcheck = %+v, want nil", got)
	}
}

// The nested `healthcheck` mapping is decoded STRICTLY: an unknown key — a
// misspelling, a camelCase variant, or a key from another system — is rejected
// together with the full accepted key set instead of silently ignored (yaml.v3
// ignores unknown fields by default), and a nested mapping where a scalar
// field is expected never coerces into a probe value.
func TestParseServiceHealthcheckRejectsUnknownKeys(t *testing.T) {
	t.Run("unknown key names itself and the accepted set", func(t *testing.T) {
		_, err := ParseTemplate([]byte(healthcheckTemplate("      startPeriod: 30s\n")))
		if err == nil {
			t.Fatal("parse of an unknown healthcheck key unexpectedly succeeded")
		}
		for _, want := range []string{
			"healthcheck: unknown key(s)", `"startPeriod"`,
			"accepted keys are path, interval, timeout, retries, start_period",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error = %v, want it to contain %q", err, want)
			}
		}
	})

	t.Run("every unknown key is reported sorted", func(t *testing.T) {
		_, err := ParseTemplate([]byte(healthcheckTemplate(
			"      path: /health\n      test: 1\n      startPeriod: 30s\n")))
		if err == nil {
			t.Fatal("parse with two unknown keys unexpectedly succeeded")
		}
		if want := `unknown key(s) "startPeriod", "test"`; !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %v, want it to contain %q (sorted, all keys named)", err, want)
		}
	})

	t.Run("known keys still parse in the same block", func(t *testing.T) {
		tmpl := mustParse(t, healthcheckTemplate("      path: /health\n      start_period: 5s\n"))
		hc := tmpl.Services[0].Healthcheck
		if hc.Path != "/health" || hc.StartPeriod != 5*time.Second {
			t.Fatalf("healthcheck = %+v, want the declared values", hc)
		}
	})

	t.Run("nested mapping never coerces into a duration", func(t *testing.T) {
		_, err := ParseTemplate([]byte(healthcheckTemplate(
			"      interval:\n        value: 2s\n")))
		if err == nil || !strings.Contains(err.Error(), "cannot unmarshal") {
			t.Fatalf("error = %v, want a decode rejection of the nested mapping", err)
		}
	})

	t.Run("nested mapping under retries is rejected by value", func(t *testing.T) {
		_, err := ParseTemplate([]byte(healthcheckTemplate(
			"      retries:\n        max: 5\n")))
		if err == nil || !strings.Contains(err.Error(), "healthcheck: retries") {
			t.Fatalf("error = %v, want a retries rejection naming the healthcheck", err)
		}
	})
}
