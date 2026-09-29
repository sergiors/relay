package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// fakeNetworkVerifier is the networkVerifier seam for the startup NETWORKS
// pre-flight: it records the exact list it was asked to verify and returns a
// scripted result, so the verification error semantics are testable without a
// Docker daemon.
type fakeNetworkVerifier struct {
	verified  []string
	missing   string
	ok        bool
	err       error
	calls     int
	lastCtxOK bool
}

func (f *fakeNetworkVerifier) VerifyNetworks(ctx context.Context, networks []string) (string, bool, error) {
	f.calls++
	f.verified = append([]string(nil), networks...)
	f.lastCtxOK = ctx.Err() == nil
	return f.missing, f.ok, f.err
}

// TestVerifyConfiguredNetworks pins the worker's startup pre-flight semantics
// for the worker-global NETWORKS set: an empty set is a no-op (the verifier is
// never called), all-present passes through, a missing network is a fatal error
// naming the network and stating Relay never creates networks, and an inspect
// failure is surfaced as a genuine error.
func TestVerifyConfiguredNetworks(t *testing.T) {
	t.Run("empty set is a no-op", func(t *testing.T) {
		f := &fakeNetworkVerifier{}
		if err := verifyConfiguredNetworks(context.Background(), f, nil); err != nil {
			t.Fatalf("verify(nil) = %v, want nil", err)
		}
		if f.calls != 0 {
			t.Fatalf("verifier called %d times for an empty set, want 0", f.calls)
		}
	})

	t.Run("all present passes", func(t *testing.T) {
		f := &fakeNetworkVerifier{ok: true}
		if err := verifyConfiguredNetworks(context.Background(), f, []string{"backend", "frontend"}); err != nil {
			t.Fatalf("verify = %v, want nil", err)
		}
		if len(f.verified) != 2 || f.verified[0] != "backend" || f.verified[1] != "frontend" {
			t.Fatalf("verified = %v, want the configured set", f.verified)
		}
	})

	t.Run("missing network fails naming the network", func(t *testing.T) {
		f := &fakeNetworkVerifier{missing: "backend", ok: false}
		err := verifyConfiguredNetworks(context.Background(), f, []string{"backend"})
		if err == nil {
			t.Fatal("a missing network must fail startup")
		}
		for _, want := range []string{"backend", "does not exist", "never creates networks"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q does not mention %q", err, want)
			}
		}
	})

	t.Run("inspect failure surfaces as an error", func(t *testing.T) {
		boom := errors.New("daemon exploded")
		f := &fakeNetworkVerifier{missing: "backend", err: boom}
		err := verifyConfiguredNetworks(context.Background(), f, []string{"backend"})
		if err == nil || !errors.Is(err, boom) {
			t.Fatalf("verify = %v, want the wrapped inspect error", err)
		}
	})
}
