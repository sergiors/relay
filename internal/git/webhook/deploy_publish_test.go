package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	git "relay/internal/git"
	"relay/internal/secrets"
)

// errBoom is the scripted publisher failure.
var errBoom = errors.New("publish failed")

// pushDelivery is one GitHub push delivery as the publish-mode handler sees
// it: the before/after SHAs and the head-commit timestamp are the three inputs
// a coordinated-deployment publication needs.
type pushDelivery struct {
	before, after, timestamp string
}

// publishCall records one invocation of the coordinated-deployment publisher.
type publishCall struct {
	cfg        git.Config
	before     string
	commit     string
	sourceTime string
}

// publishRecorder is a PublishRevisionFunc test double: it records every call
// and answers with the scripted outcome.
type publishRecorder struct {
	mu        sync.Mutex
	calls     []publishCall
	published bool
	err       error
}

func (p *publishRecorder) publish(
	_ context.Context, cfg git.Config, before, commit, sourceTime string,
) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, publishCall{
		cfg: cfg, before: before, commit: commit, sourceTime: sourceTime,
	})
	return p.published, p.err
}

func (p *publishRecorder) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

func (p *publishRecorder) last() publishCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[len(p.calls)-1]
}

// deliveryBody marshals a push payload for the given delivery against the
// configured repository, exactly as GitHub would sign it.
func deliveryBody(t *testing.T, d pushDelivery) []byte {
	t.Helper()
	p := pushPayload{
		Ref:    "refs/heads/main",
		Before: d.before,
		After:  d.after,
	}
	p.HeadCommit.Timestamp = d.timestamp
	p.Repository.FullName = "acme/backend"
	p.Repository.SSHURL = "git@github.com:acme/backend.git"
	body, err := json.Marshal(&p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return body
}

// setupPublishHandler builds a publish-mode provider over a persisted git
// source and a local secret store, mirroring the NewServer coordinated-mode
// wiring: no sync scheduler is assembled, only the publisher.
func setupPublishHandler(
	t *testing.T, rec *publishRecorder,
) (*GitHubProvider, *bytes.Buffer) {
	t.Helper()
	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	secretsDir := t.TempDir()
	store, err := secrets.NewLocal(secretsDir)
	if err != nil {
		t.Fatalf("new local: %v", err)
	}
	if err := store.Set(context.Background(), "gh_secret", "test-secret"); err != nil {
		t.Fatalf("store set: %v", err)
	}
	prov, err := secrets.NewLocalProvider(secretsDir)
	if err != nil {
		t.Fatalf("provider: %v", err)
	}

	cfgPath := filepath.Join(t.TempDir(), "source.json")
	if err := git.SetSource(
		cfgPath, "git@github.com:acme/backend.git", "main", "", "gh_secret",
	); err != nil {
		t.Fatalf("set source: %v", err)
	}

	// syncer is deliberately nil: publish mode must never need the standalone
	// branch-sync scheduler (that is how NewServer assembles the mode).
	h := newGitHubProvider(logger, "gh_secret", prov, cfgPath, nil, rec.publish)
	return h, logBuf
}

// TestPublishPushDeliversFullChainInputs pins that a matching push hands the
// publisher the delivery's EXACT chain inputs — before SHA, after SHA, and
// head-commit timestamp — plus the matched source coordinates, and that an
// accepted publication is acknowledged with 202. Nothing is resolved from a
// mutable ref anywhere on this path.
func TestPublishPushDeliversFullChainInputs(t *testing.T) {
	rec := &publishRecorder{published: true}
	h, _ := setupPublishHandler(t, rec)

	body := deliveryBody(t, pushDelivery{
		before:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		after:     "0123456789abcdef0123456789abcdef01234567",
		timestamp: "2026-01-03T10:00:00Z",
	})
	rec2 := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec2.Code != http.StatusAccepted {
		t.Fatalf("status = %d want 202; body=%s", rec2.Code, rec2.Body.String())
	}
	if !strings.Contains(rec2.Body.String(), "accepted") {
		t.Fatalf("body = %q, want accepted", rec2.Body.String())
	}
	if rec.count() != 1 {
		t.Fatalf("publish calls = %d, want 1", rec.count())
	}
	got := rec.last()
	if got.before != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" ||
		got.commit != "0123456789abcdef0123456789abcdef01234567" ||
		got.sourceTime != "2026-01-03T10:00:00Z" {
		t.Fatalf("publisher received before=%q commit=%q sourceTime=%q, want the delivery's exact values",
			got.before, got.commit, got.sourceTime)
	}
	if got.cfg.Repository != "git@github.com:acme/backend.git" || got.cfg.Ref != "main" {
		t.Fatalf("publisher received source %+v, want the matched persisted source", got.cfg)
	}
}

// TestPublishIgnoredAcknowledgedAs200 pins that a deliberately ignored
// publication (pinned revision, stale branch event, duplicate delivery —
// whatever the state layer refused) keeps the provider's historical ignored
// semantics: HTTP 200 with the ignored body, never an error.
func TestPublishIgnoredAcknowledgedAs200(t *testing.T) {
	rec := &publishRecorder{published: false}
	h, _ := setupPublishHandler(t, rec)

	body := deliveryBody(t, pushDelivery{
		before:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		after:     "0123456789abcdef0123456789abcdef01234567",
		timestamp: "2026-01-03T10:00:00Z",
	})
	rec2 := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec2.Code != http.StatusOK {
		t.Fatalf("status = %d want 200; body=%s", rec2.Code, rec2.Body.String())
	}
	if !strings.Contains(rec2.Body.String(), "ignored") {
		t.Fatalf("body = %q, want ignored", rec2.Body.String())
	}
	if rec.count() != 1 {
		t.Fatalf("publish calls = %d, want 1 (the handler still offered the delivery)", rec.count())
	}
}

// TestPublishErrorAcknowledgedAs500 pins that a persistence failure surfaces
// as HTTP 500 — the delivery must be retried by the provider, not swallowed.
func TestPublishErrorAcknowledgedAs500(t *testing.T) {
	rec := &publishRecorder{published: false, err: errBoom}
	h, _ := setupPublishHandler(t, rec)

	body := deliveryBody(t, pushDelivery{
		before: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		after:  "0123456789abcdef0123456789abcdef01234567",
	})
	rec2 := serve(h, pushReq(body, testSig("test-secret", body), "push"))
	if rec2.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d want 500; body=%s", rec2.Code, rec2.Body.String())
	}
	if strings.Contains(rec2.Body.String(), "boom") {
		t.Fatalf("response leaked the internal error: %s", rec2.Body.String())
	}
}

// TestPublishRejectsNonExactAfterBeforeCallingPublisher pins that a push whose
// after value is not a full immutable SHA never reaches the publisher: the
// handler refuses it as a bad request instead of offering a mutable or
// ambiguous value to the shared revision.
func TestPublishRejectsNonExactAfterBeforeCallingPublisher(t *testing.T) {
	rec := &publishRecorder{published: true}
	h, _ := setupPublishHandler(t, rec)

	for _, after := range []string{"deadbeef", "", "refs/heads/main"} {
		body := deliveryBody(t, pushDelivery{
			before: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			after:  after,
		})
		rec2 := serve(h, pushReq(body, testSig("test-secret", body), "push"))
		if rec2.Code != http.StatusBadRequest {
			t.Fatalf("after %q: status = %d want 400", after, rec2.Code)
		}
	}
	if rec.count() != 0 {
		t.Fatalf("publish calls = %d, want 0 (invalid after values never reach the publisher)",
			rec.count())
	}
}

// TestPublishPassesTimestampVerbatim pins that the webhook never interprets
// ordering itself: equal, future, or missing head-commit timestamps are handed
// to the publisher exactly as delivered — the state layer's anchor policy
// decides acceptance. Repeated deliveries are each offered (deduplication and
// idempotency belong to the shared revision layer).
func TestPublishPassesTimestampVerbatim(t *testing.T) {
	rec := &publishRecorder{published: false}
	h, _ := setupPublishHandler(t, rec)

	const (
		before  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		afterA  = "0123456789abcdef0123456789abcdef01234567"
		afterB  = "89abcdef0123456789abcdef0123456789abcdef"
		equalTS = "2026-01-03T10:00:00Z"
		future  = "2027-01-01T00:00:00Z"
	)
	for _, d := range []pushDelivery{
		{before: before, after: afterA, timestamp: equalTS}, // equal/normal
		{before: before, after: afterA, timestamp: equalTS}, // repeated delivery
		{before: before, after: afterB, timestamp: future},  // future timestamp
		{before: before, after: afterB, timestamp: ""},      // missing head_commit
	} {
		body := deliveryBody(t, d)
		rec2 := serve(h, pushReq(body, testSig("test-secret", body), "push"))
		if rec2.Code != http.StatusOK {
			t.Fatalf("delivery %+v: status = %d want 200 (ignored)", d, rec2.Code)
		}
		got := rec.last()
		if got.sourceTime != d.timestamp {
			t.Fatalf("publisher sourceTime = %q, want the verbatim %q",
				got.sourceTime, d.timestamp)
		}
		if got.before != d.before || got.commit != d.after {
			t.Fatalf("publisher chain = before %q commit %q, want the delivery's %q/%q",
				got.before, got.commit, d.before, d.after)
		}
	}
	if rec.count() != 4 {
		t.Fatalf("publish calls = %d, want 4 (every delivery is offered)", rec.count())
	}
}
