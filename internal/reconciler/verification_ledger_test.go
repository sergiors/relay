package reconciler

import (
	"testing"
	"time"

	"relay/internal/runner"
)

// TestVerificationLedgerDistrustsPreviousAuthorizations pins the epoch half of
// the verification ledger: an entry is only trusted while its recorded
// authorization epoch is current, so advancing the materialized authorization
// (a new deployment generation, or the first installation on a previously
// standalone worker) invalidates every pre-update timestamp instead of carrying
// it forward — the coordinator's independent backstop then hashes the whole
// authorized set once after the update. A fresh pass under the new epoch
// restores the entry.
func TestVerificationLedgerDistrustsPreviousAuthorizations(t *testing.T) {
	t.Run("an advancing authorization invalidates recorded verifications", func(t *testing.T) {
		root := t.TempDir()
		dir := writeFnDir(t, root, "authorized")

		fn := initialFn("authorized", dir)
		b := &fakeBuilder{}
		r, _ := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)
		fp, tid := authorizationFor(t, root, "authorized")
		fingerprints := map[string]string{"authorized": fp}
		identities := map[string]string{"authorized": tid}
		r.SetDeploymentAuthorization(1, fingerprints, identities, nil)

		r.reconcileApp("authorized")
		if _, ok := r.VerifiedSince(time.Time{})["authorized"]; !ok {
			t.Fatal("a successful verification under the current authorization must " +
				"be visible in the ledger")
		}

		// Even the same content re-authorized as a new generation must not
		// carry the old timestamp: the authorization it was verified against
		// is gone.
		r.SetDeploymentAuthorizationWithInvalid(2, fingerprints, identities,
			nil, nil, nil)
		if _, ok := r.VerifiedSince(time.Time{})["authorized"]; ok {
			t.Fatal("a verification recorded before the authorization advanced must " +
				"not survive the update (the backstop must re-hash the set once)")
		}

		r.reconcileApp("authorized")
		if _, ok := r.VerifiedSince(time.Time{})["authorized"]; !ok {
			t.Fatal("a pass under the new authorization must re-record the ledger entry")
		}
		if b.prepares() != 0 {
			t.Fatalf("prepare calls = %d, want 0 (unchanged authorized content)", b.prepares())
		}
	})

	t.Run("standalone verifications never count once clustered", func(t *testing.T) {
		root := t.TempDir()
		dir := writeFnDir(t, root, "local")

		fn := initialFn("local", dir)
		b := &fakeBuilder{}
		r, _ := newTestReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

		// Before any coordinated authorization exists the ledger records under
		// the initial epoch...
		r.reconcileApp("local")
		if _, ok := r.VerifiedSince(time.Time{})["local"]; !ok {
			t.Fatal("a standalone verification must be visible while standalone")
		}

		// ...and installing the first authorization invalidates it: content
		// verified against local (unauthorized) bytes proves nothing about the
		// materialized tree.
		fp, tid := authorizationFor(t, root, "local")
		r.SetDeploymentAuthorization(1,
			map[string]string{"local": fp},
			map[string]string{"local": tid}, nil)
		if _, ok := r.VerifiedSince(time.Time{})["local"]; ok {
			t.Fatal("a pre-authorizations verification must not count as verifying " +
				"the materialized tree")
		}

		r.reconcileApp("local")
		if _, ok := r.VerifiedSince(time.Time{})["local"]; !ok {
			t.Fatal("a clustered pass must re-record the ledger entry")
		}
	})
}
