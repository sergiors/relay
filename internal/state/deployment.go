package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Coordinated deployment state.
//
// deployment_desired is the shared, PostgreSQL-authoritative revision every
// clustered node converges to. It is deliberately separate from apps: the apps
// row remains one node's view of a locally materialized generation and is never
// repurposed as a global desired state. Every write here is a short transaction
// around PostgreSQL only — no network, checkout, materialization, or build runs
// inside it. Once the desired revision commits, every node converges without a
// leader, executor, or global barrier; the publisher may disappear immediately.
//
// deployment_nodes/deployment_apps record independent per-node materialization
// and per-app activation progress. They are fenced twice: a PostgreSQL-allocated
// incarnation (process generation) and the deployment generation. A stale
// process incarnation or a stale generation therefore cannot overwrite shared
// desired state or another incarnation's progress.

// Deployment phases for the per-node materialization progress.
const (
	DeploymentNodeMaterializing = "materializing"
	DeploymentNodeMaterialized  = "materialized"
	DeploymentNodeFailed        = "failed"
)

// Deployment phases for per-app activation progress. They are independent of
// the app's public apps.status: this is per (node, generation, app) cluster
// progress and distinguishes Git materialization from activation/service
// convergence.
const (
	DeploymentAppPending    = "pending"
	DeploymentAppActivating = "activating"
	DeploymentAppActive     = "active"
	DeploymentAppDegraded   = "degraded"
	DeploymentAppFailed     = "failed"
	DeploymentAppDrifted    = "drifted"
	DeploymentAppRemoved    = "removed"
)

// Deployment publication results. Reason describes why an accepted request did
// not advance the shared generation; it is presentation/diagnostic only.
const (
	DeploymentPublishReasonUnchanged = "unchanged"
	DeploymentPublishReasonPinned    = "pinned"
	DeploymentPublishReasonStale     = "stale"
	// DeploymentPublishReasonAmbiguous reports that automatic ordering could
	// not prove the candidate newer (no persisted branch-tip anchor, no before
	// SHA, or an otherwise unorderable delivery). The conservative policy ignores
	// it; an operator can publish manually.
	DeploymentPublishReasonAmbiguous = "ambiguous"
	// DeploymentPublishReasonSourceMismatch reports that an automatic publication
	// would change the shared repository/ref/path identity. Only explicit manual
	// publication may change the shared source; a webhook from a stale local
	// config must never do so.
	DeploymentPublishReasonSourceMismatch = "source-mismatch"
)

// ErrDeploymentConflict reports a compare-and-swap generation mismatch.
var ErrDeploymentConflict = errors.New("deployment generation conflict")

// ErrDeploymentNodeSuperseded reports that a node progress write carried an
// older desired generation than the node already observed. It is a normal,
// recoverable rejection (the node already moved on), not a database failure.
var ErrDeploymentNodeSuperseded = errors.New("deployment node generation superseded")

var deploymentCommitRe = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)

// NormalizeDeploymentCommit validates an immutable Git object name and returns
// its canonical lowercase form. Abbreviated hashes, branch/tag names, and
// non-hex values are rejected: a coordinated revision must be the exact object
// every node materializes, never a mutable ref resolved later. Relay's Git
// transport is SHA-1 based, so a full 40-character object name is required.
func NormalizeDeploymentCommit(raw string) (string, error) {
	commit := strings.TrimSpace(raw)
	if !deploymentCommitRe.MatchString(commit) {
		return "", fmt.Errorf("deployment commit %q must be a full 40-character hexadecimal SHA", raw)
	}
	return strings.ToLower(commit), nil
}

// DeploymentDesired is the shared desired revision. Generation is the
// monotonic compare-and-swap generation; Commit is an immutable SHA.
// Repository/Ref/Path are non-secret source coordinates every node compares
// against its local Git configuration. BranchTip is the persisted branch-tip
// anchor: the provider's "after" SHA from the last ACCEPTED automatic push. An
// automatic publication is accepted only when its provider "before" SHA equals
// this anchor, so commit timestamps never decide event order. SourceTime is
// retained only as diagnostic metadata; it never overrides an anchor mismatch.
// Manual publications carry no timestamp of their own and preserve/anchor the
// branch tip according to the pin policy. Pinned means automatic webhook pushes
// do not advance this revision until an operator unpins it or publishes again.
type DeploymentDesired struct {
	Generation  int64
	Commit      string
	Repository  string
	Ref         string
	Path        string
	BranchTip   string
	SourceTime  string
	Publisher   string
	Note        string
	Pinned      bool
	PublishedAt string
}

type deploymentDesiredPayload struct {
	Commit     string `json:"commit"`
	Repository string `json:"repository"`
	Ref        string `json:"ref"`
	Path       string `json:"path,omitempty"`
	BranchTip  string `json:"branch_tip,omitempty"`
	SourceTime string `json:"source_time,omitempty"`
	Publisher  string `json:"publisher,omitempty"`
	Note       string `json:"note,omitempty"`
	Pinned     bool   `json:"pinned,omitempty"`
}

// DeploymentPublishRequest is one publication attempt. ExpectedGeneration
// -1 means "compare against whatever is current under the publication lock";
// zero or a positive value is an explicit compare-and-swap expectation. A
// manual publication sets AllowPinned (explicit operator intent) and leaves
// SourceTime empty; its same-source ordering metadata is preserved, and an
// unpinned manual publication re-anchors the branch tip. Manual publication is
// also the ONLY way to change the shared repository/ref/path identity. An
// automatic webhook publication sets RejectStaleSource and supplies
// PreviousCommit from the provider's push payload: it replaces the current
// revision only when that previous commit equals the persisted BranchTip anchor
// (a proven branch-tip chain). Timestamps never accept a mismatched chain; equal,
// missing, or unparseable metadata is ignored as ambiguous. A pinned revision is
// never overridden automatically. A manual publication bypasses automatic
// ordering and preserves the anchor for rollbacks/pins; an unpinned manual
// publication re-anchors to its commit, which is the documented recovery when a
// chain was missed.
type DeploymentPublishRequest struct {
	Desired            DeploymentDesired
	ExpectedGeneration int64
	AllowPinned        bool
	RejectStaleSource  bool
	// PreviousCommit is the provider's "before" SHA. It is used only to prove a
	// direct push chain from the current desired revision; it is never stored.
	PreviousCommit string
}

// DeploymentPublishResult reports the committed desired revision and whether
// this call advanced it. Reason explains a non-publishing no-op.
type DeploymentPublishResult struct {
	Desired   DeploymentDesired
	Published bool
	Reason    string
}

// ReadDeploymentDesired returns the current shared desired revision. found is
// false when nothing has been published yet.
func (st *State) ReadDeploymentDesired(ctx context.Context) (DeploymentDesired, bool, error) {
	var (
		generation int64
		updatedAt  time.Time
		data       string
	)
	err := st.db.QueryRowContext(ctx,
		`SELECT generation, updated_at, data::text FROM deployment_desired WHERE id = 1`,
	).Scan(&generation, &updatedAt, &data)
	if errors.Is(err, sql.ErrNoRows) {
		return DeploymentDesired{}, false, nil
	}
	if err != nil {
		return DeploymentDesired{}, false, fmt.Errorf("read deployment desired: %w", err)
	}
	desired, err := decodeDeploymentDesired(generation, updatedAt, data)
	if err != nil {
		return DeploymentDesired{}, false, err
	}
	return desired, true, nil
}

func decodeDeploymentDesired(
	generation int64, updatedAt time.Time, data string,
) (DeploymentDesired, error) {
	var payload deploymentDesiredPayload
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return DeploymentDesired{}, fmt.Errorf("decode deployment desired generation %d: %w", generation, err)
	}
	return DeploymentDesired{
		Generation:  generation,
		Commit:      payload.Commit,
		Repository:  payload.Repository,
		Ref:         payload.Ref,
		Path:        payload.Path,
		BranchTip:   payload.BranchTip,
		SourceTime:  payload.SourceTime,
		Publisher:   payload.Publisher,
		Note:        payload.Note,
		Pinned:      payload.Pinned,
		PublishedAt: timestampString(updatedAt),
	}, nil
}

// PublishDeploymentDesired validates and commits one desired revision.
//
// Semantics under the single publication advisory lock:
//   - Repeated identical revisions (same commit/repository/ref/path/pin policy)
//     are idempotent no-ops: no generation advance, no error.
//   - A pinned current revision is not replaced by an automatic publication
//     (AllowPinned false); a manual publication may replace or unpin it.
//   - With RejectStaleSource, an automatic publication is accepted only when
//     its PreviousCommit equals the persisted BranchTip anchor. A missing anchor
//     or missing before SHA is ignored as ambiguous; a different before SHA is
//     ignored as stale regardless of commit timestamps. Timestamps are diagnostic
//     metadata only. An automatic publication may never change an existing shared
//     repository/ref/path identity. Manual publications bypass automatic ordering
//     and are the only way to change the shared source identity.
//   - Concurrent publishers serialize here. Exactly one advances a generation;
//     later readers observe the committed row and converge to it even if the
//     publisher process dies immediately after commit.
//
// A write that advances the generation also enqueues a desired-revision NOTIFY
// in the same transaction (see ListenDeploymentDesired). It is a latency hint
// for already-running nodes, never the authoritative signal: the row remains
// the only state every node reads, and a listener outage only falls back to the
// coordinator's periodic read.
//
// The transaction performs only PostgreSQL work: it never holds a network,
// checkout, materialization, or build operation.
func (st *State) PublishDeploymentDesired(
	ctx context.Context,
	req DeploymentPublishRequest,
) (DeploymentPublishResult, error) {
	desired := req.Desired
	commit, err := NormalizeDeploymentCommit(desired.Commit)
	if err != nil {
		return DeploymentPublishResult{}, err
	}
	desired.Commit = commit
	if strings.TrimSpace(desired.Repository) == "" {
		return DeploymentPublishResult{}, fmt.Errorf("deployment desired: repository must not be empty")
	}
	if strings.TrimSpace(desired.Ref) == "" {
		return DeploymentPublishResult{}, fmt.Errorf("deployment desired: ref must not be empty")
	}
	if req.ExpectedGeneration < -1 {
		return DeploymentPublishResult{}, fmt.Errorf(
			"deployment desired: expected generation must be -1, 0, or positive")
	}

	now := st.nowTime().UTC()
	var result DeploymentPublishResult
	err = st.deploymentTx(ctx, func(tx *sql.Tx) error {
		current, found, err := readDeploymentDesiredTx(ctx, tx)
		if err != nil {
			return err
		}
		advanced := false
		switch {
		case req.ExpectedGeneration > 0 && (!found || current.Generation != req.ExpectedGeneration):
			return fmt.Errorf(
				"publish deployment generation %d: %w", req.ExpectedGeneration, ErrDeploymentConflict)
		case req.ExpectedGeneration == 0 && found:
			return fmt.Errorf(
				"publish deployment generation 0 (expected absent): %w", ErrDeploymentConflict)
		}
		if found {
			// Manual publication never decides automatic ordering from a timestamp.
			// Preserve the same-source ordering metadata and decide the branch-tip
			// anchor: a rollback/pinned publication keeps the last known branch tip,
			// while an unpinned advance/republication becomes the new anchor so the
			// operator can recover a missed chain explicitly.
			if !req.RejectStaleSource {
				if deploymentSourceChanged(current, desired) {
					// Explicit manual intent may change the shared source. Old
					// ordering metadata belongs to the previous source and must not
					// leak into the new identity.
					if desired.Pinned {
						desired.BranchTip = ""
					} else if desired.BranchTip == "" {
						desired.BranchTip = desired.Commit
					}
				} else {
					if desired.SourceTime == "" {
						desired.SourceTime = current.SourceTime
					}
					if desired.BranchTip == "" {
						if desired.Pinned {
							desired.BranchTip = current.BranchTip
						} else {
							desired.BranchTip = desired.Commit
						}
					}
				}
			}
			if sameDeploymentRevision(current, desired) {
				// Repeated revision: no new generation. An automatic publication
				// still advances the ordering watermark when it carries a newer
				// provider timestamp, so a later out-of-order delivery is measured
				// against the latest observation, not the first one.
				if req.RejectStaleSource &&
					deploymentSourceNewer(desired.SourceTime, current.SourceTime) {
					current.SourceTime = desired.SourceTime
					if err := writeDeploymentDesiredTx(ctx, tx, current, now, true); err != nil {
						return err
					}
					current.PublishedAt = timestampString(now)
				}
				// An explicit unpinned manual republication of the CURRENT revision
				// is the documented recovery for a missed automatic chain: it may
				// re-anchor the branch tip without advancing the generation (the tree
				// every node materializes is unchanged).
				if !req.RejectStaleSource && desired.BranchTip != "" &&
					!strings.EqualFold(desired.BranchTip, current.BranchTip) {
					current.BranchTip = desired.BranchTip
					if err := writeDeploymentDesiredTx(ctx, tx, current, now, true); err != nil {
						return err
					}
					current.PublishedAt = timestampString(now)
				}
				result = DeploymentPublishResult{
					Desired: current, Published: false, Reason: DeploymentPublishReasonUnchanged,
				}
				return nil
			}
			if current.Pinned && !req.AllowPinned {
				result = DeploymentPublishResult{
					Desired: current, Published: false, Reason: DeploymentPublishReasonPinned,
				}
				return nil
			}
			if req.RejectStaleSource {
				if deploymentSourceChanged(current, desired) {
					// An automatic webhook may establish the source only when no
					// desired row exists (handled below). Against an existing row it
					// must never rewrite the shared repository/ref/path: a stale local
					// webhook config cannot take over or undo a manual source change.
					result = DeploymentPublishResult{
						Desired: current, Published: false,
						Reason: DeploymentPublishReasonSourceMismatch,
					}
					return nil
				}
				allowed, ambiguous := deploymentAutomaticOrderingAllows(
					current, desired, req.PreviousCommit)
				if !allowed {
					reason := DeploymentPublishReasonStale
					if ambiguous {
						reason = DeploymentPublishReasonAmbiguous
					}
					result = DeploymentPublishResult{
						Desired: current, Published: false, Reason: reason,
					}
					return nil
				}
				// The accepted push advances the persisted branch-tip anchor. Source
				// time remains diagnostic only; a clock-skewed timestamp can never
				// substitute for a matching before SHA.
				desired.BranchTip = desired.Commit
				if !deploymentSourceNewer(desired.SourceTime, current.SourceTime) {
					desired.SourceTime = current.SourceTime
				}
			}
			desired.Generation = current.Generation + 1
			if err := writeDeploymentDesiredTx(ctx, tx, desired, now, true); err != nil {
				return err
			}
			advanced = true
		} else {
			desired.Generation = 1
			if req.RejectStaleSource {
				// The first-ever automatic publication initializes the chain anchor.
				desired.BranchTip = desired.Commit
			} else if !desired.Pinned && desired.BranchTip == "" {
				// A first manual forward publication anchors itself, so the next
				// branch-tip push can chain from it. A pinned first publication has
				// no trustworthy branch tip yet.
				desired.BranchTip = desired.Commit
			}
			if err := writeDeploymentDesiredTx(ctx, tx, desired, now, false); err != nil {
				return err
			}
			advanced = true
		}
		desired.PublishedAt = timestampString(now)
		if advanced {
			// The notification is enqueued in the SAME transaction, so it is
			// delivered only if the new generation commits. A no-op (unchanged,
			// pinned, stale, ambiguous, source-mismatch) branch returns above and
			// never notifies, and SetDeploymentPinned changes policy only.
			if err := notifyDeploymentDesiredTx(ctx, tx); err != nil {
				return err
			}
		}
		result = DeploymentPublishResult{Desired: desired, Published: true}
		return nil
	})
	if err != nil {
		return DeploymentPublishResult{}, err
	}
	return result, nil
}

// WithDeploymentDesiredLock runs fn while holding the SAME transaction-scoped
// advisory lock PublishDeploymentDesired takes, and passes fn the current
// desired revision (found=false when none is published). It is the
// check-and-publish seam for registry activation: a caller that validates the
// generation inside fn and then performs its in-process swap/add inside the same
// callback cannot race a concurrent publisher commit. The transaction is held
// only around that tiny callback — never around a build, fetch, checkout, or
// materialization.
//
// Lock order: runtime image selection publication may take this lock inside its
// own per-runtime selection lock, but this package never takes a runtime
// selection lock, and a publisher of the desired revision takes only this lock.
func (st *State) WithDeploymentDesiredLock(
	ctx context.Context,
	fn func(desired DeploymentDesired, found bool) error,
) error {
	if fn == nil {
		return fmt.Errorf("with deployment desired lock: callback is nil")
	}
	tx, err := st.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin deployment desired lock: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock($1, $2)`,
		deploymentLockNamespace, deploymentLockKey); err != nil {
		return fmt.Errorf("lock deployment state: %w", err)
	}
	// The advisory lock, not a row lock, serializes writers. FOR UPDATE is
	// deliberately omitted: this transaction never writes, and a writer blocked
	// on the advisory lock must not be queued behind a row lock whose commit
	// semantics are irrelevant to a read-only publication check.
	desired, found, err := readDeploymentDesiredSnapshotTx(ctx, tx)
	if err != nil {
		return err
	}
	if err := fn(desired, found); err != nil {
		return err
	}
	// The transaction is READ-ONLY: its only job is to hold the advisory lock
	// across the authoritative read and the tiny in-process publication
	// callback. Rolling back (rather than committing) releases that lock without
	// inventing a commit failure AFTER the registry mutation has already
	// happened. A rollback error still releases the lock when the connection
	// closes and cannot change any persisted row, so the validated publication
	// outcome stands.
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		st.log.Debug("Deployment desired lock release failed", "error", err)
	}
	return nil
}

// readDeploymentDesiredSnapshotTx reads the desired revision inside the
// read-only publication-check transaction. Unlike readDeploymentDesiredTx it
// deliberately omits FOR UPDATE; the transaction-scoped advisory lock already
// serializes this read against PublishDeploymentDesired's CAS write.
func readDeploymentDesiredSnapshotTx(
	ctx context.Context,
	tx *sql.Tx,
) (DeploymentDesired, bool, error) {
	var (
		generation int64
		updatedAt  time.Time
		data       string
	)
	err := tx.QueryRowContext(ctx,
		`SELECT generation, updated_at, data::text
		 FROM deployment_desired WHERE id = 1`,
	).Scan(&generation, &updatedAt, &data)
	if errors.Is(err, sql.ErrNoRows) {
		return DeploymentDesired{}, false, nil
	}
	if err != nil {
		return DeploymentDesired{}, false, fmt.Errorf("read deployment desired: %w", err)
	}
	desired, err := decodeDeploymentDesired(generation, updatedAt, data)
	if err != nil {
		return DeploymentDesired{}, false, err
	}
	return desired, true, nil
}

// SetDeploymentPinned changes only the automatic-publication policy of the
// current desired revision. It does not advance the deployment generation
// because no node needs to materialize a different tree. A missing desired row
// reports found=false.
func (st *State) SetDeploymentPinned(
	ctx context.Context,
	pinned bool,
) (DeploymentDesired, bool, error) {
	now := st.nowTime().UTC()
	var (
		desired DeploymentDesired
		found   bool
	)
	err := st.deploymentTx(ctx, func(tx *sql.Tx) error {
		current, currentFound, err := readDeploymentDesiredTx(ctx, tx)
		if err != nil || !currentFound {
			return err
		}
		found = true
		if current.Pinned == pinned {
			desired = current
			return nil
		}
		current.Pinned = pinned
		if err := writeDeploymentDesiredTx(ctx, tx, current, now, true); err != nil {
			return err
		}
		current.PublishedAt = timestampString(now)
		desired = current
		return nil
	})
	if err != nil {
		return DeploymentDesired{}, false, err
	}
	return desired, found, nil
}

func sameDeploymentRevision(current, desired DeploymentDesired) bool {
	return current.Commit == desired.Commit &&
		current.Repository == desired.Repository &&
		current.Ref == desired.Ref &&
		current.Path == desired.Path &&
		current.Pinned == desired.Pinned
}

func deploymentSourceChanged(current, desired DeploymentDesired) bool {
	return current.Repository != desired.Repository ||
		current.Ref != desired.Ref ||
		current.Path != desired.Path
}

// deploymentSourceNewer reports whether candidate carries a strictly newer
// provider timestamp than current. Missing/unparseable timestamps are not newer.
func deploymentSourceNewer(candidate, current string) bool {
	if candidate == "" {
		return false
	}
	if current == "" {
		return true
	}
	candidateTime, candidateErr := time.Parse(time.RFC3339, candidate)
	currentTime, currentErr := time.Parse(time.RFC3339, current)
	if candidateErr != nil || currentErr != nil {
		return false
	}
	return candidateTime.After(currentTime)
}

// deploymentAutomaticOrderingAllows is the conservative automatic-publication
// policy. It accepts only a proven branch-tip chain: the provider's before SHA
// must equal the persisted anchor. A missing anchor or missing before SHA is
// ambiguous; a different before SHA is stale regardless of any commit timestamp.
// The first-ever automatic publication is handled by the caller (no row existed).
func deploymentAutomaticOrderingAllows(
	current, desired DeploymentDesired,
	previousCommit string,
) (allowed, ambiguous bool) {
	// desired carries no ordering authority: commit/source timestamps are
	// diagnostic only and must never override a branch-tip mismatch. It remains
	// in the signature for the ordering-policy call contract and future metadata
	// checks that do not weaken the anchor requirement.
	_ = desired
	if current.BranchTip == "" || previousCommit == "" {
		return false, true
	}
	if !strings.EqualFold(previousCommit, current.BranchTip) {
		return false, false
	}
	return true, false
}

func readDeploymentDesiredTx(ctx context.Context, tx *sql.Tx) (DeploymentDesired, bool, error) {
	var (
		generation int64
		updatedAt  time.Time
		data       string
	)
	err := tx.QueryRowContext(ctx,
		`SELECT generation, updated_at, data::text
		 FROM deployment_desired WHERE id = 1 FOR UPDATE`,
	).Scan(&generation, &updatedAt, &data)
	if errors.Is(err, sql.ErrNoRows) {
		return DeploymentDesired{}, false, nil
	}
	if err != nil {
		return DeploymentDesired{}, false, fmt.Errorf("read deployment desired: %w", err)
	}
	desired, err := decodeDeploymentDesired(generation, updatedAt, data)
	if err != nil {
		return DeploymentDesired{}, false, err
	}
	return desired, true, nil
}

func writeDeploymentDesiredTx(
	ctx context.Context,
	tx *sql.Tx,
	desired DeploymentDesired,
	updatedAt time.Time,
	update bool,
) error {
	payload, err := json.Marshal(deploymentDesiredPayload{
		Commit:     desired.Commit,
		Repository: desired.Repository,
		Ref:        desired.Ref,
		Path:       desired.Path,
		BranchTip:  desired.BranchTip,
		SourceTime: desired.SourceTime,
		Publisher:  desired.Publisher,
		Note:       desired.Note,
		Pinned:     desired.Pinned,
	})
	if err != nil {
		return fmt.Errorf("encode deployment desired: %w", err)
	}
	if update {
		_, err = tx.ExecContext(ctx,
			`UPDATE deployment_desired
			 SET generation = $1, updated_at = $2, data = $3
			 WHERE id = 1`,
			desired.Generation, updatedAt, string(payload))
	} else {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO deployment_desired (id, generation, updated_at, data)
			 VALUES (1, $1, $2, $3)`,
			desired.Generation, updatedAt, string(payload))
	}
	if err != nil {
		return fmt.Errorf("write deployment desired: %w", err)
	}
	return nil
}

// notifyDeploymentDesiredTx enqueues a desired revision hint in the caller's
// transaction. PostgreSQL delivers a NOTIFY only after that transaction
// commits, so a listener can never wake for a rolled-back generation. The
// payload is empty on purpose: listeners must always re-read the authoritative
// row instead of trusting a possibly superseded generation carried in a hint.
//
// pg_notify only enqueues the hint; it never waits for a listening session and
// does not depend on one existing, so a listener outage can never block or
// delay publication. A SQL failure here aborts the publication transaction
// (and therefore the generation write), which preserves the invariant that a
// committed generation is always accompanied by its hint.
func notifyDeploymentDesiredTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx,
		`SELECT pg_notify($1, '')`, deploymentNotifyChannel); err != nil {
		return fmt.Errorf("notify deployment desired: %w", err)
	}
	return nil
}

// deploymentTx runs fn under the shared publication advisory lock. The lock
// covers both row-present and row-absent CAS decisions, which a row lock alone
// cannot guard.
func (st *State) deploymentTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin deployment transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock($1, $2)`,
		deploymentLockNamespace, deploymentLockKey); err != nil {
		return fmt.Errorf("lock deployment state: %w", err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit deployment transaction: %w", err)
	}
	return nil
}

// DeploymentNodeProgress is one node's observed/materialized deployment
// progress. Generation is the latest desired generation observed, while
// AppliedGeneration is the generation whose tree is currently materialized and
// authorized on this node. Incarnation/AppliedIncarnation are PostgreSQL-allocated
// process fences; a stale process can never advance the observed generation or
// overwrite progress written by a newer incarnation.
type DeploymentNodeProgress struct {
	NodeID             string
	Generation         int64
	AppliedGeneration  int64
	Instance           string
	Incarnation        int64
	AppliedIncarnation int64
	Phase              string
	Commit             string
	AppliedCommit      string
	Error              string
	UpdatedAt          string
}

type deploymentNodePayload struct {
	Phase         string `json:"phase,omitempty"`
	Commit        string `json:"commit,omitempty"`
	AppliedCommit string `json:"applied_commit,omitempty"`
	Error         string `json:"error,omitempty"`
}

// DeploymentAppProgress is one node's activation progress for one app in one
// deployment generation. Fingerprint is the materialized app content identity
// (empty for an app that was not loadable); Commit is the desired SHA that
// produced it.
type DeploymentAppProgress struct {
	NodeID           string
	App              string
	Generation       int64
	Instance         string
	Phase            string
	Fingerprint      string
	TemplateIdentity string
	Commit           string
	Error            string
	UpdatedAt        string
}

type deploymentAppPayload struct {
	Phase            string `json:"phase,omitempty"`
	Fingerprint      string `json:"fingerprint,omitempty"`
	TemplateIdentity string `json:"template_identity,omitempty"`
	Commit           string `json:"commit,omitempty"`
	Error            string `json:"error,omitempty"`
}

// AcquireDeploymentNode claims this node's observation of generation with this
// process instance and a fresh incarnation. It refuses an older generation with
// ErrDeploymentNodeSuperseded. Applied progress is preserved until the caller
// records a successful materialization (or adopts the previous applied
// generation on a failed attempt), so a failed newer target never destroys the
// previous generation's authorization.
func (st *State) AcquireDeploymentNode(
	ctx context.Context,
	nodeID,
	instance string,
	generation int64,
	progress DeploymentNodeProgress,
) (DeploymentNodeProgress, error) {
	if strings.TrimSpace(nodeID) == "" {
		return DeploymentNodeProgress{}, fmt.Errorf("deployment node: node ID is empty")
	}
	if generation <= 0 {
		return DeploymentNodeProgress{}, fmt.Errorf("deployment node %q: generation must be positive", nodeID)
	}
	now := st.nowTime().UTC()
	payload := deploymentNodePayload{
		Phase:         progress.Phase,
		Commit:        progress.Commit,
		AppliedCommit: progress.AppliedCommit,
		Error:         progress.Error,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return DeploymentNodeProgress{}, fmt.Errorf("encode deployment node %q: %w", nodeID, err)
	}
	var (
		incarnation int64
		appliedGen  int64
		appliedInc  int64
		storedData  string
	)
	err = st.db.QueryRowContext(ctx,
		`INSERT INTO deployment_nodes
			(node_id, generation, applied_generation, instance, incarnation,
			 applied_incarnation, updated_at, data)
		 VALUES ($1, $2, 0, $3, nextval('deployment_incarnation_seq'), 0, $4, $5)
		 ON CONFLICT (node_id) DO UPDATE SET
			generation = excluded.generation,
			instance = excluded.instance,
			incarnation = nextval('deployment_incarnation_seq'),
			updated_at = excluded.updated_at,
			data = excluded.data
		 WHERE excluded.generation >= deployment_nodes.generation
		 RETURNING incarnation, applied_generation, applied_incarnation, data::text`,
		nodeID, generation, instance, now, string(data),
	).Scan(&incarnation, &appliedGen, &appliedInc, &storedData)
	if errors.Is(err, sql.ErrNoRows) {
		return DeploymentNodeProgress{}, fmt.Errorf(
			"deployment node %q generation %d: %w", nodeID, generation, ErrDeploymentNodeSuperseded)
	}
	if err != nil {
		return DeploymentNodeProgress{}, fmt.Errorf("acquire deployment node %q: %w", nodeID, err)
	}
	var stored deploymentNodePayload
	if err := json.Unmarshal([]byte(storedData), &stored); err != nil {
		return DeploymentNodeProgress{}, fmt.Errorf("decode deployment node %q: %w", nodeID, err)
	}
	return DeploymentNodeProgress{
		NodeID:             nodeID,
		Generation:         generation,
		AppliedGeneration:  appliedGen,
		Instance:           instance,
		Incarnation:        incarnation,
		AppliedIncarnation: appliedInc,
		Phase:              stored.Phase,
		Commit:             stored.Commit,
		AppliedCommit:      stored.AppliedCommit,
		Error:              stored.Error,
		UpdatedAt:          timestampString(now),
	}, nil
}

// RecordDeploymentNodeProgress updates this node's progress only while the
// observed generation and incarnation still match. It reports whether the write
// landed; a stale incarnation/generation is a normal no-op. AppliedGeneration and
// AppliedIncarnation are written exactly as supplied.
func (st *State) RecordDeploymentNodeProgress(
	ctx context.Context,
	nodeID string,
	generation,
	incarnation int64,
	progress DeploymentNodeProgress,
) (bool, error) {
	if nodeID == "" || generation <= 0 || incarnation <= 0 {
		return false, fmt.Errorf("deployment node progress: node ID, generation, and incarnation are required")
	}
	now := st.nowTime().UTC()
	payload, err := json.Marshal(deploymentNodePayload{
		Phase:         progress.Phase,
		Commit:        progress.Commit,
		AppliedCommit: progress.AppliedCommit,
		Error:         progress.Error,
	})
	if err != nil {
		return false, fmt.Errorf("encode deployment node %q: %w", nodeID, err)
	}
	res, err := st.db.ExecContext(ctx,
		`UPDATE deployment_nodes
		 SET applied_generation = $1, applied_incarnation = $2,
		     updated_at = $3, data = $4
		 WHERE node_id = $5 AND generation = $6 AND incarnation = $7`,
		progress.AppliedGeneration, progress.AppliedIncarnation, now,
		string(payload), nodeID, generation, incarnation)
	if err != nil {
		return false, fmt.Errorf("record deployment node %q: %w", nodeID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("record deployment node %q: %w", nodeID, err)
	}
	return affected > 0, nil
}

// RecordDeploymentAppProgress writes per-app activation progress for the
// supplied generation. It is accepted only while the node's observed generation
// or applied generation matches that generation AND the supplied incarnation is
// the node's current incarnation. A stale process incarnation therefore cannot
// overwrite progress; a newer generation cannot be clobbered by a delayed older
// generation.
//
// The fence read locks the node row FOR SHARE so a concurrent acquire or node
// progress write is ordered against this statement: it either commits before the
// fence is evaluated or waits for this write, and a locking read re-checks the
// predicate against the newest row. A plain snapshot read could accept an
// incarnation that a concurrent acquire had already superseded.
//
// It reports whether the call inserted a row or changed the stored instance or
// payload. An identical fenced write leaves updated_at untouched and reports
// false, as does a stale incarnation or a generation the node has neither
// observed nor applied.
func (st *State) RecordDeploymentAppProgress(
	ctx context.Context,
	nodeID,
	appName string,
	generation,
	incarnation int64,
	progress DeploymentAppProgress,
) (bool, error) {
	if nodeID == "" || appName == "" || generation <= 0 || incarnation <= 0 {
		return false, fmt.Errorf(
			"deployment app progress: node ID, app, generation, and incarnation are required",
		)
	}
	now := st.nowTime().UTC()
	payload, err := json.Marshal(deploymentAppPayload{
		Phase:            progress.Phase,
		Fingerprint:      progress.Fingerprint,
		TemplateIdentity: progress.TemplateIdentity,
		Commit:           progress.Commit,
		Error:            progress.Error,
	})
	if err != nil {
		return false, fmt.Errorf("encode deployment app %q/%q: %w", nodeID, appName, err)
	}
	res, err := st.db.ExecContext(ctx,
		`INSERT INTO deployment_apps
			(node_id, app_name, generation, instance, updated_at, data)
		 SELECT $1, $2, $3, $4, $5, $6
		 WHERE EXISTS (
			SELECT 1 FROM deployment_nodes
			WHERE node_id = $1
			  AND incarnation = $7
			  AND (generation = $3 OR applied_generation = $3)
			FOR SHARE
		 )
		 ON CONFLICT (node_id, app_name, generation) DO UPDATE SET
			instance = excluded.instance,
			updated_at = excluded.updated_at,
			data = excluded.data
		 WHERE deployment_apps.instance IS DISTINCT FROM excluded.instance
		    OR deployment_apps.data IS DISTINCT FROM excluded.data`,
		nodeID, appName, generation, progress.Instance, now, string(payload), incarnation)
	if err != nil {
		return false, fmt.Errorf("record deployment app %q/%q: %w", nodeID, appName, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("record deployment app %q/%q: %w", nodeID, appName, err)
	}
	return affected > 0, nil
}

// ReadDeploymentNode returns one node's deployment progress.
func (st *State) ReadDeploymentNode(
	ctx context.Context,
	nodeID string,
) (DeploymentNodeProgress, bool, error) {
	var (
		generation int64
		appliedGen int64
		instance   string
		incarnation,
		appliedInc int64
		updatedAt time.Time
		data      string
	)
	err := st.db.QueryRowContext(ctx,
		`SELECT generation, applied_generation, instance, incarnation,
		        applied_incarnation, updated_at, data::text
		 FROM deployment_nodes WHERE node_id = $1`, nodeID,
	).Scan(&generation, &appliedGen, &instance, &incarnation,
		&appliedInc, &updatedAt, &data)
	if errors.Is(err, sql.ErrNoRows) {
		return DeploymentNodeProgress{}, false, nil
	}
	if err != nil {
		return DeploymentNodeProgress{}, false, fmt.Errorf(
			"read deployment node %q: %w", nodeID, err)
	}
	var payload deploymentNodePayload
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return DeploymentNodeProgress{}, false, fmt.Errorf(
			"decode deployment node %q: %w", nodeID, err)
	}
	return DeploymentNodeProgress{
		NodeID:             nodeID,
		Generation:         generation,
		AppliedGeneration:  appliedGen,
		Instance:           instance,
		Incarnation:        incarnation,
		AppliedIncarnation: appliedInc,
		Phase:              payload.Phase,
		Commit:             payload.Commit,
		AppliedCommit:      payload.AppliedCommit,
		Error:              payload.Error,
		UpdatedAt:          timestampString(updatedAt),
	}, true, nil
}

// ReadDeploymentNodes returns every node's progress ordered by node ID.
func (st *State) ReadDeploymentNodes(ctx context.Context) ([]DeploymentNodeProgress, error) {
	rows, err := st.db.QueryContext(ctx,
		`SELECT node_id, generation, applied_generation, instance, incarnation,
		        applied_incarnation, updated_at, data::text
		 FROM deployment_nodes ORDER BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("read deployment nodes: %w", err)
	}
	defer rows.Close()
	var nodes []DeploymentNodeProgress
	for rows.Next() {
		var (
			nodeID     string
			generation int64
			appliedGen int64
			instance   string
			incarnation,
			appliedInc int64
			updatedAt time.Time
			data      string
		)
		if err := rows.Scan(&nodeID, &generation, &appliedGen, &instance, &incarnation,
			&appliedInc, &updatedAt, &data); err != nil {
			return nil, fmt.Errorf("read deployment nodes: %w", err)
		}
		var payload deploymentNodePayload
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			return nil, fmt.Errorf("decode deployment node %q: %w", nodeID, err)
		}
		nodes = append(nodes, DeploymentNodeProgress{
			NodeID:             nodeID,
			Generation:         generation,
			AppliedGeneration:  appliedGen,
			Instance:           instance,
			Incarnation:        incarnation,
			AppliedIncarnation: appliedInc,
			Phase:              payload.Phase,
			Commit:             payload.Commit,
			AppliedCommit:      payload.AppliedCommit,
			Error:              payload.Error,
			UpdatedAt:          timestampString(updatedAt),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read deployment nodes: %w", err)
	}
	return nodes, nil
}

// ReadDeploymentAppProgress returns one node's per-app progress for one
// generation, ordered by app name. A zero generation returns every stored row,
// which the caller may use for diagnostics.
func (st *State) ReadDeploymentAppProgress(
	ctx context.Context,
	nodeID string,
	generation int64,
) ([]DeploymentAppProgress, error) {
	query := `SELECT app_name, generation, instance, updated_at, data::text
	          FROM deployment_apps WHERE node_id = $1`
	args := []any{nodeID}
	if generation > 0 {
		query += ` AND generation = $2`
		args = append(args, generation)
	}
	query += ` ORDER BY app_name`
	rows, err := st.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read deployment apps for %q: %w", nodeID, err)
	}
	defer rows.Close()
	var progress []DeploymentAppProgress
	for rows.Next() {
		var (
			appName   string
			gen       int64
			instance  string
			updatedAt time.Time
			data      string
		)
		if err := rows.Scan(&appName, &gen, &instance, &updatedAt, &data); err != nil {
			return nil, fmt.Errorf("read deployment apps for %q: %w", nodeID, err)
		}
		var payload deploymentAppPayload
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			return nil, fmt.Errorf("decode deployment app %q/%q: %w", nodeID, appName, err)
		}
		progress = append(progress, DeploymentAppProgress{
			NodeID:           nodeID,
			App:              appName,
			Generation:       gen,
			Instance:         instance,
			Phase:            payload.Phase,
			Fingerprint:      payload.Fingerprint,
			TemplateIdentity: payload.TemplateIdentity,
			Commit:           payload.Commit,
			Error:            payload.Error,
			UpdatedAt:        timestampString(updatedAt),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read deployment apps for %q: %w", nodeID, err)
	}
	return progress, nil
}

// PruneDeploymentAppProgress removes per-app rows older than the applied
// generation. It runs after a successful materialization advances the applied
// generation, so the previous generation's rows no longer serve as recovery
// authorization and cannot grow the table without bound.
func (st *State) PruneDeploymentAppProgress(
	ctx context.Context,
	nodeID string,
	appliedGeneration int64,
) error {
	if appliedGeneration <= 0 {
		return nil
	}
	if _, err := st.db.ExecContext(ctx,
		`DELETE FROM deployment_apps
		 WHERE node_id = $1 AND generation < $2`, nodeID, appliedGeneration); err != nil {
		return fmt.Errorf("prune deployment apps for %q: %w", nodeID, err)
	}
	return nil
}
