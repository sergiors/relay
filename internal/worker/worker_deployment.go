package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"relay/internal/app"
	"relay/internal/config"
	"relay/internal/deployment"
	"relay/internal/git"
	gitwh "relay/internal/git/webhook"
	"relay/internal/secrets"
	"relay/internal/state"
)

// newDeploymentCoordinator classifies the local Git source and constructs the
// coordinated-deployment coordinator for it. Coordinated deployments are
// source-driven: a configured source makes this worker participate
// automatically, and the coordinator performs no I/O at construction (startup
// observation/materialization runs after the external-dependency preflight and
// before /apps is loaded).
//
// Only an ABSENT source (ErrConfigNotFound) keeps the existing local-/apps
// behavior and avoids all deployment state/progress access; a present but
// invalid/corrupt source fails startup closed rather than activating a
// divergent local tree.
func newDeploymentCoordinator(
	logger *slog.Logger,
	cfg config.Config,
	st *state.State,
) (*deployment.Coordinator, error) {
	sourceCfg, err := git.LoadConfig(git.ConfigPath)
	if err == nil {
		coordinator := deployment.New(deployment.Config{
			State:          st,
			NodeID:         cfg.NodeID,
			InstanceID:     cfg.InstanceID,
			GitConfigPath:  git.ConfigPath,
			GitCheckoutDir: git.CheckoutDir,
			GitSSHDir:      git.SSHDir,
			AppsDir:        app.Dir,
			Logger:         logger,
		})
		logger.Info("Deployment: coordinated source configured",
			"repository", sourceCfg.Repository,
			"ref", sourceCfg.Ref)
		return coordinator, nil
	}
	if errors.Is(err, git.ErrConfigNotFound()) {
		logger.Debug("Deployment: no local Git source; using local /apps mode")
		return nil, nil
	}
	return nil, fmt.Errorf("deployment: invalid local git source: %w", err)
}

// deploymentStartupValidation returns the coordinated-deployment recovery check
// for the startup loader's own candidates and issues. It is nil for a standalone
// worker, so local-mode startup is unchanged. Production startup uses this
// instead of a separate validation scan, so a source edit cannot land between
// the validated tree and the consumed tree.
func deploymentStartupValidation(
	ctx context.Context,
	snapshot deployment.Snapshot,
	active bool,
) func([]startupApp, []app.LoadIssue) error {
	if !active {
		return nil
	}
	return func(candidates []startupApp, issues []app.LoadIssue) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if verr := validateStartupAuthorization(
			snapshot, candidates, issues,
		); verr != nil {
			return fmt.Errorf("deployment: startup authorization incomplete: %w", verr)
		}
		return nil
	}
}

// validateStartupAuthorization is the pure startup-authorization proof over the
// loader's already-computed candidates and issues. It fails startup closed when
// the authorization the worker is about to consume cannot be served from the
// local /apps tree: an authoritative recovered generation must have every
// persisted valid app present locally with BOTH its materialized content
// fingerprint and its recorded raw template identity. The fingerprint
// deliberately excludes runtime-only template fields, so it alone cannot prove
// that the template startup would activate is the one the generation
// materialized. Without this proof an app could silently leave the startup
// registry (and its matched deliveries be ACKed unmatched) or activate a changed
// template before the reconciler's asynchronous drift check.
//
// Strictness depends on how the snapshot was obtained. When a published desired
// revision could not be materialized (Snapshot.Error set), Git may have left
// /apps partially synchronized to that revision, so ANY extra app candidate also
// fails the check, desired-invalid apps must still be present with their recorded
// identity matched exactly (an unreadable local template is not proof), and a
// locally VALID app cannot stand in for one the generation materialized as
// invalid (a valid parse is never proof of invalid content). Without a failed
// materialization, ordinary local drift — a local-only app or a local invalid
// edit — is still filtered by the normal startup authorization path and restored
// by the reconciler. A changed template identity for an authorized app fails in
// every case because the exact carried identity is the activation boundary.
// With neither an authoritative generation nor a failed materialization
// (nothing published yet), there is nothing to verify.
func validateStartupAuthorization(
	snapshot deployment.Snapshot,
	candidates []startupApp,
	issues []app.LoadIssue,
) error {
	if !snapshot.Authoritative {
		if snapshot.Error == nil {
			return nil
		}
		return startupAuthorizationError(snapshot,
			"published deployment revision could not be materialized and no previous "+
				"generation is authorized")
	}
	byName := make(map[string]startupApp, len(candidates))
	for _, candidate := range candidates {
		byName[candidate.App.Name] = candidate
	}
	issuesByName := make(map[string]app.LoadIssue, len(issues))
	for _, issue := range issues {
		issuesByName[issue.Name] = issue
	}
	// A failed materialization can leave the tree partially synchronized to the
	// published revision, which makes an extra candidate evidence of an
	// incomplete target rather than ordinary local drift.
	partialTarget := snapshot.Error != nil

	for name, expected := range snapshot.Fingerprints {
		if _, unloadable := issuesByName[name]; unloadable {
			return startupAuthorizationError(snapshot,
				"authorized app %q can no longer be loaded from the local tree", name)
		}
		candidate, found := byName[name]
		if !found {
			return startupAuthorizationError(snapshot,
				"authorized app %q is missing from the local tree; the apps tree may be "+
					"only partially materialized", name)
		}
		if candidate.Fingerprint != expected {
			return startupAuthorizationError(snapshot,
				"authorized app %q no longer matches its materialized fingerprint; the "+
					"apps tree may be only partially materialized", name)
		}
		expectedIdentity := snapshot.TemplateIdentities[name]
		if expectedIdentity == "" {
			return startupAuthorizationError(snapshot,
				"authorized app %q has no recorded template identity; its materialized "+
					"template cannot be proven", name)
		}
		// TemplateIdentity is carried from the exact bytes the loader parsed, so
		// this proof is over the same scan that produced this candidate rather
		// than a later re-read that could observe different content. Runtime-only
		// template fields are absent from the content fingerprint, so a mismatch
		// here means startup would otherwise activate a changed template.
		if candidate.App.TemplateIdentity != expectedIdentity {
			return startupAuthorizationError(snapshot,
				"app %q template bytes no longer match the materialized revision", name)
		}
	}
	for _, candidate := range candidates {
		name := candidate.App.Name
		if _, authorized := snapshot.Fingerprints[name]; authorized {
			continue
		}
		if _, invalid := snapshot.Invalid[name]; invalid {
			// A VALID local candidate for a name the previous generation materialized
			// as invalid is not proof of that generation: a successful parse cannot be
			// the invalid materialization (even when a recorded identity happens to
			// match a fingerprint-walk failure), and after a failed materialization it
			// may be newly corrected target content. Ordinary non-partial startup keeps
			// the existing filtering: the name is simply not authorized to activate.
			if partialTarget {
				return startupAuthorizationError(snapshot,
					"local app %q loads as a valid app but the previous generation "+
						"materialized it as invalid; the apps tree may belong to a "+
						"partially materialized target", name)
			}
			continue
		}
		if partialTarget {
			return startupAuthorizationError(snapshot,
				"local app %q is not part of the previous authorized generation; the "+
					"apps tree may be only partially materialized", name)
		}
	}
	for _, issue := range issues {
		name := issue.Name
		if _, authorized := snapshot.Fingerprints[name]; authorized {
			// Rejected by the authorized-app pass above.
			continue
		}
		if _, invalid := snapshot.Invalid[name]; invalid {
			// A desired-invalid app is part of the materialized revision but is never
			// authorized to activate. While a partially materialized target is being
			// recovered its local invalid bytes are part of the proof, so a recorded
			// identity must be matched exactly: an unreadable/unhashable local template
			// cannot be proven equal to it either.
			if expected := snapshot.InvalidTemplates[name]; partialTarget && expected != "" &&
				issue.Identity != expected {
				return startupAuthorizationError(snapshot,
					"desired-invalid app %q template bytes cannot be proven to match the "+
						"materialized revision", name)
			}
			continue
		}
		if partialTarget {
			return startupAuthorizationError(snapshot,
				"local app %q is not part of the previous authorized generation; the "+
					"apps tree may be only partially materialized", name)
		}
	}
	if partialTarget {
		for name := range snapshot.Invalid {
			if _, ok := byName[name]; ok {
				continue
			}
			if _, ok := issuesByName[name]; ok {
				continue
			}
			return startupAuthorizationError(snapshot,
				"desired-invalid app %q is missing from the local tree", name)
		}
	}
	return nil
}

// startupAuthorizationError builds a fail-closed startup diagnostic. A failed
// materialization's own diagnosis is wrapped too, so the operator sees both the
// incomplete recovery and its underlying cause.
func startupAuthorizationError(snapshot deployment.Snapshot, format string, args ...any) error {
	message := fmt.Sprintf(format, args...)
	if snapshot.Error != nil {
		return fmt.Errorf("%s: %w", message, snapshot.Error)
	}
	return errors.New(message)
}

// newGitWebhookServer constructs the optional Git webhook server, gated on
// GIT_WEBHOOK_ADDR. NewServer owns assembly and returns nil when disabled; the
// worker only starts and registers its teardown (see startWebhookServer).
//
// On a source-driven coordinator-active worker a matching push publishes the
// pushed immutable SHA as the shared desired revision. A worker without a
// configured Git source keeps the existing coalescing local branch sync; with no
// coordinator the PublishRevision hook stays nil, selecting that legacy path.
func newGitWebhookServer(
	logger *slog.Logger,
	cfg config.Config,
	secretProvider secrets.Provider,
	st *state.State,
	deploymentCoordinator *deployment.Coordinator,
) *gitwh.Server {
	if cfg.GitWebhookAddr == "" {
		return nil
	}
	webhookCfg := gitwh.Config{Secrets: secretProvider}
	if deploymentCoordinator != nil {
		webhookCfg.PublishRevision = webhookPublishRevision(
			logger, st, deploymentCoordinator, cfg.NodeID,
		)
	}
	return gitwh.NewServer(cfg.GitWebhookAddr, logger, webhookCfg)
}

// webhookPublishRevision builds the PublishRevision callback that publishes one
// matched push as the shared desired deployment revision. ExpectedGeneration -1
// compares against whatever is current under the publication lock, and
// RejectStaleSource accepts the push only when it chains directly from the
// provider's previous tip (before). A published revision wakes the coordinator
// immediately; that wake is non-forcing because the commit already changed the
// desired generation, so a fresh read is all this node needs. The database
// hint reaches every other node without a forced rematerialization.
func webhookPublishRevision(
	logger *slog.Logger,
	st *state.State,
	deploymentCoordinator *deployment.Coordinator,
	nodeID string,
) gitwh.PublishRevisionFunc {
	return func(
		ctx context.Context, source git.Config, before, commit, sourceTime string,
	) (bool, error) {
		result, err := st.PublishDeploymentDesired(ctx, state.DeploymentPublishRequest{
			Desired: state.DeploymentDesired{
				Commit:     commit,
				Repository: source.Repository,
				Ref:        source.Ref,
				Path:       source.Path,
				SourceTime: sourceTime,
				Publisher:  "webhook:" + nodeID,
			},
			ExpectedGeneration: -1,
			RejectStaleSource:  true,
			PreviousCommit:     before,
		})
		if err != nil {
			return false, err
		}
		if result.Published {
			logger.Info("Deployment: webhook published desired revision",
				"generation", result.Desired.Generation,
				"commit", result.Desired.Commit)
			deploymentCoordinator.Wake()
		}
		return result.Published, nil
	}
}
