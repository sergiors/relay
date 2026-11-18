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
