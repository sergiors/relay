package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/moby/moby/client"
	"github.com/urfave/cli/v3"

	"relay/internal/registryauth"
	"relay/internal/runtime"
	"relay/internal/state"
)

// runtimeImageResolveTimeout bounds one Docker Engine Distribution API lookup.
// It is independent of the state transaction and long enough for an Engine that
// must authenticate and fetch registry metadata, while still failing a hung
// daemon or registry instead of blocking the CLI indefinitely.
const runtimeImageResolveTimeout = 60 * time.Second

// runtimeCommand builds the `relay runtime ...` subcommand family. `ls` and
// `inspect` read only the state database, so they work without a running
// worker, Docker, or a registry. `check` reads the state database and resolves
// the current registry digest read-only through the Docker Engine Distribution
// API; it never writes, pulls, builds, or restarts. `update` also resolves the
// configured base tag through that API and persists the multi-platform index
// digest with a compare-and-swap. Neither command pulls or builds an image,
// starts a worker, or restarts apps.
func runtimeCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "runtime",
		Usage: "Manage runtime base-image pins",
		Description: "Managed runtimes build from their configured base tag unless it is " +
			"pinned to an immutable registry digest.",
		Action: namespaceAction(),
		Commands: []*cli.Command{
			runtimeListCommand(deps),
			runtimeInspectCommand(deps),
			runtimeCheckCommand(deps),
			runtimeUpdateCommand(deps),
		},
	}
}

func runtimeListCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:  "ls",
		Usage: "List runtimes and image pins",
		Description: "List the managed runtimes this Relay binary supports with their configured " +
			"base images and pin state: pinned, unpinned, or invalid (a stored selection a worker " +
			"would reject). A stored selection shows its generation, selection time, and digest; " +
			"digests are abbreviated to sha256: plus the first 12 hex characters unless --no-trunc " +
			"is given.",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "no-trunc",
				Usage: "show complete digests instead of display-only abbreviations",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("runtime ls: too many arguments", 2)
			}
			return runtimeList(ctx, cmd.Writer, deps, !cmd.Bool("no-trunc"))
		},
	}
}

func runtimeInspectCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "inspect",
		Usage:     "Show one runtime's configuration and image pin",
		UsageText: "relay runtime inspect RUNTIME",
		Description: "Show a runtime's engine, base image, target platform, and effective " +
			"image reference. A pinned runtime also shows its source image, digest, generation, " +
			"and selection time; an unpinned runtime is shown as unpinned.",
		Arguments: []cli.Argument{
			&cli.StringArgs{Name: "runtime", Min: 1, Max: 1, UsageText: "RUNTIME"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("runtime inspect: too many arguments", 2)
			}
			return runtimeInspect(ctx, cmd.Writer, deps, cmd.StringArgs("runtime")[0])
		},
	}
}

func runtimeCheckCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "check",
		Usage:     "Check runtime image pins",
		UsageText: "relay runtime check [RUNTIME]",
		Description: "Compare one runtime, or every runtime when RUNTIME is omitted, with " +
			"the current registry digest. Validates any stored pin and reports it as matching, " +
			"different, invalid, or failed; unpinned runtimes are reported as available or " +
			"unpinned, never as matching. Performs no pull, build, restart, or write. " +
			"In all mode, each runtime is checked " +
			"independently and failures do not stop later runtimes; per-runtime errors are shown, " +
			"and the command exits non-zero if any runtime fails. --no-trunc shows complete " +
			"digests.",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "no-trunc",
				Usage: "show complete digests instead of display-only abbreviations",
			},
		},
		Arguments: []cli.Argument{
			&cli.StringArgs{Name: "runtime", Min: 0, Max: 1, UsageText: "[RUNTIME]"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("runtime check: too many arguments", 2)
			}
			runtimeIDs := cmd.StringArgs("runtime")
			var runtimeID string
			if len(runtimeIDs) > 0 {
				runtimeID = runtimeIDs[0]
			}
			return runtimeCheck(ctx, cmd.Writer, deps, runtimeID,
				!cmd.Bool("no-trunc"))
		},
	}
}

func runtimeUpdateCommand(deps Dependencies) *cli.Command {
	return &cli.Command{
		Name:      "update",
		Usage:     "Pin a runtime base image",
		UsageText: "relay runtime update RUNTIME | relay runtime update --all",
		Description: "Resolve the runtime's configured base tag to its registry digest and " +
			"persist the multi-platform index digest. The tag must resolve to an OCI image " +
			"index or Docker manifest list; missing, unknown, or platform-specific manifests " +
			"are rejected. " +
			"Writes nothing when the digest is unchanged. A single runtime reports the full " +
			"digest; --all updates every runtime independently, continues after failures, " +
			"abbreviates digests unless --no-trunc is given, and exits non-zero if any fail. " +
			"Does not pull, build, restart, or deploy; workers apply a new pin at their next " +
			"reconciliation pass, or at startup if no worker is running.",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "all",
				Aliases: []string{"a"},
				Usage:   "update every configured runtime independently",
			},
			&cli.BoolFlag{
				Name:  "no-trunc",
				Usage: "with --all, show complete digests instead of abbreviations",
			},
		},
		Arguments: []cli.Argument{
			&cli.StringArgs{Name: "runtime", Min: 0, Max: 1, UsageText: "[RUNTIME]"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Args().Present() {
				return cli.Exit("runtime update: too many arguments", 2)
			}
			runtimeIDs := cmd.StringArgs("runtime")
			all := cmd.Bool("all")
			noTrunc := cmd.Bool("no-trunc")
			switch {
			case all && len(runtimeIDs) > 0:
				return cli.Exit("runtime update: cannot combine --all with a runtime", 2)
			case !all && len(runtimeIDs) == 0:
				return cli.Exit("runtime update: missing runtime (or use --all)", 2)
			case !all && noTrunc:
				return cli.Exit("runtime update: --no-trunc requires --all", 2)
			case all:
				return runtimeUpdateAll(ctx, cmd.Writer, deps, !noTrunc)
			default:
				return runtimeUpdate(ctx, cmd.Writer, deps, runtimeIDs[0])
			}
		},
	}
}

// readRuntimeSelections reads the complete persisted selection snapshot from an
// already-open state handle. The state package returns the same rows the worker
// refreshes from, so the CLI never invents a second view of the pin.
func readRuntimeSelections(
	ctx context.Context,
	st *state.State,
) (map[string]state.RuntimeImageSelection, error) {
	selections, err := st.ReadRuntimeImageSelections(ctx)
	if err != nil {
		return nil, fmt.Errorf("runtime: read image selections: %w", err)
	}
	byRuntime := make(map[string]state.RuntimeImageSelection, len(selections))
	for _, selection := range selections {
		byRuntime[selection.RuntimeID] = selection
	}
	return byRuntime, nil
}

// runtimeSelections opens the state database for a read-only runtime command.
func runtimeSelections(
	ctx context.Context,
	deps Dependencies,
) (map[string]state.RuntimeImageSelection, error) {
	st, cleanup, err := deps.OpenState(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	return readRuntimeSelections(ctx, st)
}

func runtimeList(
	ctx context.Context,
	w io.Writer,
	deps Dependencies,
	truncate bool,
) error {
	selections, err := runtimeSelections(ctx, deps)
	if err != nil {
		return err
	}
	return renderRuntimeList(w, runtime.Runtimes(), selections, truncate)
}

// resolveRuntimeBaseImage performs one read-only Docker Engine Distribution API
// lookup for a configured runtime and validates the returned index metadata.
// It never pulls, builds, or writes state; the caller supplies the enclosing
// bounded context.
func resolveRuntimeBaseImage(
	ctx context.Context,
	resolver RuntimeImageResolver,
	info runtime.RuntimeInfo,
) (runtime.BaseImageResolution, error) {
	resolveCtx, cancel := context.WithTimeout(ctx, runtimeImageResolveTimeout)
	defer cancel()
	resolution, err := resolver.ResolveBaseImage(resolveCtx, info.BaseImage)
	if err != nil {
		return runtime.BaseImageResolution{}, err
	}
	if err := runtime.ValidateBaseImageResolution(info.BaseImage, resolution); err != nil {
		return runtime.BaseImageResolution{}, err
	}
	return resolution, nil
}

func runtimeInspect(
	ctx context.Context,
	w io.Writer,
	deps Dependencies,
	runtimeID string,
) error {
	info, ok := runtime.LookupRuntime(runtimeID)
	if !ok {
		return fmt.Errorf("unknown runtime %q", runtimeID)
	}
	selections, err := runtimeSelections(ctx, deps)
	if err != nil {
		return err
	}
	selection, pinned := runtimeSelectionFor(selections, runtimeID)
	return renderRuntimeInspect(w, info, selection, pinned)
}

func runtimeCheck(
	ctx context.Context,
	w io.Writer,
	deps Dependencies,
	runtimeID string,
	truncate bool,
) error {
	var infos []runtime.RuntimeInfo
	if runtimeID != "" {
		info, ok := runtime.LookupRuntime(runtimeID)
		if !ok {
			return fmt.Errorf("unknown runtime %q", runtimeID)
		}
		infos = []runtime.RuntimeInfo{info}
	} else {
		infos = runtime.Runtimes()
	}
	selections, err := runtimeSelections(ctx, deps)
	if err != nil {
		return err
	}

	// check is read-only: it opens the same Docker Engine resolver as update to
	// read the registry's current digest, but it never creates/updates a
	// selection, pulls, builds, or restarts anything. A resolver-open failure is
	// rendered per runtime so all-mode still reports every configured runtime.
	resolver, cleanup, resolverErr := openRuntimeImageResolver(ctx, deps)
	if resolverErr == nil {
		defer cleanup()
	}

	rows := make([]runtimeCheckRow, 0, len(infos))
	failures := 0
	for _, info := range infos {
		selection, pinned := runtimeSelectionFor(selections, info.ID)
		row := runtimeCheckRow{Runtime: info, Selection: selection, Pinned: pinned}
		if pinned {
			row.SelectionErr = runtime.ValidateRuntimeImageSelection(
				info.ID, selection.SourceImage, selection.Digest)
		}
		if resolverErr != nil {
			row.RemoteErr = resolverErr
		} else {
			resolution, err := resolveRuntimeBaseImage(ctx, resolver, info)
			if err != nil {
				row.RemoteErr = err
			} else {
				row.RemoteDigest = resolution.Digest
			}
		}
		row.Status = runtimeCheckStatus(row)
		if row.SelectionErr != nil || row.RemoteErr != nil {
			failures++
		}
		rows = append(rows, row)
	}
	if err := renderRuntimeCheck(w, rows, truncate); err != nil {
		return err
	}
	if failures > 0 {
		return fmt.Errorf("runtime check: %d runtime(s) failed", failures)
	}
	return nil
}

// runtimeCheckStatus classifies one local selection against the registry digest
// read by check. "available" means there is no persisted selection but the
// registry digest could be read, so an update is available; "unpinned" with a
// remote error means the selection is still honestly unpinned even though the
// registry lookup failed (the error is shown in the row and the command fails).
func runtimeCheckStatus(row runtimeCheckRow) string {
	switch {
	case row.SelectionErr != nil:
		return "invalid"
	case row.RemoteErr != nil:
		if row.Pinned {
			return "failed"
		}
		return "unpinned"
	case !row.Pinned:
		return "available"
	case row.Selection.Digest == row.RemoteDigest:
		return "matching"
	default:
		return "different"
	}
}

func runtimeUpdate(
	ctx context.Context,
	w io.Writer,
	deps Dependencies,
	runtimeID string,
) error {
	info, ok := runtime.LookupRuntime(runtimeID)
	if !ok {
		return fmt.Errorf("unknown runtime %q", runtimeID)
	}
	st, cleanup, err := deps.OpenState(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	selections, err := readRuntimeSelections(ctx, st)
	if err != nil {
		return err
	}
	resolver, resolverCleanup, err := openRuntimeImageResolver(ctx, deps)
	if err != nil {
		return fmt.Errorf("runtime update: %w", err)
	}
	defer resolverCleanup()

	selection, pinned := runtimeSelectionFor(selections, runtimeID)
	outcome := updateRuntimeSelection(ctx, st, resolver, info, selection, pinned)
	if outcome.Err != nil {
		// Render the failed attempt before returning the error: the operator
		// still needs the known previous identity, the digest the Engine
		// attempted (when resolution succeeded), and the failure detail. This
		// never labels an unpersisted digest as the selected pin.
		if err := renderRuntimeUpdate(w, outcome); err != nil {
			return err
		}
		return fmt.Errorf("runtime update %s: %w", runtimeID, outcome.Err)
	}
	if err := renderRuntimeUpdate(w, outcome); err != nil {
		return err
	}
	return renderRuntimeUpdateNote(w, outcome.Changed)
}

func runtimeUpdateAll(
	ctx context.Context,
	w io.Writer,
	deps Dependencies,
	truncate bool,
) error {
	infos := runtime.Runtimes()
	st, cleanup, stateErr := deps.OpenState(ctx)
	if stateErr == nil {
		defer cleanup()
	}
	var selections map[string]state.RuntimeImageSelection
	if stateErr == nil {
		selections, stateErr = readRuntimeSelections(ctx, st)
	}
	outcomes := make([]runtimeUpdateOutcome, 0, len(infos))

	var resolver RuntimeImageResolver
	var resolverCleanup func()
	var resolverErr error
	if stateErr == nil {
		resolver, resolverCleanup, resolverErr = openRuntimeImageResolver(ctx, deps)
		if resolverErr == nil {
			defer resolverCleanup()
		}
	}
	for _, info := range infos {
		var selection *state.RuntimeImageSelection
		var pinned bool
		if stateErr == nil {
			selection, pinned = runtimeSelectionFor(selections, info.ID)
		}
		outcome := runtimeUpdateOutcome{RuntimeID: info.ID}
		if stateErr == nil {
			outcome.PreviousKnown = true
			if pinned {
				outcome.PreviousPinned = true
				outcome.PreviousDigest = selection.Digest
			}
		}
		switch {
		case stateErr != nil:
			outcome.Status = runtimeUpdateFailed
			outcome.Err = stateErr
		case resolverErr != nil:
			outcome.Status = runtimeUpdateFailed
			outcome.Err = resolverErr
		default:
			outcome = updateRuntimeSelection(
				ctx, st, resolver, info, selection, pinned)
		}
		outcomes = append(outcomes, outcome)
	}
	if err := renderRuntimeUpdateAll(w, outcomes, truncate); err != nil {
		return err
	}

	failed := 0
	for _, outcome := range outcomes {
		if outcome.Status == runtimeUpdateFailed {
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("runtime update --all: %d runtime(s) failed", failed)
	}
	return nil
}

// runtimeUpdateOutcome is one runtime's independent `update --all` result. It
// carries the persisted digest for updated/unchanged and the failure reason for
// a failed runtime, so the renderer never turns a failure into a blank success.
type runtimeUpdateOutcome struct {
	RuntimeID      string
	Status         string
	PreviousKnown  bool
	PreviousPinned bool
	PreviousDigest string
	Digest         string
	Err            error
	Changed        bool
}

const (
	runtimeUpdateUpdated   = "updated"
	runtimeUpdateUnchanged = "unchanged"
	runtimeUpdateFailed    = "failed"
)

// errRuntimeSelectionChanged marks a no-op confirmation that found a different
// authoritative row under the shared selection lock. It never triggers a retry:
// the operator is told to rerun against the current pin.
var errRuntimeSelectionChanged = errors.New("runtime image selection changed concurrently")

// updateRuntimeSelection resolves one configured base reference, validates the
// Engine metadata, and compare-and-swaps the persisted row. selection is the
// snapshot read before any loop work; a stale generation is an explicit conflict,
// never a blind overwrite. An equal source image and digest performs no write, so
// its generation and timestamp remain untouched.
func updateRuntimeSelection(
	ctx context.Context,
	st *state.State,
	resolver RuntimeImageResolver,
	info runtime.RuntimeInfo,
	selection *state.RuntimeImageSelection,
	pinned bool,
) runtimeUpdateOutcome {
	outcome := runtimeUpdateOutcome{RuntimeID: info.ID, PreviousKnown: true}
	if pinned {
		outcome.PreviousPinned = true
		outcome.PreviousDigest = selection.Digest
	}

	resolution, err := resolveRuntimeBaseImage(ctx, resolver, info)
	if err != nil {
		outcome.Status = runtimeUpdateFailed
		outcome.Err = err
		return outcome
	}
	outcome.Digest = resolution.Digest

	if pinned &&
		selection.SourceImage == info.BaseImage &&
		selection.Digest == resolution.Digest {
		// The snapshot says this is already a no-op, but another writer could
		// have advanced the row after that read. Confirm the identity under the
		// SAME per-runtime lock every CAS write takes; do not invent a second
		// locking mechanism and do not retry blindly.
		var authoritativeDigest string
		var authoritativePinned bool
		err := st.WithRuntimeImageSelectionLock(ctx, info.ID,
			func(_ context.Context, current *state.RuntimeImageSelection) error {
				if current == nil ||
					current.SourceImage != info.BaseImage ||
					current.Digest != resolution.Digest {
					if current != nil {
						authoritativePinned = true
						authoritativeDigest = current.Digest
					}
					return errRuntimeSelectionChanged
				}
				return nil
			})
		if err != nil {
			outcome.Status = runtimeUpdateFailed
			if errors.Is(err, errRuntimeSelectionChanged) {
				current := "unpinned"
				if authoritativePinned {
					current = authoritativeDigest
				}
				outcome.Err = fmt.Errorf(
					"selection changed concurrently to %s; rerun to resolve the current pin",
					current)
			} else {
				outcome.Err = fmt.Errorf("confirm selection: %w", err)
			}
			return outcome
		}
		outcome.Status = runtimeUpdateUnchanged
		return outcome
	}

	expectedGeneration := int64(0)
	if pinned {
		expectedGeneration = selection.Generation
	}
	if _, err := st.CompareAndSwapRuntimeImageSelection(ctx, state.RuntimeImageSelection{
		RuntimeID:   info.ID,
		SourceImage: info.BaseImage,
		Digest:      resolution.Digest,
	}, expectedGeneration); err != nil {
		outcome.Status = runtimeUpdateFailed
		if errors.Is(err, state.ErrRuntimeImageSelectionConflict) {
			outcome.Err = fmt.Errorf(
				"selection changed concurrently (expected generation %d); rerun to resolve the current pin",
				expectedGeneration)
		} else {
			outcome.Err = fmt.Errorf("persist selection: %w", err)
		}
		return outcome
	}
	outcome.Status = runtimeUpdateUpdated
	outcome.Changed = true
	return outcome
}

// openRuntimeImageResolver opens the production Docker Engine resolver lazily,
// only for `runtime check` and `runtime update`. The local commands (`ls`,
// `inspect`) never call it, so they work without Docker. A nil seam is a clear
// configuration error, never a panic.
func openRuntimeImageResolver(
	ctx context.Context,
	deps Dependencies,
) (RuntimeImageResolver, func(), error) {
	if deps.OpenRuntimeImageResolver == nil {
		return nil, nil, errors.New("Docker Engine resolver is not configured")
	}
	resolver, cleanup, err := deps.OpenRuntimeImageResolver(ctx)
	if err != nil {
		return nil, nil, err
	}
	if resolver == nil {
		if cleanup != nil {
			cleanup()
		}
		return nil, nil, errors.New("Docker Engine resolver is unavailable")
	}
	if cleanup == nil {
		cleanup = func() {}
	}
	return resolver, cleanup, nil
}

// openDistributionResolver is the production RuntimeImageResolver opener. It
// uses the same Docker Engine client configuration as the worker and reads the
// operator's Docker client config for registry auth. It does not pull the image.
func openDistributionResolver(context.Context) (RuntimeImageResolver, func(), error) {
	cli, err := client.New(client.FromEnv)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to Docker Engine: %w", err)
	}
	auth, err := registryauth.Load()
	if err != nil {
		_ = cli.Close()
		return nil, nil, err
	}
	return runtime.NewDistributionResolver(cli, auth), func() { _ = cli.Close() }, nil
}
