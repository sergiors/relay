package cli

import (
	"fmt"
	"io"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"relay/internal/runtime"
	"relay/internal/state"
)

const runtimeDigestDisplayHexLength = 12

func runtimeDigestForDisplay(digest string, truncate bool) string {
	if !truncate || !runtime.ValidImageDigest(digest) {
		return digest
	}
	return digest[:len("sha256:")+runtimeDigestDisplayHexLength]
}

// runtimeSelectionFor resolves one persisted row from the complete snapshot. A
// missing entry is unpinned; the returned pointer is to a copy owned by the
// caller, never to the state package's internal storage.
func runtimeSelectionFor(
	selections map[string]state.RuntimeImageSelection,
	runtimeID string,
) (*state.RuntimeImageSelection, bool) {
	selection, ok := selections[runtimeID]
	if !ok {
		return nil, false
	}
	return &selection, true
}

// runtimeSelectionStatus labels a selection exactly as an operator needs to read
// it: "pinned" has a structural valid row, "unpinned" has no row, and "invalid"
// has a row a worker would reject (for example a source image that no longer
// matches the configured base).
func runtimeSelectionStatus(
	runtimeID string,
	selection *state.RuntimeImageSelection,
	pinned bool,
) (string, error) {
	if !pinned {
		return "unpinned", nil
	}
	if err := runtime.ValidateRuntimeImageSelection(
		runtimeID, selection.SourceImage, selection.Digest); err != nil {
		return "invalid", err
	}
	return "pinned", nil
}

func renderRuntimeList(
	w io.Writer,
	infos []runtime.RuntimeInfo,
	selections map[string]state.RuntimeImageSelection,
	truncate bool,
) error {
	out := newOutput(w)
	tw := out.table(2)
	fmt.Fprintln(tw, "RUNTIME\tBASE IMAGE\tSTATE\tGENERATION\tSELECTED AT\tDIGEST")
	for _, info := range infos {
		selection, pinned := runtimeSelectionFor(selections, info.ID)
		status, _ := runtimeSelectionStatus(info.ID, selection, pinned)
		generation := "-"
		selectedAt := "-"
		digest := "-"
		if pinned {
			generation = fmt.Sprintf("%d", selection.Generation)
			selectedAt = selection.SelectedAt
			if selectedAt == "" {
				selectedAt = "-"
			}
			digest = runtimeDigestForDisplay(selection.Digest, truncate)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			info.ID, info.BaseImage, status, generation, selectedAt, digest)
	}
	out.flush(tw)
	return out.err
}

// formatPlatform renders the authoritative OCI build platform for an operator.
// The platform values themselves come from runtime.Runtimes, never a CLI-side
// architecture mapping.
func formatPlatform(platform ocispec.Platform) string {
	if platform.Variant == "" {
		return platform.OS + "/" + platform.Architecture
	}
	return platform.OS + "/" + platform.Architecture + "/" + platform.Variant
}

func renderRuntimeInspect(
	w io.Writer,
	info runtime.RuntimeInfo,
	selection *state.RuntimeImageSelection,
	pinned bool,
) error {
	out := newOutput(w)
	tw := out.table(2)
	fmt.Fprintf(tw, "Runtime:\t%s\n", info.ID)
	fmt.Fprintf(tw, "Engine:\t%s\n", info.Engine)
	fmt.Fprintf(tw, "Base image:\t%s\n", info.BaseImage)
	fmt.Fprintf(tw, "Target platform:\t%s\n", formatPlatform(info.TargetPlatform))

	status, validationErr := runtimeSelectionStatus(info.ID, selection, pinned)
	fmt.Fprintf(tw, "Selection:\t%s\n", status)
	effectiveDigest := ""
	if pinned {
		effectiveDigest = selection.Digest
		fmt.Fprintf(tw, "Source image:\t%s\n", selection.SourceImage)
		fmt.Fprintf(tw, "Digest:\t%s\n", selection.Digest)
		fmt.Fprintf(tw, "Generation:\t%d\n", selection.Generation)
		if selection.SelectedAt != "" {
			fmt.Fprintf(tw, "Selected:\t%s (%s)\n",
				selection.SelectedAt, relativeAgo(selection.SelectedAt))
		}
	}
	fmt.Fprintf(tw, "Effective image:\t%s\n",
		runtime.EffectiveBaseImage(info.BaseImage, effectiveDigest))
	if validationErr != nil {
		fmt.Fprintf(tw, "Problem:\t%s\n", validationErr)
	}
	out.flush(tw)
	if !pinned {
		out.line("")
		out.line("This runtime is unpinned and builds from the configured base image tag.")
	}
	return out.err
}

type runtimeCheckRow struct {
	Runtime      runtime.RuntimeInfo
	Selection    *state.RuntimeImageSelection
	Pinned       bool
	SelectionErr error
	RemoteDigest string
	RemoteErr    error
	Status       string
}

func renderRuntimeCheck(
	w io.Writer,
	rows []runtimeCheckRow,
	truncate bool,
) error {
	out := newOutput(w)
	tw := out.table(2)
	fmt.Fprintln(tw, "RUNTIME\tSTATUS\tSELECTED\tREMOTE\tDETAIL")
	for _, row := range rows {
		selected := "unpinned"
		if row.Pinned && row.Selection != nil {
			selected = runtimeDigestForDisplay(row.Selection.Digest, truncate)
		}
		remote := row.RemoteDigest
		if remote == "" {
			remote = "-"
		} else {
			remote = runtimeDigestForDisplay(remote, truncate)
		}
		var details []string
		if row.SelectionErr != nil {
			details = append(details, "selection: "+row.SelectionErr.Error())
		}
		if row.RemoteErr != nil {
			details = append(details, "registry: "+row.RemoteErr.Error())
		}
		detail := "-"
		if len(details) > 0 {
			detail = strings.Join(details, "; ")
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			row.Runtime.ID, row.Status, selected, remote, detail)
	}
	out.flush(tw)
	return out.err
}

// previousSelectionLabel renders the pre-update identity for operator output.
// "unpinned" is distinct from a digest, and an unknown pre-read state (for
// example a database open failure in --all) is "-" rather than a fabricated
// unpinned claim.
func previousSelectionLabel(outcome runtimeUpdateOutcome) string {
	if !outcome.PreviousKnown {
		return "-"
	}
	if !outcome.PreviousPinned {
		return "unpinned"
	}
	return outcome.PreviousDigest
}

func renderRuntimeUpdate(w io.Writer, outcome runtimeUpdateOutcome) error {
	out := newOutput(w)
	tw := out.table(2)
	fmt.Fprintf(tw, "Runtime:\t%s\n", outcome.RuntimeID)
	fmt.Fprintf(tw, "Status:\t%s\n", outcome.Status)
	fmt.Fprintf(tw, "Previous:\t%s\n", previousSelectionLabel(outcome))
	if outcome.Status == runtimeUpdateFailed {
		attempted := outcome.Digest
		if attempted == "" {
			attempted = "-"
		}
		fmt.Fprintf(tw, "Attempted:\t%s\n", attempted)
		if outcome.Err != nil {
			fmt.Fprintf(tw, "Error:\t%s\n", outcome.Err.Error())
		}
	} else {
		fmt.Fprintf(tw, "Selected:\t%s\n", outcome.Digest)
	}
	out.flush(tw)
	return out.err
}

func renderRuntimeUpdateNote(w io.Writer, changed bool) error {
	out := newOutput(w)
	out.line("")
	if changed {
		out.line("Selection saved. Workers apply it during their next periodic reconciliation pass;")
		out.line("this command does not build, pull, restart, or deploy any app.")
	} else {
		out.line("No selection change; workers continue using the existing pin.")
	}
	return out.err
}

func renderRuntimeUpdateAll(
	w io.Writer,
	outcomes []runtimeUpdateOutcome,
	truncate bool,
) error {
	out := newOutput(w)
	tw := out.table(2)
	fmt.Fprintln(tw, "RUNTIME\tSTATUS\tPREVIOUS\tNEW\tDETAIL")
	updated, unchanged, failed := 0, 0, 0
	for _, outcome := range outcomes {
		selected := outcome.Digest
		if selected == "" {
			selected = "-"
		}
		detail := "-"
		switch outcome.Status {
		case runtimeUpdateUpdated:
			updated++
		case runtimeUpdateUnchanged:
			unchanged++
		case runtimeUpdateFailed:
			failed++
		}
		if outcome.Err != nil {
			detail = outcome.Err.Error()
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			outcome.RuntimeID, outcome.Status,
			runtimeDigestForDisplay(previousSelectionLabel(outcome), truncate),
			runtimeDigestForDisplay(selected, truncate), detail)
	}
	out.flush(tw)
	out.blank()
	out.line("Updated %d, unchanged %d, failed %d.", updated, unchanged, failed)
	if updated > 0 {
		out.line("Workers apply updated selections during their next periodic reconciliation pass;")
		out.line("this command does not build, pull, restart, or deploy any app.")
	}
	return out.err
}
