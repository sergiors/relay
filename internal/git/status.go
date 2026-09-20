package git

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/tabwriter"

	git "github.com/go-git/go-git/v5"
)

// Status gathers the current git-sync state for rendering by `relay git status`.
// It returns a snapshot struct that holds fields only — never key material, never
// credentials — so callers (and tests) render whatever they need. When no config
// is persisted, StatusConfig returns a zero value; callers distinguish the
// "nothing configured" case with errors.Is(err, ErrConfigNotFound()).
type StatusConfig struct {
	Configured bool
	Repository string
	Ref        string
	Path       string
	KeyExists  bool
	Checkout   bool
	Commit     string // resolved HEAD commit of the checkout, if any
	Synced     bool
	LastSynced string // RFC3339, if any
}

// Status builds a StatusConfig from the given dirs. It never fails on a missing
// checkout or key (those are just false fields); it only returns an error when
// the config file exists but is unreadable/corrupt.
func Status(cfgPath, sshDir, checkoutDir string) (StatusConfig, error) {
	s := StatusConfig{}
	cfg, err := LoadConfig(cfgPath)
	switch {
	case err == nil:
		s.Configured = true
		s.Repository = cfg.Repository
		s.Ref = cfg.Ref
		s.Path = cfg.Path
		s.Synced = cfg.Synced
		s.LastSynced = cfg.LastSyncedAt
	case errors.Is(err, ErrConfigNotFound()):
		// Not configured: that is a normal state, not an error.
	default:
		return s, err
	}

	s.KeyExists = keyExists(sshDir)
	if _, err := os.Stat(filepath.Join(checkoutDir, ".git")); err == nil {
		s.Checkout = true
		if r, oerr := git.PlainOpen(checkoutDir); oerr == nil {
			if h, herr := r.Head(); herr == nil {
				s.Commit = h.Hash().String()
			}
		}
	}
	return s, nil
}

// PrintStatus renders a StatusConfig to w with tab-aligned labels (the same
// presentation style as `relay function inspect`). It prints no key material
// and no repository credentials, only the summary fields.
func PrintStatus(w io.Writer, s StatusConfig) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if !s.Configured {
		// "No git source configured." is a normal, exit-0 condition (see the
		// CLI binding). It prints standalone so the operator sees at a glance
		// that no source is remembered.
		fmt.Fprintln(w, "No git source configured.")
		return
	}
	fmt.Fprintf(tw, "Repository:\t%s\n", s.Repository)
	fmt.Fprintf(tw, "Ref:\t%s\n", s.Ref)
	if s.Path != "" {
		fmt.Fprintf(tw, "Path:\t%s\n", s.Path)
	}
	fmt.Fprintf(tw, "SSH key:\t%s\n", yesNo(s.KeyExists))
	fmt.Fprintf(tw, "Checkout:\t%s\n", yesNo(s.Checkout))
	if s.Commit != "" {
		fmt.Fprintf(tw, "Resolved commit:\t%s\n", s.Commit)
	}
	fmt.Fprintf(tw, "Last sync:\t%s\n", syncedStr(s))
	_ = tw.Flush()
}

// yesNo renders a bool as "yes"/"no".
func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// syncedStr renders the sync bookkeeping: "never" when no sync has run, the
// RFC3339 timestamp otherwise.
func syncedStr(s StatusConfig) string {
	if !s.Synced {
		return "never"
	}
	return s.LastSynced
}
