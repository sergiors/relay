package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"relay/internal/runtime/plan"
)

// bootstrapHash returns a short (16 hex char) sha256 over the plan's
// runtime-injected files (the embedded bootstrap and anything else the engine
// injects, e.g. a package.json), the externally copied runtime tools (the pinned
// uv binary), the build-time Install commands (e.g. the pinned esbuild
// TypeScript transpilation), AND the entrypoint. It is the content the
// relay.bootstrap image label pins: an image whose label differs was built with a
// different (stale) bootstrap, tool version, or compile step and must be rebuilt
// even though its tag is content-current for the function source.
func bootstrapHash(p plan.BuildPlan) string {
	h := sha256.New()
	for _, f := range p.Files {
		_, _ = h.Write([]byte(f.Path))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(f.Content)
		_, _ = h.Write([]byte{0})
	}
	for _, tc := range p.ToolCopies {
		_, _ = h.Write([]byte(tc.From))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(tc.Source))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(tc.Dest))
		_, _ = h.Write([]byte{0})
	}
	for _, cmd := range p.Install {
		hashField(h, cmd)
	}
	for _, e := range p.Entrypoint {
		_, _ = h.Write([]byte(e))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// bootstrapLabelMatches reports whether the image with the given reference
// carries a relay.bootstrap label equal to want — i.e. it was built with the
// CURRENT bootstrap content. It is conservative in the same direction as
// imageExists: any inspect failure (daemon hiccup, label missing because the
// image predates the label) reports a mismatch so the caller rebuilds rather
// than reusing a possibly-stale image.
func (m *Manager) bootstrapLabelMatches(ctx context.Context, image, want string) bool {
	if want == "" {
		// No bootstrap content to pin (defensive; engines always inject one).
		return true
	}
	insp, err := m.cli.ImageInspect(ctx, image)
	if err != nil {
		m.log.Debug("Function: image inspect failed; rebuilding",
			"image", image, "error", err)
		return false
	}
	var labels map[string]string
	if insp.Config != nil {
		labels = insp.Config.Labels
	}
	if labels == nil || labels[labelBootstrap] != want {
		m.log.Debug("Function: image has stale bootstrap; rebuilding",
			"image", image)
		return false
	}
	return true
}
