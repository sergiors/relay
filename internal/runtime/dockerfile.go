package runtime

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"unicode"

	"relay/internal/runtime/plan"
)

// renderDockerfile is the only Dockerfile renderer, shared by every engine; an
// engine expresses its needs as plan data rather than generating a Dockerfile.
// The app source is copied into WorkDir (the historical baked-source image).
func renderDockerfile(p plan.BuildPlan) (string, error) {
	return renderDockerfileWithSource(p, true)
}

// renderDockerfileWithSource makes the source COPY optional. copySource is false
// for a HOT_RELOAD image: the live source is bind-mounted at runtime instead of
// copied, while the generated plan files and the install/user/entrypoint steps
// are emitted exactly as before. The build context still holds the same
// immutable snapshot the fingerprint was derived from, so the tag and the
// mounted tree cannot describe different bytes.
func renderDockerfileWithSource(p plan.BuildPlan, copySource bool) (string, error) {
	// Validate the generated file list once and use the SAME relative paths the
	// stager writes, so a rendered COPY can never name a path the stager would
	// reject or resolve differently.
	planFileRels, err := validatePlanFiles(p.Files)
	if err != nil {
		return "", err
	}

	var b strings.Builder

	fmt.Fprintf(&b, "FROM %s\n", p.BaseImage)
	if p.WorkDir != "" {
		fmt.Fprintf(&b, "WORKDIR %s\n", p.WorkDir)
	}

	if copySource {
		dest := p.WorkDir
		if dest == "" {
			dest = "/"
		}
		fmt.Fprintf(&b, "COPY . %s\n", dest)
	}

	// Each file's absolute Path is mirrored into the context at its validated
	// relative path, so COPY names that same relative path and writes it into
	// the file's parent directory in the image.
	for i, f := range p.Files {
		fmt.Fprintf(&b, "COPY %s %s/\n", planFileRels[i], path.Dir(f.Path))
	}

	// Runtime tools are materialized before the install RUN so it can use them.
	if err := renderRuntimeTools(&b, p); err != nil {
		return "", err
	}

	for _, cmd := range p.Install {
		if cmd != "" {
			fmt.Fprintf(&b, "RUN %s\n", cmd)
		}
	}

	// UserSetup runs as root after Install (which needs root for
	// site-packages/node_modules) and before the USER switch.
	if p.UserSetup != "" {
		fmt.Fprintf(&b, "RUN %s\n", p.UserSetup)
	}
	if p.User != "" {
		fmt.Fprintf(&b, "USER %s\n", p.User)
	}
	if len(p.Entrypoint) > 0 {
		fmt.Fprintf(&b, "ENTRYPOINT %s\n", quoteEntrypointJSON(p.Entrypoint))
	}
	return b.String(), nil
}

// renderRuntimeTools writes the Dockerfile instructions for every runtime tool,
// in plan order. An image-copy tool becomes a COPY --from; an artifact tool
// becomes one fail-closed RUN. An artifact with no variant for the target
// architecture fails the render, so an unsupported target is a build-time error,
// never a silently missing tool.
func renderRuntimeTools(b *strings.Builder, p plan.BuildPlan) error {
	for i, tool := range p.RuntimeTools {
		if err := tool.ValidateForm(); err != nil {
			return fmt.Errorf("runtime tool %d: %w", i, err)
		}
		if tool.Artifact == nil {
			// Image-copy form: pull the pinned binary out of another image.
			if tool.From == "" || tool.Source == "" || tool.Destination == "" {
				return fmt.Errorf("runtime tool %d: incomplete image copy (from=%q source=%q destination=%q)",
					i, tool.From, tool.Source, tool.Destination)
			}
			fmt.Fprintf(b, "COPY --from=%s %s %s\n", tool.From, tool.Source, tool.Destination)
			continue
		}
		if p.TargetArch == "" {
			return fmt.Errorf("runtime tool %q: artifact requires a target architecture", tool.Destination)
		}
		variant, ok := tool.Artifact.VariantForArch(p.TargetArch)
		if !ok {
			return fmt.Errorf("runtime tool %q: no artifact variant for architecture %q", tool.Destination, p.TargetArch)
		}
		if err := validateArtifactVariant(variant); err != nil {
			return fmt.Errorf("runtime tool %q: %w", tool.Destination, err)
		}
		if tool.Destination == "" || !strings.HasPrefix(tool.Destination, "/") {
			return fmt.Errorf("runtime tool: artifact destination %q must be an absolute path", tool.Destination)
		}
		b.WriteString(renderArtifactTool(i, tool.Destination, variant))
	}
	return nil
}

// renderArtifactTool renders one fail-closed RUN that retrieves and verifies the
// pinned archive, extracts exactly the validated member, installs it at
// destination, and removes scratch paths before the layer commits. A
// checksum-keyed BuildKit cache mount shares verified archives across builds.
// Locked sharing serializes access while temporary data is verified/published.
func renderArtifactTool(i int, destination string, variant plan.ArtifactVariant) string {
	archive := fmt.Sprintf("/tmp/relay-tool-%d.tar.gz", i)
	unpack := fmt.Sprintf("/tmp/relay-tool-%d.unpack", i)
	destDir := path.Dir(destination)
	archiveArg := dockerfileShellArg(archive)
	unpackArg := dockerfileShellArg(unpack)
	destDirArg := dockerfileShellArg(destDir)
	destinationArg := dockerfileShellArg(destination)
	cacheID := "relay-artifact-" + variant.SHA256
	sources := make([]plan.ArtifactSource, 0, 1+len(variant.Fallbacks))
	sources = append(sources, plan.ArtifactSource{URL: variant.URL, SHA256: variant.SHA256, Member: variant.Member})
	sources = append(sources, variant.Fallbacks...)

	var b strings.Builder
	fmt.Fprintf(&b,
		"RUN --mount=type=cache,id=%s,target=/var/cache/relay-artifacts,sharing=locked set -eu; ", cacheID)
	b.WriteString("archive_sha256=; archive_member=; ")
	// Inspect every source's cache before any network access. The primary wins
	// when both caches are valid, while a verified fallback cache bypasses a
	// temporarily unavailable primary on future builds.
	for _, source := range sources {
		cached := artifactCachePath(source.SHA256)
		fmt.Fprintf(&b,
			"if [ -f %s ]; then if echo %s | sha256sum -c - >/dev/null 2>&1; then if [ ! -f %s ]; then cp %s %s; archive_sha256=%s; archive_member=%s; fi; else rm -f %s; fi; fi; ",
			dockerfileShellArg(cached),
			dockerfileShellArg(source.SHA256+"  "+cached),
			archiveArg, dockerfileShellArg(cached), archiveArg, source.SHA256, dockerfileShellArg(source.Member),
			dockerfileShellArg(cached))
	}
	for _, source := range sources {
		cached := artifactCachePath(source.SHA256)
		temp := artifactTempPath(source.SHA256)
		cachedArg := dockerfileShellArg(cached)
		tempArg := dockerfileShellArg(temp)
		fmt.Fprintf(&b,
			"if [ ! -f %s ]; then attempt=1; while [ \"$attempt\" -le 3 ]; do rm -f %s; "+
				"if busybox timeout -k 2 25 busybox wget -T 20 -O %s %s; then "+
				"if ! echo %s | sha256sum -c -; then rm -f %s %s %s %s; echo 'artifact SHA-256 mismatch' >&2; exit 1; fi; "+
				"mv %s %s; cp %s %s; archive_sha256=%s; archive_member=%s; break; fi; rm -f %s; "+
				"if [ \"$attempt\" -lt 3 ]; then sleep \"$attempt\"; fi; attempt=$((attempt + 1)); done; fi; ",
			archiveArg, tempArg, tempArg, dockerfileShellArg(source.URL),
			dockerfileShellArg(source.SHA256+"  "+temp), tempArg, cachedArg, archiveArg, unpackArg,
			tempArg, cachedArg, cachedArg, archiveArg, source.SHA256, dockerfileShellArg(source.Member), tempArg)
	}
	if len(sources) == 1 {
		fmt.Fprintf(&b, "if [ ! -f %s ]; then echo 'artifact download failed after 3 attempts' >&2; exit 1; fi; ", archiveArg)
	} else {
		fmt.Fprintf(&b, "if [ ! -f %s ]; then echo 'artifact download failed for all sources after 3 attempts each' >&2; exit 1; fi; ", archiveArg)
	}
	fmt.Fprintf(&b,
		"echo \"$archive_sha256  %s\" | sha256sum -c - && rm -rf %s && mkdir -p %s && ",
		archiveArg, unpackArg, unpackArg)
	fmt.Fprintf(&b, "tar -xzf %s -C %s \"$archive_member\" && mkdir -p %s && mv %s/\"$archive_member\" %s && chmod 0755 %s && rm -rf %s %s\n",
		archiveArg, unpackArg, destDirArg, unpackArg, destinationArg, destinationArg, unpackArg, archiveArg)
	return b.String()
}

func artifactCachePath(digest string) string {
	return "/var/cache/relay-artifacts/" + digest + ".tar.gz"
}

func artifactTempPath(digest string) string {
	// sharing=locked makes this deterministic temporary name safe even when
	// builds for the same artifact arrive concurrently.
	return "/var/cache/relay-artifacts/." + digest + ".download"
}

// validateArtifactVariant rejects a variant whose data could produce an unsafe
// or non-deterministic Dockerfile: a non-HTTPS URL, shell-hostile URL bytes, a
// malformed digest, or a member that is not a clean relative path. The plan data
// is trusted (registry constants); this is defense-in-depth against a future
// tool definition and makes path traversal impossible.
func validateArtifactVariant(v plan.ArtifactVariant) error {
	if v.Arch == "" {
		return fmt.Errorf("artifact variant has no architecture")
	}
	if !strings.HasPrefix(v.URL, "https://") {
		return fmt.Errorf("artifact variant for %s: url %q must be https", v.Arch, v.URL)
	}
	if strings.ContainsAny(v.URL, " \t\r\n\"'`\\") {
		return fmt.Errorf("artifact variant for %s: url %q contains unsafe characters", v.Arch, v.URL)
	}
	if len(v.SHA256) != 64 {
		return fmt.Errorf("artifact variant for %s: sha256 %q must be 64 hex characters", v.Arch, v.SHA256)
	}
	for _, c := range v.SHA256 {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return fmt.Errorf("artifact variant for %s: sha256 %q must be lowercase hex", v.Arch, v.SHA256)
		}
	}
	if err := validateArtifactMember(v.Member); err != nil {
		return err
	}
	for i, fallback := range v.Fallbacks {
		if err := validateArtifactSource(v.Arch, fallback); err != nil {
			return fmt.Errorf("fallback %d: %w", i, err)
		}
	}
	return nil
}

func validateArtifactSource(arch string, source plan.ArtifactSource) error {
	return validateArtifactVariant(plan.ArtifactVariant{
		Arch: arch, URL: source.URL, SHA256: source.SHA256, Member: source.Member,
	})
}

// validateArtifactMember rejects a tar member that is not a clean, non-option
// relative path: an empty member, an absolute path, a leading "-", a backslash,
// or any "."/".." segment could escape the extraction directory or be mistaken
// for a tar flag.
func validateArtifactMember(member string) error {
	if member == "" {
		return fmt.Errorf("artifact member is empty")
	}
	if strings.IndexFunc(member, unicode.IsControl) >= 0 {
		return fmt.Errorf("artifact member %q must not contain control characters", member)
	}
	if strings.HasPrefix(member, "/") || strings.HasPrefix(member, "-") {
		return fmt.Errorf("artifact member %q must be a relative path without a leading %q", member, string(member[0]))
	}
	if strings.Contains(member, "\\") {
		return fmt.Errorf("artifact member %q must not contain a backslash", member)
	}
	if path.Clean(member) != member {
		return fmt.Errorf("artifact member %q must be a clean relative path", member)
	}
	for seg := range strings.SplitSeq(member, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return fmt.Errorf("artifact member %q must not contain an empty, \".\", or \"..\" segment", member)
		}
	}
	return nil
}

// dockerfileShellArg renders s as exactly one POSIX shell word for a Dockerfile
// RUN. Conservative shell-safe bytes are emitted bare so ordinary paths and
// digests stay readable; anything else is single-quoted. Plan data is validated
// before it reaches here; the quoting is defense-in-depth so a future value
// cannot split or expand inside the RUN.
func dockerfileShellArg(s string) string {
	if s == "" {
		return "''"
	}
	if dockerfileShellSafe(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// dockerfileShellSafe reports whether every byte of s needs no quoting. It is
// deliberately conservative: a byte outside the set always triggers quoting.
func dockerfileShellSafe(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '_', c == '@', c == '%', c == '+', c == '=', c == ':', c == ',', c == '.', c == '/', c == '-':
		default:
			return false
		}
	}
	return true
}

// quoteEntrypointJSON renders an ENTRYPOINT as a JSON array so arguments with
// special characters are preserved.
func quoteEntrypointJSON(args []string) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, a := range args {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(strconv.Quote(a))
	}
	b.WriteByte(']')
	return b.String()
}
