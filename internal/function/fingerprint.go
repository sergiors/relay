package function

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"relay/internal/source"
)

// runtimeOnlyTemplateKeys are the top-level template keys that configure how
// Relay RUNS a function but never what goes INTO its image. They are removed
// from template.yaml before the IMAGE fingerprint is computed (see
// ImageFingerprint), so editing them — a network change, say — invalidates warm
// and service containers WITHOUT forcing an image rebuild. The full
// Fingerprint deliberately still covers them, so a runtime-only edit is still
// detected by the reconciler and reaches the live container set.
var runtimeOnlyTemplateKeys = []string{"networks"}

// Fingerprint returns a deterministic SHA-256 over the complete contents of the
// function's SELECTED source: every included file's slash-separated relative path
// plus its bytes, sorted by path. template.yaml is hashed VERBATIM (including
// runtime-only keys such as `networks`), so any template edit is visible to the
// reconciler as a content change.
//
// Selection is the shared source policy (internal/source): the function's own
// .gitignore rules decide which files under dir are source. A file the rules
// ignore is not source, so its bytes never enter the digest — editing or removing
// it cannot force a rebuild. The applicable .gitignore files themselves ARE source
// (the policy is an input to selection), so their content is hashed: editing a
// rule changes the digest, which is exactly the change-detection guarantee a
// rule-only edit needs (a rule edit can alter the source set without any included
// file changing).
//
// Renames (path change), adds, removes, and content edits to included files all
// change the digest. File permissions are intentionally excluded: mode changes
// are rare and do not alter the image inputs (the build copies dirs wholesale).
// An unreadable included file — or an unreadable .gitignore — is surfaced as an
// error so the reconciler can retain the previous version rather than guessing.
func Fingerprint(dir string) (string, error) {
	sel, err := source.ForDir(dir)
	if err != nil {
		return "", fmt.Errorf("select %q: %w", dir, err)
	}
	return FingerprintSelection(sel)
}

// FingerprintSelection fingerprints an already-resolved source selection. It is
// the seam the manager uses when it has already selected a function's source (so
// the policy is not re-derived), and it keeps the hashing logic in one place.
func FingerprintSelection(sel *source.Selection) (string, error) {
	return fingerprintSelection(sel, nil)
}

// ImageFingerprint returns the fingerprint that versions a function's IMAGE: the
// same digest as Fingerprint, except that template.yaml's runtime-only top-level
// keys (see runtimeOnlyTemplateKeys) are removed before hashing. A template that
// declares no runtime-only key is hashed verbatim, so it fingerprints EXACTLY as
// it did before the runtime/content split existed.
//
// It exists so a runtime-only template edit — a `networks` change, which only
// affects which Docker networks containers join — does NOT yield a new image
// tag and therefore does NOT rebuild the image. The reconciler still detects the
// edit through the full Fingerprint and re-prepares; the reused image is
// unchanged, and the runtime generation (networks) reaches the container set.
func ImageFingerprint(dir string) (string, error) {
	sel, err := source.ForDir(dir)
	if err != nil {
		return "", fmt.Errorf("select %q: %w", dir, err)
	}
	return ImageFingerprintSelection(sel)
}

// ImageFingerprintSelection is ImageFingerprint over an already-resolved
// selection, for callers (the runtime manager and the service build path) that
// already hold one. It shares FingerprintSelection's file-walk and hashing; only
// the template.yaml bytes differ.
func ImageFingerprintSelection(sel *source.Selection) (string, error) {
	return fingerprintSelection(sel, templateImageContent)
}

// fingerprintSelection hashes the selected source. When transform is non-nil it
// is applied to each included file's relative path and raw bytes to produce the
// bytes that are hashed (nil means hash the raw file). A transform returning nil
// leaves the raw bytes unchanged.
func fingerprintSelection(sel *source.Selection, transform func(rel string, raw []byte) []byte) (string, error) {
	// Collect the included files as (hash path, filesystem path) pairs. The hash
	// path is root-relative and slash-separated so the serialization is canonical
	// and independent of how the walk produced absolute paths.
	type entry struct{ rel, path string }
	var entries []entry
	err := sel.WalkDir(func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := sel.Rel(path)
		if err != nil {
			return err
		}
		entries = append(entries, entry{rel: rel, path: path})
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("walk %q: %w", sel.Dir(), err)
	}
	seen := make(map[string]bool, len(entries))
	for _, e := range entries {
		seen[e.rel] = true
	}
	// A subtree walk cannot visit policy files above the selected directory,
	// but those files still affect the selected source and must be versioned.
	for _, path := range sel.ApplicableIgnoreFiles() {
		rel, err := sel.Rel(path)
		if err != nil {
			continue
		}
		if seen[rel] {
			continue
		}
		entries = append(entries, entry{rel: rel, path: path})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].rel < entries[j].rel })

	h := sha256.New()
	for _, e := range entries {
		io.WriteString(h, e.rel)
		h.Write([]byte{0})
		f, err := os.Open(e.path)
		if err != nil {
			return "", fmt.Errorf("read %q: %w", e.rel, err)
		}
		raw, err := io.ReadAll(f)
		if err != nil {
			_ = f.Close()
			return "", fmt.Errorf("read %q: %w", e.rel, err)
		}
		if err := f.Close(); err != nil {
			return "", fmt.Errorf("close %q: %w", e.rel, err)
		}
		if transform != nil {
			if transformed := transform(e.rel, raw); transformed != nil {
				raw = transformed
			}
		}
		h.Write(raw)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// templateImageContent returns the bytes hashed for the IMAGE fingerprint: the
// template.yaml's raw bytes with the runtime-only top-level keys (see
// runtimeOnlyTemplateKeys) removed — and NOTHING else changed. It returns nil
// when the file is not template.yaml, is not a single top-level YAML mapping, or
// does not contain any runtime-only key; the caller then hashes the raw bytes.
//
// Removing only the runtime-only key's own bytes keeps the transform INVARIANT
// to adding, removing, or changing that key while leaving every other byte of
// the document untouched. That is what lets a `networks`-only edit reuse the
// existing image with no rebuild: the spliced remainder is byte-identical to a
// template authored without the key. It also means a template with no
// runtime-only key fingerprints its raw bytes verbatim — exactly the pre-split
// image fingerprint — so upgrading Relay does not rebuild every function that
// does not use `networks`.
//
// A whole-document re-marshal (yaml.Unmarshal into a map, then yaml.Marshal)
// would instead drop comments and rewrite sequence indentation, changing the
// digest of the NON-runtime content too. That both changed the image
// fingerprint of every existing no-`networks` function (forcing a spurious
// rebuild) and made "adding" or "removing" `networks` a rebuild, because the
// canonicalized remainder did not equal the raw remainder. The byte splice
// avoids all of that.
//
// In a block root mapping the removed span runs from the key's line start
// through the end of its value subtree, consuming the trailing newline so the
// join is exact. A comment attached above the key is left in place: deleting the
// key in an editor does the same. In a flow-style root mapping that places
// several keys on one line, removing the line would drop the siblings, so only
// the pair's own bytes (key through the value's closing bracket, plus one
// adjacent separator comma) are removed; see flowPairSpan.
//
// The value-extent computation is byte-precise only for the value shapes a
// `networks` list can legally take (a flow sequence in a flow root; block/flow
// sequence or mapping, or a null value, in a block root); anything else falls
// back to hashing the raw bytes, so the transform can never remove more than the
// key it targets.
func templateImageContent(rel string, raw []byte) []byte {
	base := rel
	if i := strings.LastIndexByte(rel, '/'); i >= 0 {
		base = rel[i+1:]
	}
	if base != "template.yaml" {
		return nil
	}
	root, ok := parseTopLevelMapping(raw)
	if !ok {
		return nil
	}
	starts := lineStartOffsets(raw)
	// A key that is alone on its line can have its whole line removed, whether the
	// root is a block mapping or a multi-line flow mapping. A key sharing its line
	// with a sibling (a single-line flow root, `{networks: [a], runtime: node24}`)
	// cannot, and even a key alone on its line can have its flow value close on a
	// line a sibling also occupies (`networks: [\n  a,\n], runtime: node24`); both
	// are detected below and spliced pair-wise instead (see flowPairSpan).
	startsPerLine := make(map[int]int, len(root.Content)/2)
	for i := 0; i+1 < len(root.Content); i += 2 {
		startsPerLine[root.Content[i].Line]++
	}
	var spans []byteSpan
	for i := 0; i+1 < len(root.Content); i += 2 {
		key := root.Content[i]
		if !isRuntimeOnlyTemplateKey(key.Value) {
			continue
		}
		if startsPerLine[key.Line] == 1 && key.Line >= 1 && key.Line <= len(starts) {
			start := starts[key.Line-1]
			end, ok := keyValueEnd(raw, starts, root.Content[i+1])
			// The whole-line span is removed blindly, so refuse it if any sibling
			// key's token falls inside — an unforeseen node position can then never
			// delete a sibling.
			if ok && end >= start && !spanContainsOtherKey(raw, starts, root, i, start, end) {
				spans = append(spans, byteSpan{start: start, end: end})
				continue
			}
		}
		// The whole-line span is unsafe or unavailable (the key shares its line):
		// remove only the pair and one adjacent separator. A failure here leaves
		// the key in place and the raw bytes are hashed.
		if span, ok := flowPairSpan(raw, starts, key, root.Content[i+1]); ok {
			spans = append(spans, span)
		}
	}
	if len(spans) == 0 {
		return nil
	}
	return removeSpans(raw, spans)
}

// spanContainsOtherKey reports whether any top-level key node other than index
// skip could start inside the half-open byte span [start, end). A block line span
// is removed wholesale, so a sibling key occupying any of its bytes would be
// deleted too; the caller then falls back to the pair splice. A sibling whose
// offset is unknown is treated as a conflict, so an unplaceable key can never be
// silently dropped.
func spanContainsOtherKey(raw []byte, starts []int, root *yaml.Node, skip, start, end int) bool {
	for i := 0; i+1 < len(root.Content); i += 2 {
		if i == skip {
			continue
		}
		off := nodeOffset(starts, root.Content[i])
		if off < 0 || (off >= start && off < end) {
			return true
		}
	}
	return false
}

// flowPairSpan returns the byte span to remove for a runtime-only key/value pair
// that shares its line with another top-level key, which only happens in a
// flow-style root mapping (a block mapping puts every key on its own line).
// Removing the whole line would drop the siblings, so this removes exactly the
// pair's own bytes — from the key token through the end of its value — plus one
// adjacent separator comma and the surrounding spaces/tabs. Because the value
// and the cosmetic separator bytes are removed whole, the transform is invariant
// to the pair's contents AND to author spacing, so a `networks` edit in a
// flow-style root no longer forces a rebuild.
//
// The value must be measurable: a flow sequence (the shape a legal `networks`
// list takes here, found by matching its closing bracket) or a null (an omitted
// or empty list, whose extent is the null token or, for the empty spelling, the
// next token yaml.v3 points at). Any other value — a block collection cannot
// share its key's line, and a plain scalar such as a number is malformed for
// `networks` — returns false, so the caller hashes the raw bytes and malformed
// input is never mangled.
func flowPairSpan(raw []byte, starts []int, key, val *yaml.Node) (byteSpan, bool) {
	start := nodeOffset(starts, key)
	valStart := nodeOffset(starts, val)
	if start < 0 || valStart < 0 || valStart < start {
		return byteSpan{}, false
	}
	var end int
	switch {
	case val.Kind == yaml.SequenceNode && val.Style&yaml.FlowStyle != 0:
		e, ok := scanFlowClose(raw, valStart)
		if !ok {
			return byteSpan{}, false
		}
		end = e
	case val.Kind == yaml.ScalarNode && val.Tag == "!!null":
		if val.Value == "" {
			// An empty null has no token of its own; yaml.v3 reports its position
			// at the next token, which is exactly where the (empty) value ends.
			end = valStart
		} else {
			// An explicit `null`/`~` token: its literal source length is its
			// extent, so no sibling token can be touched.
			end = valStart + len(val.Value)
		}
	default:
		return byteSpan{}, false
	}
	if end < start {
		return byteSpan{}, false
	}
	// Removing the pair joins its two neighbours, so exactly one adjacent
	// separator comma and the spaces/tabs around it are removed: the trailing
	// comma and the spaces after it when there is one, otherwise the preceding
	// comma and the spaces between it and the key. The leading spaces before the
	// key are deliberately kept in the trailing case because they belong to the
	// preceding separator; consuming them would delete a byte the equivalent
	// no-key template still has. This keeps the join byte-identical to the same
	// document authored without the key. Only spaces and tabs are crossed, never
	// a newline, so a key alone on its line (handled by the block path above)
	// and the neighbours' lines are untouched.
	if post := flowSpacesEnd(raw, end); post < len(raw) && raw[post] == ',' {
		return byteSpan{start: start, end: flowSpacesEnd(raw, post+1)}, true
	}
	pre := flowSpacesStart(raw, start)
	if pre > 0 && raw[pre-1] == ',' {
		return byteSpan{start: pre - 1, end: end}, true
	}
	// No adjacent separator: the pair is the only top-level entry, so it is
	// removed on its own.
	return byteSpan{start: start, end: end}, true
}

// flowSpacesEnd returns the index after any run of spaces and tabs at i.
func flowSpacesEnd(raw []byte, i int) int {
	for i < len(raw) && (raw[i] == ' ' || raw[i] == '\t') {
		i++
	}
	return i
}

// flowSpacesStart returns the index at or before i after moving left over any
// run of spaces and tabs.
func flowSpacesStart(raw []byte, i int) int {
	for i > 0 && (raw[i-1] == ' ' || raw[i-1] == '\t') {
		i--
	}
	return i
}

// byteSpan is a half-open [start, end) byte range to drop from a template.
type byteSpan struct{ start, end int }

// parseTopLevelMapping parses raw as a single YAML document whose root is a
// mapping. It returns false for anything else (unparseable, a scalar, a
// sequence, or a multi-document stream), so the caller falls back to hashing the
// raw bytes rather than guessing.
func parseTopLevelMapping(raw []byte) (*yaml.Node, bool) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, false
	}
	root := &doc
	if root.Kind == yaml.DocumentNode {
		if len(root.Content) != 1 {
			return nil, false
		}
		root = root.Content[0]
	}
	if root.Kind != yaml.MappingNode {
		return nil, false
	}
	return root, true
}

// lineStartOffsets returns the byte offset at which each 1-based line begins.
// The result is indexed by line-1; an offset equal to len(raw) means the line is
// empty and past the final newline.
func lineStartOffsets(raw []byte) []int {
	starts := make([]int, 1, 1+len(raw)/24)
	for i, b := range raw {
		if b == '\n' {
			starts = append(starts, i+1)
		}
	}
	return starts
}

// keyValueEnd returns the offset just past the newline terminating the last line
// of the value belonging to key. It returns false when the extent cannot be
// determined safely, so the caller leaves the key in place and the raw bytes are
// hashed instead.
//
// Only sequence and mapping values (the shapes a `networks` list can legally
// take) and a null value (an omitted/empty list, which parses to no networks)
// are measured: a block collection's extent is the deepest line of its child
// nodes, and a flow collection's extent is found by scanning to its matching
// close bracket (covering multi-line flow). Any other scalar — a valid template
// never has one for `networks` — conservatively returns false, and the raw bytes
// are hashed.
func keyValueEnd(raw []byte, starts []int, val *yaml.Node) (int, bool) {
	if val.Kind == yaml.ScalarNode {
		if val.Tag != "!!null" {
			return 0, false
		}
		return endOfLine(raw, starts, val.Line), true
	}
	if val.Kind != yaml.SequenceNode && val.Kind != yaml.MappingNode {
		return 0, false
	}
	if val.Style&yaml.FlowStyle != 0 {
		start := nodeOffset(starts, val)
		if start < 0 {
			return 0, false
		}
		return scanFlowEnd(raw, start), true
	}
	last := maxLine(val)
	if last < 1 {
		return 0, false
	}
	return endOfLine(raw, starts, last), true
}

// nodeOffset returns the byte offset of a node's first character, or -1 when its
// position is unknown.
func nodeOffset(starts []int, n *yaml.Node) int {
	if n.Line < 1 || n.Line > len(starts) {
		return -1
	}
	off := starts[n.Line-1] + (n.Column - 1)
	if off < 0 {
		return -1
	}
	return off
}

// maxLine reports the greatest line occupied by n or any of its descendants, so
// a block collection's full extent is removed, not just its first line.
func maxLine(n *yaml.Node) int {
	max := n.Line
	for _, c := range n.Content {
		if l := maxLine(c); l > max {
			max = l
		}
	}
	return max
}

// scanFlowEnd finds the byte just past the closing bracket of the flow
// collection beginning at start (a '[' or '{'), then extends to the end of that
// line so the trailing newline is consumed. It tracks the matching bracket pair
// only; nested opposite brackets are skipped byte by byte. Quoted scalars and
// `#` comments are honored so a bracket inside them does not close the
// collection early. An unterminated collection falls back to the end of input.
func scanFlowEnd(raw []byte, start int) int {
	end, ok := scanFlowClose(raw, start)
	if !ok {
		if start < len(raw) && (raw[start] == '[' || raw[start] == '{') {
			// The collection opened but never closed: the doc is malformed and
			// the raw fallback will hash the whole thing, so consume to the end.
			return len(raw)
		}
		return endOfLineAt(raw, start)
	}
	return endOfLineAt(raw, end-1)
}

// scanFlowClose finds the byte just past the closing bracket matching the flow
// collection beginning at start (raw[start] must be a '[' or '{'). It returns
// false when start is not an opening bracket, start is past the input, or the
// collection is unterminated — callers treat that as "cannot measure safely".
func scanFlowClose(raw []byte, start int) (int, bool) {
	if start >= len(raw) {
		return 0, false
	}
	var open, close byte
	switch raw[start] {
	case '[':
		open, close = '[', ']'
	case '{':
		open, close = '{', '}'
	default:
		return 0, false
	}
	depth := 0
	for i := start; i < len(raw); {
		switch c := raw[i]; c {
		case '"', '\'':
			i = skipQuoted(raw, i)
		case '#':
			// A `#` starts a comment only at the start of a line or after
			// whitespace; a `#` inside a plain scalar (e.g. `a#b`) is literal
			// and must not swallow a closing bracket on the same line.
			if i == 0 || raw[i-1] == ' ' || raw[i-1] == '\t' || raw[i-1] == '\n' {
				for i < len(raw) && raw[i] != '\n' {
					i++
				}
				continue
			}
			i++
		case open:
			depth++
			i++
		case close:
			depth--
			i++
			if depth == 0 {
				return i, true
			}
		default:
			i++
		}
	}
	return 0, false
}

// skipQuoted returns the offset just past the quoted scalar beginning at start
// (raw[start] is the quote). Double-quoted scalars honor backslash escapes;
// single-quoted scalars honor the doubled-quote escape.
func skipQuoted(raw []byte, start int) int {
	quote := raw[start]
	for i := start + 1; i < len(raw); {
		if quote == '"' && raw[i] == '\\' {
			i += 2
			continue
		}
		if raw[i] == quote {
			if quote == '\'' && i+1 < len(raw) && raw[i+1] == '\'' {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return len(raw)
}

// removeSpans returns raw with the given half-open byte ranges removed. Spans
// may be given in any order; overlapping or out-of-range spans are clamped so
// the result is always a valid slice of raw.
func removeSpans(raw []byte, spans []byteSpan) []byte {
	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })
	out := make([]byte, 0, len(raw))
	prev := 0
	for _, s := range spans {
		if s.start < prev {
			s.start = prev
		}
		if s.end < s.start {
			s.end = s.start
		}
		if s.start > len(raw) {
			s.start = len(raw)
		}
		if s.end > len(raw) {
			s.end = len(raw)
		}
		out = append(out, raw[prev:s.start]...)
		prev = s.end
	}
	return append(out, raw[prev:]...)
}

// endOfLine returns the offset just past the newline that terminates the 1-based
// line, or len(raw) when the line is the last (unterminated) one.
func endOfLine(raw []byte, starts []int, line int) int {
	if line < 1 || line > len(starts) {
		return len(raw)
	}
	return endOfLineAt(raw, starts[line-1])
}

// endOfLineAt returns the offset just past the first newline at or after from,
// or len(raw) when there is none.
func endOfLineAt(raw []byte, from int) int {
	if from < 0 {
		from = 0
	}
	for i := from; i < len(raw); i++ {
		if raw[i] == '\n' {
			return i + 1
		}
	}
	return len(raw)
}

// isRuntimeOnlyTemplateKey reports whether name is a top-level template key that
// configures how Relay runs a function but never what goes into its image.
func isRuntimeOnlyTemplateKey(name string) bool {
	for _, key := range runtimeOnlyTemplateKeys {
		if key == name {
			return true
		}
	}
	return false
}
