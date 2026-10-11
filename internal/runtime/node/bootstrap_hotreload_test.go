package node

import (
	"strings"
	"testing"
)

// TestNodeRuntimeSourceMountRunsEditedSourceOnFreshProcess is the non-Docker
// "latest source executes" proof for HOT_RELOAD: a handler resolved from the
// mounted source root runs its code, and after the live mounted module is edited
// a FRESH bootstrap process — mirroring the warm-container recycling a
// source-only edit triggers, since the image itself is reused — runs the edited
// code. The image-reuse half is pinned by the runtime-package source-mount tests.
func TestNodeRuntimeSourceMountRunsEditedSourceOnFreshProcess(t *testing.T) {
	skipIfNoNode(t)
	root := appRoot(t)
	writeMountedModule(t, root, "index.mjs", `
export function handler(event) {
  console.log("src V1 " + event.msg);
}
`)
	p1 := startNodeMounted(t, root)
	r1 := p1.invoke(t, "index.handler", `{"msg":"one"}`, nil)
	if !r1.OK {
		t.Fatalf("v1 mounted invoke failed: %s", r1.Err)
	}
	if !strings.Contains(r1.Out, "src V1 one") {
		t.Fatalf("v1 out = %q, want it to contain 'src V1 one'", r1.Out)
	}

	// Edit the LIVE mounted source. The image is unchanged, so the reload takes
	// effect by recycling the warm process: a fresh bootstrap reads the mount.
	writeMountedModule(t, root, "index.mjs", `
export function handler(event) {
  console.log("src V2 " + event.msg);
}
`)
	p2 := startNodeMounted(t, root)
	r2 := p2.invoke(t, "index.handler", `{"msg":"two"}`, nil)
	if !r2.OK {
		t.Fatalf("v2 mounted invoke failed: %s", r2.Err)
	}
	if !strings.Contains(r2.Out, "src V2 two") {
		t.Fatalf("v2 out = %q, want it to contain 'src V2 two' (edited live source)", r2.Out)
	}
}
