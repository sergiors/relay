package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// runtimeVersionConfig combines the resource-config fingerprint with a managed
// app process-config fingerprint into the config half of a warm-pool generation
// key. An empty managed fingerprint preserves the historical resources-only key
// exactly, so runtimes without a managed provider (Node, and every hand-built
// Prepared) keep their existing generation identity.
func runtimeVersionConfig(resources, managed string) string {
	if managed == "" {
		return resources
	}
	h := sha256.New()
	_, _ = h.Write([]byte("relay-runtime-version-config-v1"))
	writeConfigPart(h, resources)
	writeConfigPart(h, managed)
	return hex.EncodeToString(h.Sum(nil))
}

// writeConfigPart writes one length-prefixed field so a value containing a
// separator cannot shift the field boundary into a colliding pair.
func writeConfigPart(h interface{ Write([]byte) (int, error) }, part string) {
	_, _ = h.Write([]byte(strconv.Itoa(len(part))))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(part))
}
