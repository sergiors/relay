package testutil

import (
	"bytes"
	"sync"
)

// SyncBuffer is a mutex-protected buffer for log output written by background
// goroutines while the test goroutine asserts on it concurrently. A plain
// bytes.Buffer would itself race (which -race would flag), so a logger fed by
// concurrent goroutines must write through this.
type SyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write implements io.Writer, serializing concurrent logger writes.
func (b *SyncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// String returns the buffered output so far, safe to call concurrently with
// Write.
func (b *SyncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Bytes returns a copy of the buffered output so far, safe to call concurrently
// with Write.
func (b *SyncBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

// Reset discards the buffered output, safe to call concurrently with Write.
func (b *SyncBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}
