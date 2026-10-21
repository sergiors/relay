//go:build linux

package git

import "golang.org/x/sys/unix"

// platformExchange atomically swaps the two directory entries at a and b. Both
// paths must exist and reside on the same filesystem. Linux renameat2 with
// RENAME_EXCHANGE is the primitive that lets materialize replace a live app
// without ever unlinking its name: after the call the old tree is at a and the
// new tree is at b.
func platformExchange(a, b string) error {
	return unix.Renameat2(unix.AT_FDCWD, a, unix.AT_FDCWD, b, unix.RENAME_EXCHANGE)
}
