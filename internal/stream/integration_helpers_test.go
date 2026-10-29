//go:build integration

package stream

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

// auxRedisClient opens an independent Redis client for a test that closes one
// client to force a transport failure while another continues to inspect state.
func auxRedisClient(t *testing.T, ref *redis.Client) *redis.Client {
	t.Helper()
	opts := *ref.Options()
	cli := redis.NewClient(&opts)
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}
