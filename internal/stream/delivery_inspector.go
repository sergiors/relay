package stream

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// DeliveryInspector answers the only broker question settlement reconciliation
// needs: is one logical source delivery still recoverable from its consumer
// group's PEL? It is a read-only Redis adapter owned by the stream layer; the
// PostgreSQL DLQ store never performs broker I/O.
type DeliveryInspector struct {
	client *redis.Client
}

// NewDeliveryInspector builds the reconciliation adapter for client. A nil
// client makes every query fail, so reconciliation preserves unsettled rows.
func NewDeliveryInspector(client *redis.Client) *DeliveryInspector {
	return &DeliveryInspector{client: client}
}

// Pending reports whether messageID is still pending for the exact source stream
// and group. It is a read-only XPENDING lookup: the message remains in the PEL
// (including under another consumer, or after its body was deleted) and is
// therefore still recoverable.
//
// Any Redis error — including NOGROUP or an unavailable stream — is returned so
// the caller leaves the DLQ rows unsettled. Ambiguity must never settle a row
// whose delivery might still be recovered.
func (i *DeliveryInspector) Pending(ctx context.Context, source, group, messageID string) (bool, error) {
	if i == nil || i.client == nil {
		return false, fmt.Errorf("delivery inspector: Redis client is not configured")
	}
	if source == "" || group == "" || messageID == "" {
		return false, fmt.Errorf("delivery inspector: incomplete source delivery identity")
	}
	entries, err := i.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: source,
		Group:  group,
		Start:  messageID,
		End:    messageID,
		Count:  1,
	}).Result()
	if err != nil {
		return false, fmt.Errorf("check pending source delivery %s/%s/%s: %w",
			source, group, messageID, err)
	}
	return len(entries) > 0, nil
}
