package worker

import (
	"context"
	"log/slog"
	"time"

	"relay/internal/dlq"
	"relay/internal/stream"
)

// dlqSettlementInterval is the fixed cadence of the bounded ACK/settlement
// reconciliation. It is independent of DLQ_RETENTION: even with retention
// disabled, a crash between a successful Redis ACK and the PostgreSQL settlement
// update must eventually be repaired.
const dlqSettlementInterval = time.Minute

// dlqSettlementBatch bounds one reconciliation pass. Large unsettled backlogs
// drain across ticks rather than in one unbounded scan.
const dlqSettlementBatch = 100

// dlqSettlementStore is the PostgreSQL side of reconciliation: list a bounded
// page of unsettled source deliveries after a cursor, and mark a confirmed
// delivery settled.
type dlqSettlementStore interface {
	ListUnsettledDLQDeliveries(ctx context.Context, afterDeliveryID string, limit int) ([]dlq.DeliveryRef, error)
	MarkDLQDeliverySettled(ctx context.Context, deliveryID string, settledAt time.Time) error
}

// deliveryInspector is the broker side of reconciliation. The stream package's
// DeliveryInspector is the production implementation.
type deliveryInspector interface {
	Inspect(ctx context.Context, source, group, messageID string) (stream.DeliveryState, error)
}

// dlqSettlementTick performs one bounded reconciliation page starting strictly
// after afterDeliveryID, then returns the cursor for the next tick. It asks the
// broker for each unsettled source delivery's exact disposition and marks a row
// settled ONLY on stream.DeliverySettled (an extant body is past the group's
// last-delivered ID, or an ACK receipt proves a bodyless delivery was acked).
//
// Pending, unread, and unknown dispositions preserve the row. "Not pending" is
// deliberately insufficient: an unread entry will be delivered again after a
// group recreation, and an unknown disposition (absent body with no ACK receipt)
// is missing data, not settlement. If receipt retention expires before
// reconciliation, the row stays unsettled rather than inferring an ACK.
//
// There is an unavoidable ordering assumption between Redis and PostgreSQL: the
// observation and the settlement mark are not atomic across the two systems. A
// group recreation immediately after a settled observation can make the source
// entry unread again, so a row may have been settled just before a legitimate
// replay. That replay is at-least-once and re-inserts the same idempotent DLQ
// row; settlement only governs admin removal and retention, never matching or
// delivery.
//
// The cursor advances past every returned row, including pending and malformed
// ones, so 100 permanently pending deliveries cannot starve later rows. When the
// page is short, exhausted, or empty, it wraps to "" for the next tick.
//
// Any broker error preserves the rows: an unavailable stream/group is never
// treated as proof that a delivery is settled. A settlement write failure is
// logged and retried on the next tick (or by the next stream ACK settlement).
func dlqSettlementTick(
	ctx context.Context,
	store dlqSettlementStore,
	inspector deliveryInspector,
	afterDeliveryID string,
	now func() time.Time,
	logger *slog.Logger,
) string {
	refs, err := store.ListUnsettledDLQDeliveries(ctx, afterDeliveryID, dlqSettlementBatch)
	if err != nil {
		logger.Warn("DLQ settlement: list unsettled deliveries failed; will retry next tick", "error", err)
		return afterDeliveryID
	}
	if len(refs) == 0 {
		return ""
	}
	for _, ref := range refs {
		if ref.DeliveryID == "" || ref.Source == "" || ref.SourceGroup == "" || ref.SourceMessageID == "" {
			logger.Warn("DLQ settlement: incomplete stored delivery identity; preserving",
				"delivery_id", ref.DeliveryID)
			continue
		}
		state, err := inspector.Inspect(ctx, ref.Source, ref.SourceGroup, ref.SourceMessageID)
		if err != nil {
			logger.Warn("DLQ settlement: source delivery check failed; preserving unsettled rows",
				"delivery_id", ref.DeliveryID,
				"error", err,
			)
			continue
		}
		if state != stream.DeliverySettled {
			logger.Debug("DLQ settlement: source delivery is not knowingly settled; preserving",
				"delivery_id", ref.DeliveryID,
				"state", deliveryStateName(state),
			)
			continue
		}
		if err := store.MarkDLQDeliverySettled(ctx, ref.DeliveryID, now()); err != nil {
			logger.Warn("DLQ settlement: mark failed; will retry next tick",
				"delivery_id", ref.DeliveryID,
				"error", err,
			)
			continue
		}
		logger.Debug("DLQ settlement: source delivery settled", "delivery_id", ref.DeliveryID)
	}
	if len(refs) < dlqSettlementBatch {
		return ""
	}
	return refs[len(refs)-1].DeliveryID
}

// deliveryStateName renders a broker disposition as a fixed, low-cardinality
// label for logs.
func deliveryStateName(state stream.DeliveryState) string {
	switch state {
	case stream.DeliveryPending:
		return "pending"
	case stream.DeliveryUnread:
		return "unread"
	case stream.DeliverySettled:
		return "settled"
	default:
		return "unknown"
	}
}

// dlqSettlementLoop runs the bounded reconciliation until ctx is cancelled. It
// performs one initial pass so an already-crashed settlement is repaired
// promptly, then one pass per fixed interval. It owns no Redis or PostgreSQL
// lifecycle itself; the worker's shutdown barrier joins it before either closes.
func dlqSettlementLoop(
	ctx context.Context,
	store dlqSettlementStore,
	inspector deliveryInspector,
	logger *slog.Logger,
) {
	logger.Info("DLQ settlement: starting", "interval", dlqSettlementInterval, "batch", dlqSettlementBatch)

	cursor := ""
	initCtx, initCancel := context.WithTimeout(ctx, reconcileTimeout)
	cursor = dlqSettlementTick(initCtx, store, inspector, cursor, time.Now, logger)
	initCancel()

	ticker := time.NewTicker(dlqSettlementInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tickCtx, tickCancel := context.WithTimeout(ctx, reconcileTimeout)
			cursor = dlqSettlementTick(tickCtx, store, inspector, cursor, time.Now, logger)
			tickCancel()
		}
	}
}
