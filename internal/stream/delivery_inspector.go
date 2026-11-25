package stream

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// DeliveryState is the broker-side disposition of one logical source delivery.
// It is the only information DLQ settlement reconciliation needs, and it is
// deliberately richer than a boolean: "not pending" is not proof of
// settlement.
type DeliveryState uint8

const (
	// DeliveryUnknown means the body is gone and no ACK receipt proves
	// settlement. Missing data is not settlement, so the caller must preserve
	// anything that depends on it. It is the zero value so an uninitialized
	// result is conservative.
	DeliveryUnknown DeliveryState = iota
	// DeliveryPending means the message is still in the exact source group's
	// PEL (under any consumer, including a dangling reference whose body was
	// deleted). It remains recoverable and must be preserved.
	DeliveryPending
	// DeliveryUnread means the entry body exists but the group has not reached
	// it yet (its last-delivered ID is before the message). It will be
	// delivered again, so it is not settled.
	DeliveryUnread
	// DeliverySettled means the group's last-delivered ID proves it passed a
	// body that still exists, or an ACK receipt proves a now-bodyless delivery
	// was acknowledged.
	DeliverySettled
)

// deliveryInspectScript answers, atomically, where one message ID stands in one
// consumer group. It verifies the stream exists and is a stream, finds the
// exact group and its last-delivered ID, checks the exact PEL entry and source
// body, and reads the per-delivery ACK receipt when the body is gone. IDs are
// compared component-wise by digit length then lexicographically (canonical
// decimals), never as whole strings or lossy numbers.
//
// Returned status: 1 pending, 2 unread, 3 settled, 4 unknown. A confirmed
// missing stream/group is a NOGROUP error reply (the caller preserves rows);
// WRONGTYPE, ACL, protocol, and other Redis errors stay generic errors and are
// never reported as settled.
//
// KEYS[1] = source stream; KEYS[2] = invocation-state hash;
// ARGV[1] = group, ARGV[2] = message ID, ARGV[3] = ACK receipt field.
var deliveryInspectScript = redis.NewScript(`
local function parse_id(s)
  if type(s) ~= 'string' then return nil end
  local ms, seq = string.match(s, '^(%d+)%-(%d+)$')
  if not ms then return nil end
  return ms, seq
end

local function cmp_uint(a, b)
  if #a ~= #b then
    if #a < #b then return -1 end
    return 1
  end
  if a == b then return 0 end
  if a < b then return -1 end
  return 1
end

local function cmp_id(a, b)
  local am, as = parse_id(a)
  local bm, bs = parse_id(b)
  if not am or not bm then return nil end
  local c = cmp_uint(am, bm)
  if c ~= 0 then return c end
  return cmp_uint(as, bs)
end

local function contains(s, needle)
  if type(s) ~= 'string' then return false end
  return string.find(s, needle, 1, true) ~= nil
end

local NOGROUP_ERR = 'NOGROUP no such stream or consumer group'

local streamType = redis.call('TYPE', KEYS[1])
local streamKind = streamType['ok']
if streamKind == 'none' then
  return redis.error_reply(NOGROUP_ERR)
end
if streamKind ~= 'stream' then
  return redis.error_reply('deliveryInspect: source key is not a stream')
end

local groups = redis.pcall('XINFO', 'GROUPS', KEYS[1])
if type(groups) ~= 'table' then
  return redis.error_reply('deliveryInspect: malformed group reply')
end
if groups.err then
  if contains(groups.err, 'NOGROUP') or contains(groups.err, 'no such key') then
    return redis.error_reply(NOGROUP_ERR)
  end
  return redis.error_reply('deliveryInspect: group lookup failed')
end

local lastDelivered = nil
for _, g in ipairs(groups) do
  if type(g) ~= 'table' then
    return redis.error_reply('deliveryInspect: malformed group reply')
  end
  local name = nil
  local ldid = nil
  for i = 1, #g, 2 do
    if g[i] == 'name' then name = g[i+1] end
    if g[i] == 'last-delivered-id' then ldid = g[i+1] end
  end
  if name == ARGV[1] then
    lastDelivered = ldid
    break
  end
end
if lastDelivered == nil then
  return redis.error_reply(NOGROUP_ERR)
end
if type(lastDelivered) ~= 'string' then
  return redis.error_reply('deliveryInspect: malformed group reply')
end

local pending = redis.pcall('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if type(pending) ~= 'table' then
  return redis.error_reply('deliveryInspect: pending lookup failed')
end
if pending.err then
  if contains(pending.err, 'NOGROUP') or contains(pending.err, 'no such key') then
    return redis.error_reply(NOGROUP_ERR)
  end
  return redis.error_reply('deliveryInspect: pending lookup failed')
end
if #pending > 0 then
  local entry = pending[1]
  if type(entry) ~= 'table' or type(entry[1]) ~= 'string' then
    return redis.error_reply('deliveryInspect: malformed pending reply')
  end
  if entry[1] ~= ARGV[2] then
    return redis.error_reply('deliveryInspect: malformed pending reply')
  end
  return 1
end

local cmp = cmp_id(lastDelivered, ARGV[2])
if cmp == nil then
  return redis.error_reply('deliveryInspect: unparseable stream id')
end

local entries = redis.pcall('XRANGE', KEYS[1], ARGV[2], ARGV[2])
if type(entries) ~= 'table' then
  return redis.error_reply('deliveryInspect: source lookup failed')
end
if entries.err then
  if contains(entries.err, 'NOGROUP') or contains(entries.err, 'no such key') then
    return redis.error_reply(NOGROUP_ERR)
  end
  return redis.error_reply('deliveryInspect: source lookup failed')
end

if #entries > 0 then
  if cmp < 0 then
    -- The group has not reached it yet: it will be delivered again.
    return 2
  end
  return 3
end

-- A missing body and empty PEL are ambiguous: XAUTOCLAIM may have purged a
-- dangling PEL entry, or retention may have trimmed it before delivery. Only
-- the ACK receipt distinguishes confirmed settlement from missing data.
local stateType = redis.call('TYPE', KEYS[2])['ok']
if stateType ~= 'none' and stateType ~= 'hash' then
  return redis.error_reply('deliveryInspect: invocation-state key is not a hash')
end
if stateType == 'hash' and redis.call('HGET', KEYS[2], ARGV[3]) == '1' then
  return 3
end
return 4
`)

// DeliveryInspector answers the broker question settlement reconciliation
// needs: is one logical source delivery still recoverable from its consumer
// group, already settled, not yet delivered, or unknown? It is a read-only
// Redis adapter owned by the stream layer; the PostgreSQL DLQ store never
// performs broker I/O.
type DeliveryInspector struct {
	client *redis.Client
}

// NewDeliveryInspector builds the reconciliation adapter for client. A nil
// client makes every query fail, so reconciliation preserves unsettled rows.
func NewDeliveryInspector(client *redis.Client) *DeliveryInspector {
	return &DeliveryInspector{client: client}
}

// Inspect reports the exact disposition of messageID for the source stream and
// group. It performs one atomic read-only script, so the stream/group
// existence, the PEL entry, the source entry, and the group's last-delivered
// ID are observed consistently.
//
// Any error — a confirmed NOGROUP, an unavailable stream, a wrong-type key, or
// another Redis failure — is returned so the caller preserves anything that
// depends on the answer. Ambiguity is never reported as DeliverySettled.
func (i *DeliveryInspector) Inspect(
	ctx context.Context,
	source,
	group,
	messageID string,
) (DeliveryState, error) {
	if i == nil || i.client == nil {
		return DeliveryUnknown, fmt.Errorf("delivery inspector: Redis client is not configured")
	}
	if source == "" || group == "" || messageID == "" {
		return DeliveryUnknown, fmt.Errorf("delivery inspector: incomplete source delivery identity")
	}
	status, err := deliveryInspectScript.Run(ctx, i.client,
		[]string{source, invocationStateKey(source, group, messageID)},
		group, messageID, ackReceiptField).Int64()
	if err != nil {
		return DeliveryUnknown, fmt.Errorf("inspect source delivery %s/%s/%s: %w",
			source, group, messageID, err)
	}
	switch status {
	case 1:
		return DeliveryPending, nil
	case 2:
		return DeliveryUnread, nil
	case 3:
		return DeliverySettled, nil
	case 4:
		return DeliveryUnknown, nil
	default:
		return DeliveryUnknown, fmt.Errorf(
			"delivery inspector: unexpected status %d for %s/%s/%s",
			status, source, group, messageID)
	}
}

// Pending reports whether messageID is still in the exact source group's PEL.
// It is the boolean view used by callers that genuinely only need PEL
// membership (not settlement); reconciliation must use Inspect, because "not
// pending" can mean unread or unknown and only DeliverySettled is safe to
// settle.
//
// Any Redis error is returned so the caller preserves its rows.
func (i *DeliveryInspector) Pending(
	ctx context.Context,
	source,
	group,
	messageID string,
) (bool, error) {
	state, err := i.Inspect(ctx, source, group, messageID)
	if err != nil {
		return false, err
	}
	return state == DeliveryPending, nil
}
