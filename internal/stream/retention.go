package stream

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// ErrRetentionGroupAbsent reports that a retention trim was skipped because the
// Relay consumer group is not present on the stream (or the stream itself does
// not exist yet). It is not a trim failure: retention must never repair or
// recreate the group. The caller logs the transition and tries again on a later
// tick.
var ErrRetentionGroupAbsent = errors.New("stream: relay consumer group is absent")

// AckedTrimmer is the production retention trimmer. One method call runs a
// single Lua script that verifies the Relay consumer group exists and then
// issues XTRIM ... MINID ~ ... ACKED, so the group-existence check and the trim
// are atomic: retention can never trim a stream whose Relay group was removed
// between a separate check and the trim.
//
// It deliberately does NOT create or recover a group. A missing group (or
// missing stream) skips the trim with ErrRetentionGroupAbsent; a group that is
// recreated later resumes trimming on a subsequent tick.
type AckedTrimmer struct {
	client *redis.Client
	group  string
}

// NewAckedTrimmer builds the retention trimmer for one source stream's Relay
// consumer group. The group is bound here so the worker's retention loop needs
// no knowledge of the command sequence.
func NewAckedTrimmer(client *redis.Client, group string) *AckedTrimmer {
	return &AckedTrimmer{client: client, group: group}
}

// trimAckedIfGroupScript atomically checks for the named group and performs the
// approximate MINID trim in the requested mode.
//
// KEYS[1] = stream; ARGV[1] = group, ARGV[2] = cutoff ID, ARGV[3] = limit (0
// omits LIMIT), ARGV[4] = trim mode (must be non-empty; ACKED in production).
// Returns {1, removed} when trimmed, or {0, 0} when the stream or group is
// absent. Any other Redis error is returned as an error reply.
var trimAckedIfGroupScript = redis.NewScript(`
local kt = redis.call('TYPE', KEYS[1])
local kind = kt['ok']
if kind == 'none' then
  return {0, 0}
end
if kind ~= 'stream' then
  return redis.error_reply('retention: source key is not a stream')
end

local groups = redis.pcall('XINFO', 'GROUPS', KEYS[1])
if type(groups) ~= 'table' then
  return redis.error_reply('retention: malformed group reply')
end
if groups.err then
  local msg = tostring(groups.err)
  if string.find(msg, 'no such key', 1, true) ~= nil then
    return {0, 0}
  end
  return redis.error_reply('retention: group lookup failed')
end

local found = false
for _, g in ipairs(groups) do
  if type(g) ~= 'table' then
    return redis.error_reply('retention: malformed group reply')
  end
  for i = 1, #g, 2 do
    if g[i] == 'name' and g[i+1] == ARGV[1] then
      found = true
      break
    end
  end
  if found then break end
end
if not found then
  return {0, 0}
end

if ARGV[4] == '' then
  return redis.error_reply('retention: trim mode is required')
end

local limit = tonumber(ARGV[3]) or 0
if limit > 0 then
  return {1, redis.call('XTRIM', KEYS[1], 'MINID', '~', ARGV[2], 'LIMIT', limit, ARGV[4])}
end
return {1, redis.call('XTRIM', KEYS[1], 'MINID', '~', ARGV[2], ARGV[4])}
`)

// XTrimMinIDApproxMode satisfies the worker retention loop's trimmer seam. It
// returns ErrRetentionGroupAbsent (as the command error) when the stream has no
// Relay group, and never issues a trim in that case.
func (t *AckedTrimmer) XTrimMinIDApproxMode(
	ctx context.Context,
	key,
	minID string,
	limit int64,
	mode string,
) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	if t == nil || t.client == nil {
		cmd.SetErr(errors.New("retention: Redis client is not configured"))
		return cmd
	}
	if t.group == "" {
		cmd.SetErr(errors.New("retention: consumer group is not configured"))
		return cmd
	}
	if minID == "" {
		cmd.SetErr(errors.New("retention: cutoff ID is not configured"))
		return cmd
	}
	res, err := trimAckedIfGroupScript.Run(ctx, t.client, []string{key},
		t.group, minID, limit, mode).Slice()
	if err != nil {
		cmd.SetErr(fmt.Errorf("retention trim: %w", err))
		return cmd
	}
	if len(res) < 2 {
		cmd.SetErr(fmt.Errorf(
			"retention trim: unexpected script reply of length %d", len(res)))
		return cmd
	}
	if toInt64(res[0]) == 0 {
		cmd.SetErr(ErrRetentionGroupAbsent)
		return cmd
	}
	cmd.SetVal(toInt64(res[1]))
	return cmd
}
