package stream

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type groupCreator interface {
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
}

const ensureGroupScript = `
local streamKind = redis.call('TYPE', KEYS[1])['ok']
if streamKind ~= 'none' and streamKind ~= 'stream' then
  return redis.error_reply('consumer group creation: stream key is not a stream')
end

local groupExists = false
if streamKind == 'stream' then
  local groups = redis.pcall('XINFO', 'GROUPS', KEYS[1])
  if type(groups) ~= 'table' then
    return redis.error_reply('consumer group creation: group lookup failed')
  end
  if groups.err then
    return redis.error_reply(groups.err)
  end
  for _, group in ipairs(groups) do
    for i = 1, #group, 2 do
      if group[i] == 'name' and group[i + 1] == ARGV[1] then
        groupExists = true
        break
      end
    end
    if groupExists then break end
  end
end

if groupExists then
  local token = redis.call('GET', KEYS[2])
  if not token then
    redis.call('SET', KEYS[2], ARGV[2], 'NX')
    token = redis.call('GET', KEYS[2])
  end
  return 'existing:' .. token
end

-- Rotate before creating the group. Lua scripts serialize commands but do not
-- roll back earlier writes when a later command returns a runtime error.
local previousToken = redis.call('GET', KEYS[2])
redis.call('SET', KEYS[2], ARGV[2])
local result = redis.pcall('XGROUP', 'CREATE', KEYS[1], ARGV[1], '0', 'MKSTREAM')
if type(result) == 'table' and result.err then
  if string.find(result.err, 'BUSYGROUP', 1, true) then
    if previousToken then redis.call('SET', KEYS[2], previousToken) end
    local token = redis.call('GET', KEYS[2])
    return 'existing:' .. token
  end
  return redis.error_reply(result.err)
end

return 'created:' .. ARGV[2]
`

const repairGroupGenerationScript = `
local groups = redis.pcall('XINFO', 'GROUPS', KEYS[1])
if type(groups) ~= 'table' then
  return redis.error_reply('group generation: group lookup failed')
end
if groups.err then
  if string.find(groups.err, 'no such key', 1, true) then
    return redis.error_reply('NOGROUP no such stream or consumer group')
  end
  return redis.error_reply(groups.err)
end

local found = false
for _, group in ipairs(groups) do
  for i = 1, #group, 2 do
    if group[i] == 'name' and group[i + 1] == ARGV[1] then
      found = true
      break
    end
  end
  if found then break end
end
if not found then
  return redis.error_reply('NOGROUP no such stream or consumer group')
end

local token = redis.call('GET', KEYS[2])
if not token then
  redis.call('SET', KEYS[2], ARGV[2], 'NX')
  token = redis.call('GET', KEYS[2])
end
return token
`

func groupGenerationKey(stream, group string) string {
	return "relay:stream-group-generation:" + encodeComponent(stream) + ":" + encodeComponent(group)
}

func ensureGroup(
	ctx context.Context,
	client groupCreator,
	stream,
	group string,
) (GroupCreation, string, error) {
	token, err := newOpaqueToken("consumer-group generation")
	if err != nil {
		return GroupCreated, "", err
	}
	result, err := client.Eval(ctx, ensureGroupScript, []string{
		stream,
		groupGenerationKey(stream, group),
	}, group, token).Text()
	if err != nil {
		return GroupCreated, "", fmt.Errorf(
			"create consumer group %q on stream %q: %w", group, stream, err)
	}
	created := strings.HasPrefix(result, "created:")
	existing := strings.HasPrefix(result, "existing:")
	if !created && !existing {
		return GroupCreated, "", fmt.Errorf("create consumer group %q on stream %q: malformed generation response", group, stream)
	}
	generation := strings.TrimPrefix(strings.TrimPrefix(result, "created:"), "existing:")
	if generation == "" {
		return GroupCreated, "", fmt.Errorf("create consumer group %q on stream %q: missing generation token", group, stream)
	}
	if created {
		return GroupCreated, generation, nil
	}
	return GroupAlreadyExists, generation, nil
}

func readGroupGeneration(ctx context.Context, client *redis.Client, stream, group string) (string, error) {
	key := groupGenerationKey(stream, group)
	token, err := client.Get(ctx, key).Result()
	if err == nil {
		if token != "" {
			return token, nil
		}
		return "", fmt.Errorf("read consumer-group generation for %q on stream %q: empty token", group, stream)
	}
	if err != redis.Nil {
		return "", fmt.Errorf("read consumer-group generation for %q on stream %q: %w", group, stream, err)
	}

	candidate, err := newOpaqueToken("consumer-group generation")
	if err != nil {
		return "", err
	}
	token, err = client.Eval(ctx, repairGroupGenerationScript, []string{stream, key}, group, candidate).Text()
	if err != nil {
		return "", fmt.Errorf("repair consumer-group generation for %q on stream %q: %w", group, stream, err)
	}
	if token == "" {
		return "", fmt.Errorf("repair consumer-group generation for %q on stream %q: empty token", group, stream)
	}
	return token, nil
}
