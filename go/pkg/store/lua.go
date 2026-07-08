package store

// Lua scripts mirror lib/kafka_batch/stores/redis_store.rb (wire-compatible).

const batchDoneJobLua = `
local seq = tonumber(ARGV[1])
if not seq or seq < 1 then return {0, 'invalid'} end

local bit = seq - 1
if redis.call('GETBIT', KEYS[2], bit) == 1 then return {0, 'duplicate'} end
redis.call('SETBIT', KEYS[2], bit, 1)
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))

if redis.call('EXISTS', KEYS[1]) == 0 then return {0, 'not_found'} end

local status = redis.call('HGET', KEYS[1], 'status')
if status == 'success' or status == 'complete' or status == 'cancelled' then
  return {0, 'duplicate'}
end

redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
redis.call('HINCRBY', KEYS[1], ARGV[2], 1)

local total     = tonumber(redis.call('HGET', KEYS[1], 'total_jobs'))      or 0
local completed = tonumber(redis.call('HGET', KEYS[1], 'completed_count')) or 0
local failed    = tonumber(redis.call('HGET', KEYS[1], 'failed_count'))    or 0
local sealed    = redis.call('HGET', KEYS[1], 'locked_at')

if (completed + failed) >= total and sealed and sealed ~= '' then
  local outcome = (failed > 0) and 'complete' or 'success'
  redis.call('HSET', KEYS[1], 'status',      outcome)
  redis.call('HSET', KEYS[1], 'finished_at', ARGV[4])
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
  local batch_id = redis.call('HGET', KEYS[1], 'id')
  if batch_id then
    redis.call('ZREM', KEYS[3], batch_id)
    redis.call('ZADD', KEYS[4], tonumber(ARGV[5]), batch_id)
  end
  redis.call('HINCRBY', KEYS[5], 'running', -1)
  redis.call('HINCRBY', KEYS[5], outcome, 1)
  return {1, outcome}
end

return {2, 'continue'}
`

const claimCallbackLua = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local won = redis.call('HSETNX', KEYS[1], 'callback_dispatched_at', ARGV[1])
if won == 1 then
  if ARGV[2] ~= '' then
    redis.call('HSET', KEYS[1], 'callback_dispatched_by', ARGV[2])
  end
  redis.call('ZREM', KEYS[2], ARGV[3])
end
return won
`

const keyPrefix = "kafka_batch:b"
const runningIndex = "kafka_batch:index:running"
const doneIndex = "kafka_batch:index:done"
const countsKey = "kafka_batch:counts"
const cancelledIndex = "kafka_batch:index:cancelled"

func batchKey(id string) string   { return keyPrefix + ":" + id }
func bitmapKey(id string) string  { return keyPrefix + ":bitmap:" + id }
