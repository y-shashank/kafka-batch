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

const createBatchLua = `
local created = redis.call('HSETNX', KEYS[1], 'id', ARGV[1])
if created == 0 then return 0 end
redis.call('HMSET', KEYS[1],
  'total_jobs',      ARGV[2],
  'completed_count', '0',
  'failed_count',    '0',
  'status',          'running',
  'on_success',      ARGV[3],
  'on_complete',     ARGV[4],
  'meta',            ARGV[5],
  'created_at',      ARGV[6],
  'locked_at',       ARGV[8],
  'description',     ARGV[9],
  'tenant_id',       ARGV[10]
)
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[7]))
return 1
`

const addJobsLua = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local status = redis.call('HGET', KEYS[1], 'status')
if status == 'cancelled' then return 2 end
if status == 'success' or status == 'complete' then return 3 end
local dispatched = redis.call('HGET', KEYS[1], 'callback_dispatched_at')
if dispatched and dispatched ~= '' then return 3 end
local n = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
redis.call('HINCRBY', KEYS[1], 'total_jobs', n)
redis.call('EXPIRE', KEYS[1], ttl)
if n > 0 then
  local seq_end = redis.call('INCRBY', KEYS[2], n)
  redis.call('EXPIRE', KEYS[2], ttl)
  return {1, seq_end - n + 1, seq_end}
end
return {1}
`

const sealBatchLua = `
if redis.call('EXISTS', KEYS[1]) == 0 then return {0, 'not_found'} end
local status = redis.call('HGET', KEYS[1], 'status')
local sealed = redis.call('HGET', KEYS[1], 'locked_at')
if not sealed or sealed == '' then
  redis.call('HSET', KEYS[1], 'locked_at', ARGV[1])
end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))

local total = tonumber(redis.call('HGET', KEYS[1], 'total_jobs')) or 0
if total > 0 then
  redis.call('SETBIT', KEYS[5], total, 0)
  redis.call('EXPIRE', KEYS[5], tonumber(ARGV[2]))
end

if status == 'running' then
  local total     = tonumber(redis.call('HGET', KEYS[1], 'total_jobs'))      or 0
  local completed = tonumber(redis.call('HGET', KEYS[1], 'completed_count')) or 0
  local failed    = tonumber(redis.call('HGET', KEYS[1], 'failed_count'))    or 0
  if (completed + failed) >= total then
    local outcome = (failed > 0) and 'complete' or 'success'
    redis.call('HSET', KEYS[1], 'status', outcome)
    redis.call('HSET', KEYS[1], 'finished_at', ARGV[1])
    redis.call('HINCRBY', KEYS[2], 'running', -1)
    redis.call('HINCRBY', KEYS[2], outcome, 1)
    local batch_id = redis.call('HGET', KEYS[1], 'id')
    if batch_id then
      redis.call('ZREM', KEYS[3], batch_id)
      redis.call('ZADD', KEYS[4], tonumber(ARGV[3]), batch_id)
    end
    return {1, outcome}
  end
end
return {2, 'sealed'}
`

const keyPrefix = "kafka_batch:b"
const runningIndex = "kafka_batch:index:running"
const doneIndex = "kafka_batch:index:done"
const allIndex = "kafka_batch:index:all"
const countsKey = "kafka_batch:counts"
const cancelledIndex = "kafka_batch:index:cancelled"

func batchKey(id string) string   { return keyPrefix + ":" + id }
func bitmapKey(id string) string  { return keyPrefix + ":bitmap:" + id }
func seqKey(id string) string     { return keyPrefix + ":seq:" + id }
