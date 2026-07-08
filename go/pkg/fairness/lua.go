package fairness

// Lua scripts mirror lib/kafka_batch/fairness/scheduler.rb (wire-compatible).

const EnqueueLua = `
local ring    = KEYS[1]
local vth     = KEYS[2]
local tenant  = ARGV[1]
local payload = ARGV[2]
local window  = tonumber(ARGV[3])
local rk      = ARGV[4] .. tenant

if window > 0 and redis.call('LLEN', rk) >= window then return 0 end

if redis.call('ZSCORE', ring, tenant) == false then
  local vt = tonumber(redis.call('HGET', vth, tenant) or '0')
  local mn = redis.call('ZRANGE', ring, 0, 0, 'WITHSCORES')
  if mn[2] and tonumber(mn[2]) > vt then vt = tonumber(mn[2]) end
  redis.call('ZADD', ring, vt, tenant)
  redis.call('HSET', vth, tenant, vt)
end

redis.call('RPUSH', rk, payload)
return 1
`

const CheckoutLuaTime = `
local ring     = KEYS[1]
local gl       = KEYS[2]
local wh       = KEYS[3]
local fwd      = KEYS[4]
local fwd_meta = KEYS[5]
local budget   = tonumber(ARGV[1])
local cap      = tonumber(ARGV[2])
local rprefix  = ARGV[3]
local fetch_n  = tonumber(ARGV[4]) - 1
local dw       = tonumber(ARGV[5])
local weighted = tonumber(ARGV[6])
local ahint    = tonumber(ARGV[7]) or 0
local shint    = tonumber(ARGV[8]) or 0
local t        = redis.call('TIME')
local now      = tonumber(t[1]) + tonumber(t[2]) / 1000000.0
local ttl      = tonumber(ARGV[10]) or 0
local slot_id  = ARGV[11]
local lprefix  = ARGV[12]

redis.call('ZREMRANGEBYSCORE', gl, '-inf', now)
local total = redis.call('ZCARD', gl)
if budget > 0 and total >= budget then return {0, 'budget'} end

local active = redis.call('ZCARD', ring)
if active < 1 then active = 1 end
if ahint > active then active = ahint end

local sum_w = 0
if weighted == 1 and budget > 0 then
  if shint > 0 then
    sum_w = shint
  else
    local all = redis.call('ZRANGE', ring, 0, -1)
    for i = 1, #all do
      local wi = tonumber(redis.call('HGET', wh, all[i]) or dw)
      if wi == nil or wi <= 0 then wi = dw end
      sum_w = sum_w + wi
    end
    if sum_w <= 0 then sum_w = dw end
  end
end

local members = redis.call('ZRANGE', ring, 0, fetch_n)
for round = 1, 2 do
  local fallback = (round == 2)
  for i = 1, #members do
    local t  = members[i]
    local lk = lprefix .. t
    redis.call('ZREMRANGEBYSCORE', lk, '-inf', now)
    local tin = redis.call('ZCARD', lk)

    local dispatchable
    if fallback then
      dispatchable = (cap == 0 or tin < cap)
    else
      local eff_cap = 0
      if budget > 0 then
        if weighted == 1 then
          local w_t = tonumber(redis.call('HGET', wh, t) or dw)
          if w_t == nil or w_t <= 0 then w_t = dw end
          eff_cap = math.floor(budget * w_t / sum_w)
          if eff_cap < 1 then eff_cap = 1 end
        else
          eff_cap = math.ceil(budget / active)
          if eff_cap < 1 then eff_cap = 1 end
        end
      end
      if cap > 0 and (eff_cap == 0 or cap < eff_cap) then eff_cap = cap end
      dispatchable = (eff_cap == 0 or tin < eff_cap)
    end

    if dispatchable then
      local rk  = rprefix .. t
      local job = redis.call('LPOP', rk)
      if job then
        if redis.call('LLEN', rk) == 0 then
          redis.call('ZREM', ring, t)
        end
        local exp = now + ttl
        redis.call('ZADD', gl, exp, slot_id)
        redis.call('ZADD', lk, exp, slot_id)
        redis.call('HSET', fwd, slot_id, job)
        redis.call('HSET', fwd_meta, slot_id, t)
        return {1, t, job}
      else
        redis.call('ZREM', ring, t)
      end
    end
  end
end
return {0, 'none'}
`

const CheckoutLuaCount = `
local ring     = KEYS[1]
local vth      = KEYS[2]
local gl       = KEYS[3]
local wh       = KEYS[4]
local fwd      = KEYS[5]
local fwd_meta = KEYS[6]
local budget   = tonumber(ARGV[1])
local cap      = tonumber(ARGV[2])
local rprefix  = ARGV[3]
local dw       = tonumber(ARGV[4])
local fetch_n  = tonumber(ARGV[5]) - 1
local weighted = tonumber(ARGV[6])
local ahint    = tonumber(ARGV[7]) or 0
local shint    = tonumber(ARGV[8]) or 0
local t        = redis.call('TIME')
local now      = tonumber(t[1]) + tonumber(t[2]) / 1000000.0
local ttl      = tonumber(ARGV[10]) or 0
local slot_id  = ARGV[11]
local lprefix  = ARGV[12]

redis.call('ZREMRANGEBYSCORE', gl, '-inf', now)
local total = redis.call('ZCARD', gl)
if budget > 0 and total >= budget then return {0, 'budget'} end

local active = redis.call('ZCARD', ring)
if active < 1 then active = 1 end
if ahint > active then active = ahint end

local sum_w = 0
if weighted == 1 and budget > 0 then
  if shint > 0 then
    sum_w = shint
  else
    local all = redis.call('ZRANGE', ring, 0, -1)
    for i = 1, #all do
      local wi = tonumber(redis.call('HGET', wh, all[i]) or dw)
      if wi == nil or wi <= 0 then wi = dw end
      sum_w = sum_w + wi
    end
    if sum_w <= 0 then sum_w = dw end
  end
end

local members = redis.call('ZRANGE', ring, 0, fetch_n)
for round = 1, 2 do
  local fallback = (round == 2)
  for i = 1, #members do
    local tnt  = members[i]
    local lk = lprefix .. tnt
    redis.call('ZREMRANGEBYSCORE', lk, '-inf', now)
    local tin = redis.call('ZCARD', lk)

    local dispatchable
    if fallback then
      dispatchable = (cap == 0 or tin < cap)
    else
      local eff_cap = 0
      if budget > 0 then
        if weighted == 1 then
          local w_t = tonumber(redis.call('HGET', wh, tnt) or dw)
          if w_t == nil or w_t <= 0 then w_t = dw end
          eff_cap = math.floor(budget * w_t / sum_w)
          if eff_cap < 1 then eff_cap = 1 end
        else
          eff_cap = math.ceil(budget / active)
          if eff_cap < 1 then eff_cap = 1 end
        end
      end
      if cap > 0 and (eff_cap == 0 or cap < eff_cap) then eff_cap = cap end
      dispatchable = (eff_cap == 0 or tin < eff_cap)
    end

    if dispatchable then
      local rk  = rprefix .. tnt
      local job = redis.call('LPOP', rk)
      if job then
        local w = tonumber(redis.call('HGET', wh, tnt) or dw)
        if w == nil or w <= 0 then w = dw end
        local vt = tonumber(redis.call('ZSCORE', ring, tnt)) + (1.0 / w)
        redis.call('HSET', vth, tnt, vt)
        if redis.call('LLEN', rk) == 0 then
          redis.call('ZREM', ring, tnt)
        else
          redis.call('ZADD', ring, vt, tnt)
        end
        local exp = now + ttl
        redis.call('ZADD', gl, exp, slot_id)
        redis.call('ZADD', lk, exp, slot_id)
        redis.call('HSET', fwd, slot_id, job)
        redis.call('HSET', fwd_meta, slot_id, tnt)
        return {1, tnt, job}
      else
        redis.call('ZREM', ring, tnt)
      end
    end
  end
end
return {0, 'none'}
`

const CompleteLuaTimeLease = `
local t       = ARGV[1]
local inc     = tonumber(ARGV[2])
local rprefix = ARGV[3]

redis.call('ZREM', KEYS[1], ARGV[4])
if redis.call('ZREM', KEYS[2], ARGV[4]) == 1 then
  local vt = tonumber(redis.call('HGET', KEYS[3], t) or '0') + inc
  redis.call('HSET', KEYS[3], t, vt)
  local rk = rprefix .. t
  if redis.call('LLEN', rk) > 0 then
    redis.call('ZADD', KEYS[4], vt, t)
  end
end
return 1
`

const CompleteLuaTimeLegacy = `
local t   = ARGV[1]
local inc = tonumber(ARGV[2])
local vt  = tonumber(redis.call('HGET', KEYS[1], t) or '0') + inc
redis.call('HSET', KEYS[1], t, vt)
local rk = ARGV[3] .. t
if redis.call('LLEN', rk) > 0 then
  redis.call('ZADD', KEYS[2], vt, t)
end
return 1
`

const CompleteLuaCountLease = `
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
`

const ConfirmForwardLua = `
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
  redis.call('HDEL', KEYS[1], ARGV[1])
  redis.call('HDEL', KEYS[2], ARGV[1])
  return 1
end
return 0
`

const AbortForwardLuaTime = `
local job = redis.call('HGET', KEYS[1], ARGV[1])
if not job then return 0 end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('RPUSH', ARGV[3] .. ARGV[2], job)
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
return 1
`

const AbortForwardLuaCount = `
local job = redis.call('HGET', KEYS[1], ARGV[1])
if not job then return 0 end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('RPUSH', ARGV[3] .. ARGV[2], job)
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
local w = tonumber(redis.call('HGET', KEYS[7], ARGV[2]) or ARGV[4])
if w == nil or w <= 0 then w = tonumber(ARGV[4]) end
local vt = tonumber(redis.call('HGET', KEYS[6], ARGV[2]) or '0') - (1.0 / w)
redis.call('HSET', KEYS[6], ARGV[2], vt)
local rk = ARGV[3] .. ARGV[2]
if redis.call('LLEN', rk) > 0 then
  redis.call('ZADD', KEYS[5], vt, ARGV[2])
end
return 1
`

const RearmLeaseLua = `
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
return 1
`
