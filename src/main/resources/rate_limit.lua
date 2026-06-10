local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local max_requests = tonumber(ARGV[3])
local request_id = ARGV[4]
local expire_seconds = tonumber(ARGV[5])

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

local current = redis.call('ZCARD', key)
if current >= max_requests then
    redis.call('EXPIRE', key, expire_seconds)
    return 0
end

redis.call('ZADD', key, now, request_id)
redis.call('EXPIRE', key, expire_seconds)
return 1
