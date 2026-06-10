-- ARGV[1]: voucher id
-- ARGV[2]: user id
-- ARGV[3]: order id
-- ARGV[4]: current time millis
-- ARGV[5]: processing timeout millis
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]
local nowMillis = tonumber(ARGV[4])
local processingTimeoutMillis = tonumber(ARGV[5])

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
local pendingKey = 'seckill:pending'
local pendingDetailKey = 'seckill:pending:detail:' .. orderId
local reservationKey = 'seckill:reservation:' .. voucherId .. ':' .. userId

local reservation = redis.call('get', reservationKey)
if (not reservation) then
    redis.call('zrem', pendingKey, orderId)
    redis.call('del', pendingDetailKey)
    return 1
end

local expectedPrefix = orderId .. ':'
if (string.sub(reservation, 1, string.len(expectedPrefix)) ~= expectedPrefix) then
    redis.call('zrem', pendingKey, orderId)
    redis.call('del', pendingDetailKey)
    return 1
end

local pendingValue = orderId .. ':PENDING:'
local processingValue = orderId .. ':PROCESSING:'
local rollbackAllowed = string.sub(reservation, 1, string.len(pendingValue)) == pendingValue
if (not rollbackAllowed and string.sub(reservation, 1, string.len(processingValue)) == processingValue) then
    local processingTime = tonumber(string.sub(reservation, string.len(processingValue) + 1))
    rollbackAllowed = processingTime and nowMillis - processingTime >= processingTimeoutMillis
end

if (not rollbackAllowed) then
    return 3
end

if (not redis.call('get', stockKey)) then
    return 2
end

redis.call('srem', orderKey, userId)
redis.call('incrby', stockKey, 1)
redis.call('del', reservationKey)
redis.call('zrem', pendingKey, orderId)
redis.call('del', pendingDetailKey)

-- 0: rollback was done by this execution.
return 0
