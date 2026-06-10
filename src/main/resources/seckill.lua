-- ARGV[1]: voucher id
-- ARGV[2]: user id
-- ARGV[3]: order id
-- ARGV[4]: message id
-- ARGV[5]: current time millis
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]
local messageId = ARGV[4]
local nowMillis = ARGV[5]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
local pendingKey = 'seckill:pending'
local pendingDetailKey = 'seckill:pending:detail:' .. orderId
local reservationKey = 'seckill:reservation:' .. voucherId .. ':' .. userId

local stock = redis.call('get', stockKey)
if (not stock) then
    return 3
end

-- 1: stock is not enough
if (tonumber(stock) <= 0) then
    return 1
end

-- 2: duplicate order by the same user
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end

-- Pre-deduct Redis stock and mark this user as ordered.
redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userId)
redis.call('set', reservationKey, orderId .. ':PENDING:' .. nowMillis)
redis.call('zadd', pendingKey, nowMillis, orderId)
redis.call('hset', pendingDetailKey,
        'voucherId', voucherId,
        'userId', userId,
        'orderId', orderId,
        'messageId', messageId,
        'createTime', nowMillis)

-- 0: seckill qualification passed and the pending reservation was recorded.
return 0
