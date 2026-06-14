-- ARGV[1]: voucher id
-- ARGV[2]: user id
-- ARGV[3]: order id
-- ARGV[4]: current time millis
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]
local nowMillis = ARGV[4]

local pendingKey = 'seckill:pending'
local pendingDetailKey = 'seckill:pending:detail:' .. orderId
local reservationKey = 'seckill:reservation:' .. voucherId .. ':' .. userId

local reservation = redis.call('get', reservationKey)
local expectedPrefix = orderId .. ':'
if (reservation and string.sub(reservation, 1, string.len(expectedPrefix)) == expectedPrefix) then
    redis.call('set', reservationKey, orderId .. ':COMMITTED:' .. nowMillis)
end

redis.call('zrem', pendingKey, orderId)
redis.call('del', pendingDetailKey)
return 1
