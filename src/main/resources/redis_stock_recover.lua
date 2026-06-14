-- ARGV[1]: voucher id
-- ARGV[2]: order id
local voucherId = ARGV[1]
local orderId = ARGV[2]
local stockKey = 'seckill:stock:' .. voucherId
local recoveredKey = 'seckill:stock:recovered:' .. voucherId

if (not redis.call('get', stockKey)) then
    return 2
end

if (redis.call('sadd', recoveredKey, orderId) == 0) then
    return 1
end

redis.call('incrby', stockKey, 1)
return 0
