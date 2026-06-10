-- ARGV[1]: order id
local orderId = ARGV[1]
redis.call('zrem', 'seckill:pending', orderId)
redis.call('del', 'seckill:pending:detail:' .. orderId)
return 1
