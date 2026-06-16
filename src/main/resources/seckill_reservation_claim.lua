-- ARGV[1]: voucher id
-- ARGV[2]: user id
-- ARGV[3]: order id
-- ARGV[4]: current time millis
local reservationKey = 'seckill:reservation:' .. ARGV[1] .. ':' .. ARGV[2]
local expectedPrefix = ARGV[3] .. ':PENDING:'
local reservation = redis.call('get', reservationKey)

if (reservation and string.sub(reservation, 1, string.len(expectedPrefix)) == expectedPrefix) then
    redis.call('set', reservationKey, ARGV[3] .. ':PROCESSING:' .. ARGV[4])
    return 1
end

return 0
