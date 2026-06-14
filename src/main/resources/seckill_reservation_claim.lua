-- ARGV[1]: voucher id
-- ARGV[2]: user id
-- ARGV[3]: order id
-- ARGV[4]: current time millis
-- ARGV[5]: processing timeout millis
local reservationKey = 'seckill:reservation:' .. ARGV[1] .. ':' .. ARGV[2]
local expectedPrefix = ARGV[3] .. ':PENDING:'
local processingPrefix = ARGV[3] .. ':PROCESSING:'
local nowMillis = tonumber(ARGV[4])
local processingTimeoutMillis = tonumber(ARGV[5])
local reservation = redis.call('get', reservationKey)

if (reservation and string.sub(reservation, 1, string.len(expectedPrefix)) == expectedPrefix) then
    redis.call('set', reservationKey, ARGV[3] .. ':PROCESSING:' .. ARGV[4])
    return 1
end

if (reservation and string.sub(reservation, 1, string.len(processingPrefix)) == processingPrefix) then
    local processingTime = tonumber(string.sub(reservation, string.len(processingPrefix) + 1))
    if (processingTime and nowMillis - processingTime >= processingTimeoutMillis) then
        redis.call('set', reservationKey, ARGV[3] .. ':PROCESSING:' .. ARGV[4])
        return 1
    end
end

return 0
