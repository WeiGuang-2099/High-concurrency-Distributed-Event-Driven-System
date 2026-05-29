-- KEYS[1] stock key (e.g. stock:1001:VIP)
-- ARGV[1] quantity to reserve
--
-- Returns {ok, code}
--   ok   1 = success, -1 = failure
--   code 'OK' | 'STOCK_NOT_FOUND' | 'OUT_OF_STOCK'

local stockKey = KEYS[1]
local quantity = tonumber(ARGV[1])

local current = tonumber(redis.call('GET', stockKey))
if current == nil then
    return {-1, 'STOCK_NOT_FOUND'}
end

if current < quantity then
    return {-1, 'OUT_OF_STOCK'}
end

redis.call('DECRBY', stockKey, quantity)
return {1, 'OK'}
